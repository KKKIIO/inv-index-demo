package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/IBM/sarama"
	"github.com/KKKIIO/inv-index-pg/index"
	"github.com/KKKIIO/inv-index-pg/store"
	"github.com/RoaringBitmap/roaring"
)

type Config struct {
	Brokers       []string
	Topic         string
	ConsumerGroup string
}

type Consumer struct {
	client sarama.ConsumerGroup
	topic  string
}

func NewConsumer(config Config) (*Consumer, error) {
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.ClientID = "inv-index-pg-sync"
	kafkaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	client, err := sarama.NewConsumerGroup(config.Brokers, config.ConsumerGroup, kafkaConfig)
	if err != nil {
		return nil, fmt.Errorf("Error creating consumer group client: %w", err)
	}
	return &Consumer{
		client: client,
		topic:  config.Topic,
	}, nil
}

func (c *Consumer) Start(bmStore *store.RedisBmStore, sortedBmStore *store.RedisSortKeyBitmapStore, fvStore *store.RedisFvStore) {
	saramaConsumer := &saramaConsumer{
		BmStore:                bmStore,
		SortedBmStore:          sortedBmStore,
		FvStore:                fvStore,
		AllIndexWriter:         NewTermIndexWriter[int64]("orders", "__all"),
		OrderStatusIndexWriter: NewTermIndexWriter[int64]("orders", "order_status"),
		ProductIdIndexWriter:   NewTermIndexWriter[int64]("orders", "product_id"),
		ProviderIdIndexWriter:  NewTermIndexWriter[*int64]("orders", "provider_id"),
		CreateTimeIndexWriter: &SparseU64IndexWriter{
			Index:          index.SparseIndex{TableName: "orders", FieldName: "create_time"},
			SplitThreshold: 1000,
		},
	}
	go func() {
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := c.client.Consume(context.Background(), []string{c.topic}, saramaConsumer); err != nil {
				if err == sarama.ErrClosedConsumerGroup {
					return
				}
				slog.Error("Error from consumer", "error", err)
				time.Sleep(time.Second * 1)
			}
		}
	}()
}

func (c *Consumer) Shutdown() error {
	slog.Info("Shutting down consumer...")
	return c.client.Close()
}

// saramaConsumer represents a Sarama consumer group consumer
type saramaConsumer struct {
	BmStore                *store.RedisBmStore
	SortedBmStore          *store.RedisSortKeyBitmapStore
	FvStore                *store.RedisFvStore
	AllIndexWriter         *TermIndexWriter[int64]
	OrderStatusIndexWriter *TermIndexWriter[int64]
	ProductIdIndexWriter   *TermIndexWriter[int64]
	ProviderIdIndexWriter  *TermIndexWriter[*int64]
	CreateTimeIndexWriter  *SparseU64IndexWriter
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *saramaConsumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *saramaConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the Handler must finish its processing
// loop and exit.
func (consumer *saramaConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				slog.Info("Message channel was closed", "topic", claim.Topic(), "partition", claim.Partition())
				return nil
			}
			slog.Debug("Message claimed", "topic", claim.Topic(), "partition", claim.Partition(), "offset", message.Offset, "value", string(message.Value))
			var dataChangedMessage DataChangedMessage
			if err := json.Unmarshal(message.Value, &dataChangedMessage); err != nil {
				return fmt.Errorf("Failed to unmarshal message, offset=%d, value=%s, err: %w", message.Offset, message.Value, err)
			}
			var err error
			switch dataChangedMessage.Op {
			case "r", "c":
				err = consumer.onInsert(*dataChangedMessage.After)
			case "u":
				err = consumer.onUpdate(*dataChangedMessage.Before, *dataChangedMessage.After)
			case "d":
				err = consumer.onDelete(*dataChangedMessage.Before)
			default:
				err = fmt.Errorf("Unknown op, op=%s, value=%s", dataChangedMessage.Op, message.Value)
			}
			if err != nil {
				return err
			}
			// TODO: commit store
			session.MarkMessage(message, "")
		case <-session.Context().Done():
			slog.Debug("Session was closed", "topic", claim.Topic(), "partition", claim.Partition())
			return nil
		}
	}
}

type DataChangedMessage struct {
	Op     string `json:"op"`
	Before *Order `json:"before"`
	After  *Order `json:"after"`
}

type Order struct {
	ID          uint32 `json:"id"`
	OrderStatus int64  `json:"order_status"`
	ProductID   int64  `json:"product_id"`
	ProviderID  *int64 `json:"provider_id"`
	CreateTime  uint64 `json:"create_time"`
}

func (consumer *saramaConsumer) onInsert(order Order) error {
	if err := consumer.AllIndexWriter.Add(consumer.BmStore, 0, order.ID); err != nil {
		return err
	}
	if err := consumer.OrderStatusIndexWriter.Add(consumer.BmStore, order.OrderStatus, order.ID); err != nil {
		return err
	}
	if err := consumer.ProductIdIndexWriter.Add(consumer.BmStore, order.ProductID, order.ID); err != nil {
		return err
	}
	if err := consumer.ProviderIdIndexWriter.Add(consumer.BmStore, order.ProviderID, order.ID); err != nil {
		return err
	}
	if err := consumer.CreateTimeIndexWriter.Add(consumer.SortedBmStore, consumer.FvStore, order.CreateTime, order.ID); err != nil {
		return err
	}
	return nil
}

func (consumer *saramaConsumer) onUpdate(before Order, after Order) error {
	if err := consumer.OrderStatusIndexWriter.Move(consumer.BmStore, before.OrderStatus, after.OrderStatus, after.ID); err != nil {
		return err
	}
	if err := consumer.ProductIdIndexWriter.Move(consumer.BmStore, before.ProductID, after.ProductID, after.ID); err != nil {
		return err
	}
	if err := consumer.ProviderIdIndexWriter.Move(consumer.BmStore, before.ProviderID, after.ProviderID, after.ID); err != nil {
		return err
	}
	if err := consumer.CreateTimeIndexWriter.Move(consumer.SortedBmStore, consumer.FvStore, before.CreateTime, after.CreateTime, after.ID); err != nil {
		return err
	}
	return nil
}

func (consumer *saramaConsumer) onDelete(order Order) error {
	if err := consumer.AllIndexWriter.Remove(consumer.BmStore, 0, order.ID); err != nil {
		return err
	}
	if err := consumer.OrderStatusIndexWriter.Remove(consumer.BmStore, order.OrderStatus, order.ID); err != nil {
		return err
	}
	if err := consumer.ProductIdIndexWriter.Remove(consumer.BmStore, order.ProductID, order.ID); err != nil {
		return err
	}
	if err := consumer.ProviderIdIndexWriter.Remove(consumer.BmStore, order.ProviderID, order.ID); err != nil {
		return err
	}
	if err := consumer.CreateTimeIndexWriter.Remove(consumer.SortedBmStore, consumer.FvStore, order.CreateTime, order.ID); err != nil {
		return err
	}
	return nil
}

type TermIndexWriter[T index.Term] struct {
	Index index.TermIndex
}

func NewTermIndexWriter[T index.Term](tableName string, fieldName string) *TermIndexWriter[T] {
	return &TermIndexWriter[T]{
		Index: index.TermIndex{
			TableName: tableName,
			FieldName: fieldName,
		},
	}
}

func (w *TermIndexWriter[T]) Add(bmStore *store.RedisBmStore, fv T, id uint32) error {
	indexKey := w.Index.GetIndexKey()
	key := w.Index.MakeValueKey(fv)
	bm, err := bmStore.Get(indexKey, key)
	if err != nil {
		return err
	}
	bm.Add(id)
	if err := bmStore.Set(indexKey, key, bm); err != nil {
		return err
	}
	return nil
}

func (w *TermIndexWriter[T]) Remove(bmStore *store.RedisBmStore, fv T, id uint32) error {
	indexKey := w.Index.GetIndexKey()
	key := w.Index.MakeValueKey(fv)
	bm, err := bmStore.Get(indexKey, key)
	if err != nil {
		return err
	}
	bm.Remove(id)
	if err := bmStore.Set(indexKey, key, bm); err != nil {
		return err
	}
	return nil
}

func (w *TermIndexWriter[K]) Move(bmStore *store.RedisBmStore, before K, after K, id uint32) error {
	if before == after {
		return nil
	}
	if err := w.Remove(bmStore, before, id); err != nil {
		return err
	}
	if err := w.Add(bmStore, after, id); err != nil {
		return err
	}
	return nil
}

type SparseU64IndexWriter struct {
	Index          index.SparseIndex
	SplitThreshold int
}

func (w *SparseU64IndexWriter) Add(bmStore *store.RedisSortKeyBitmapStore, fvStore *store.RedisFvStore, fv uint64, id uint32) error {
	fieldKey := w.Index.MakeIndexKey()
	floorSortedBm, err := getFloorSortedBm(bmStore, fieldKey, fv)
	if err != nil {
		return err
	}
	var updateSortedBms []store.SortKeyBitmap
	if floorSortedBm == nil {
		updateSortedBms = []store.SortKeyBitmap{{SortKey: fv, Bitmap: roaring.New()}}
	} else if floorSortedBm.Bitmap.GetCardinality() < uint64(w.SplitThreshold) {
		updateSortedBms = []store.SortKeyBitmap{*floorSortedBm}
	} else {
		// sort ids by fv, split ids into 2 parts
		sortedIds, err := index.QuerySortedIds(fvStore, fieldKey, floorSortedBm.Bitmap)
		if err != nil {
			return err
		}
		bm1 := floorSortedBm.Bitmap
		bm1.Clear()
		mid := len(sortedIds) / 2
		for _, sortId := range sortedIds[:mid] {
			bm1.Add(sortId.Id)
		}
		bm2 := roaring.New()
		for _, sortId := range sortedIds[mid:] {
			bm2.Add(sortId.Id)
		}
		updateSortedBms = []store.SortKeyBitmap{{SortKey: sortedIds[0].Score, Bitmap: bm1}, {SortKey: sortedIds[mid].Score, Bitmap: bm2}}
		// make first sorted bitmap the floor sorted bitmap
		if updateSortedBms[1].SortKey <= fv {
			updateSortedBms[0], updateSortedBms[1] = updateSortedBms[1], updateSortedBms[0]
		}
	}
	updateSortedBms[0].Bitmap.Add(id)
	if err := fvStore.Set(fieldKey, id, fv); err != nil {
		return err
	}
	if err := bmStore.MSet(fieldKey, updateSortedBms); err != nil {
		return err
	}
	return nil
}

func (w *SparseU64IndexWriter) Remove(bmStore *store.RedisSortKeyBitmapStore, fvStore *store.RedisFvStore, fv uint64, id uint32) error {
	fieldKey := w.Index.MakeIndexKey()
	floorSortedBm, err := getFloorSortedBm(bmStore, fieldKey, fv)
	if err != nil {
		return err
	}
	if floorSortedBm != nil {
		floorSortedBm.Bitmap.Remove(id)
		if err := bmStore.MSet(fieldKey, []store.SortKeyBitmap{*floorSortedBm}); err != nil {
			return err
		}
	} else {
		slog.Warn("cannot find floor sorted bitmap", "fv", fv, "id", id, "fieldKey", fieldKey)
	}
	if err := fvStore.Remove(fieldKey, id); err != nil {
		return err
	}
	return nil
}

func (w *SparseU64IndexWriter) Move(bmStore *store.RedisSortKeyBitmapStore, fvStore *store.RedisFvStore, before uint64, after uint64, id uint32) error {
	if before == after {
		return nil
	}
	if err := w.Remove(bmStore, fvStore, before, id); err != nil {
		return err
	}
	if err := w.Add(bmStore, fvStore, after, id); err != nil {
		return err
	}
	return nil
}

func getFloorSortedBm(bmStore *store.RedisSortKeyBitmapStore, fieldKey string, fv uint64) (*store.SortKeyBitmap, error) {
	sortedBms, err := bmStore.Scan(fieldKey, fv, 0, true, 1)
	if err != nil {
		return nil, err
	}
	if len(sortedBms) == 0 {
		return nil, nil
	}
	return &sortedBms[0], nil
}
