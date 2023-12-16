package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/IBM/sarama"
	"github.com/KKKIIO/inv-index-pg/query"
	"github.com/KKKIIO/inv-index-pg/store"
	"github.com/KKKIIO/inv-index-pg/sync"
	"github.com/gin-gonic/gin"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
)

func main() {
	var indexName string
	var topicPrefix string
	flag.StringVar(&indexName, "index", "0", "index name")
	flag.StringVar(&topicPrefix, "topic-prefix", "", "topic prefix")
	flag.Parse()
	if indexName == "" || topicPrefix == "" {
		flag.Usage()
		return
	}
	namespace := fmt.Sprintf("inv-pg-%s", indexName)
	logLevel := slog.LevelDebug
	h := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel})
	slog.SetDefault(slog.New(h))
	db, err := sql.Open("pgx", fmt.Sprintf("postgres://%s:%s@%s:5432/%s?sslmode=disable",
		os.Getenv("POSTGRES_USER"), os.Getenv("POSTGRES_PASSWORD"), os.Getenv("POSTGRES_HOSTNAME"), os.Getenv("POSTGRES_DB")))
	if err != nil {
		slog.Error("Failed to connect to database", "error", err)
		return
	}
	rdb := redis.NewClient(&redis.Options{Addr: "redis:6379"})
	bmStore := &store.RedisBmStore{RDB: rdb, Prefix: namespace + ":bm:"}
	skbmStore := &store.RedisSortKeyBitmapStore{RDB: rdb, Prefix: namespace + ":skbm:"}
	fvStore := &store.RedisFvStore{RDB: rdb, Prefix: namespace + ":fv:"}
	sarama.Logger = slog.NewLogLogger(h, logLevel)
	c, err := sync.NewConsumer(sync.Config{
		Brokers:       []string{"localhost:9092"},
		Topic:         fmt.Sprintf("%s.public.orders", topicPrefix),
		ConsumerGroup: namespace,
	})
	if err != nil {
		slog.Error("Failed to create consumer", "error", err)
		return
	}
	c.Start(bmStore, skbmStore, fvStore)
	defer func() {
		if err := c.Shutdown(); err != nil {
			slog.Error("Failed to shutdown consumer", "error", err)
		}
	}()
	s := query.NewOrdersSearchService(bmStore, skbmStore, fvStore)
	defer db.Close()
	r := gin.Default()
	r.GET("/orders", func(c *gin.Context) {
		QueryOrders(s, db, c)
	})
	slog.Info("Server listening on :8080")
	if err := r.Run(":8080"); err != nil && err != http.ErrServerClosed {
		slog.Error("Error running server", "error", err)
	}
}

func QueryOrders(s *query.OrdersSearchService, db *sql.DB, c *gin.Context) {
	var q struct {
		OrderStatusEq     *int64 `form:"order_status_eq"`
		ProductIDEq       *int64 `form:"product_id_eq"`
		ProviderIDEq      string `form:"provider_id_eq"`
		ProviderIDNotNull string `form:"provider_id_not_null"`
		Limit             *int   `form:"limit"`
	}
	if err := c.BindQuery(&q); err != nil {
		return
	}
	r := query.Request{
		OrderStatusEq: q.OrderStatusEq,
		ProductIDEq:   q.ProductIDEq,
		Limit:         q.Limit,
	}
	if q.ProviderIDEq == "null" {
		r.ProviderIDFilter = &query.NullableValueFilter[int64]{
			Mode: query.FilterModeNull,
		}
	} else if q.ProviderIDEq != "" {
		id, err := strconv.ParseInt(q.ProviderIDEq, 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": gin.H{
					"message": "Invalid provider_id_eq",
				},
			})
			return
		}
		r.ProviderIDFilter = &query.NullableValueFilter[int64]{
			Mode:  query.FilterModeEq,
			Value: id,
		}
	} else if q.ProviderIDNotNull != "" {
		r.ProviderIDFilter = &query.NullableValueFilter[int64]{
			Mode: query.FilterModeNotNull,
		}
	}
	listResp, err := s.List(r)
	if err != nil {
		slog.Error("Error querying orders", "error", err)
		c.JSON(http.StatusInternalServerError, internalErrorBody)
		return
	}
	resp := QueryOrdersResponse{Total: listResp.Total}
	if len(listResp.IDs) == 0 {
		c.JSON(http.StatusOK, resp)
		return
	}
	orders, err := queryDbOrders(db, listResp.IDs)
	if err != nil {
		slog.Error("Error querying orders", "error", err)
		c.JSON(http.StatusInternalServerError, internalErrorBody)
		return
	}
	orderMap := make(map[int64]*Order)
	for _, order := range orders {
		orderMap[order.ID] = order
	}
	resp.Orders = make([]*Order, len(listResp.IDs))
	for i, id := range listResp.IDs {
		if order, ok := orderMap[int64(id)]; ok {
			resp.Orders[i] = order
		} else { // WARN: may be out of sync
			resp.Orders[i] = &Order{ID: int64(id)}
		}
	}
	c.JSON(http.StatusOK, resp)
}

type QueryOrdersResponse struct {
	Orders []*Order `json:"orders"`
	Total  uint64   `json:"total"`
}

type Order struct {
	ID          int64  `json:"id"`
	OrderStatus int64  `json:"order_status"`
	ProductID   int64  `json:"product_id"`
	ProviderID  *int64 `json:"provider_id"`
	CreateTime  string `json:"create_time"`
}

func queryDbOrders(db *sql.DB, ids []uint32) ([]*Order, error) {
	rows, err := db.Query("SELECT id, order_status, product_id, provider_id, create_time FROM orders WHERE id = ANY($1::int[])", ids)
	if err != nil {
		return nil, fmt.Errorf("Error querying orders: %w", err)
	}
	defer rows.Close()
	orders := make([]*Order, 0, len(ids))
	for rows.Next() {
		var order Order
		var createTime time.Time
		if err := rows.Scan(&order.ID, &order.OrderStatus, &order.ProductID, &order.ProviderID, &createTime); err != nil {
			return nil, fmt.Errorf("Error scanning order: %w", err)
		}
		order.CreateTime = createTime.Format(time.RFC3339)
		orders = append(orders, &order)
	}
	return orders, nil
}

var internalErrorBody = gin.H{
	"error": gin.H{
		"message": "Internal server error",
	},
}
