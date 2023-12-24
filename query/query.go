package query

import (
	"log/slog"
	"slices"

	"github.com/KKKIIO/inv-index-demo/index"
	"github.com/KKKIIO/inv-index-demo/store"
	"github.com/RoaringBitmap/roaring"
)

type OrdersSearchService struct {
	AllIndexReader         *TermIndexReader[int64]
	OrderStatusIndexReader *TermIndexReader[int64]
	ProductIdIndexReader   *TermIndexReader[int64]
	ProviderIdIndexReader  *TermIndexReader[*int64]
	CreateTimeIndexReader  *SparseU64IndexReader
}

func NewOrdersSearchService(bmStore *store.RedisBmStore, sortedBmStore *store.RedisSortKeyBitmapStore,
	fvStore *store.RedisFvStore) *OrdersSearchService {
	return &OrdersSearchService{
		AllIndexReader: &TermIndexReader[int64]{
			Index: index.TermIndex{
				TableName: "orders",
				FieldName: "__all",
			},
			BmStore: bmStore,
		},
		OrderStatusIndexReader: &TermIndexReader[int64]{
			Index: index.TermIndex{
				TableName: "orders",
				FieldName: "order_status",
			},
			BmStore: bmStore,
		},
		ProductIdIndexReader: &TermIndexReader[int64]{
			Index: index.TermIndex{
				TableName: "orders",
				FieldName: "product_id",
			},
			BmStore: bmStore,
		},
		ProviderIdIndexReader: &TermIndexReader[*int64]{
			Index: index.TermIndex{
				TableName: "orders",
				FieldName: "provider_id",
			},
			BmStore: bmStore,
		},
		CreateTimeIndexReader: &SparseU64IndexReader{
			Index: index.SparseIndex{
				TableName: "orders",
				FieldName: "create_time",
			},
			BmStore: sortedBmStore,
			FvStore: fvStore,
		},
	}
}

type Request struct {
	OrderStatusEq    *int64
	ProductIDEq      *int64
	ProviderIDFilter *NullableValueFilter[int64]
	Limit            *int
}

type Response struct {
	IDs   []uint32
	Total uint64
}

// List returns a list of order IDs matching the given query ordered by createTime desc.
func (s *OrdersSearchService) List(r Request) (*Response, error) {
	slog.Debug("Querying orders", slog.Group("request",
		slog.Any("OrderStatusEq", r.OrderStatusEq),
		slog.Any("ProductIDEq", r.ProductIDEq),
		slog.Any("ProviderIDFilter", r.ProviderIDFilter),
	))
	accBm, err := s.AllIndexReader.Get(0)
	if err != nil {
		return nil, err
	}
	if r.OrderStatusEq != nil {
		bm, err := s.OrderStatusIndexReader.Get(*r.OrderStatusEq)
		if err != nil {
			return nil, err
		}
		accBm.And(bm)
	}
	if r.ProductIDEq != nil {
		bm, err := s.ProductIdIndexReader.Get(*r.ProductIDEq)
		if err != nil {
			return nil, err
		}
		accBm.And(bm)
	}
	if r.ProviderIDFilter != nil {
		switch r.ProviderIDFilter.Mode {
		case FilterModeEq:
			bm, err := s.ProviderIdIndexReader.Get(&r.ProviderIDFilter.Value)
			if err != nil {
				return nil, err
			}
			accBm.And(bm)
		case FilterModeNull:
			bm, err := s.ProviderIdIndexReader.Get(nil)
			if err != nil {
				return nil, err
			}
			accBm.And(bm)
		case FilterModeNotNull:
			bm, err := s.ProviderIdIndexReader.Get(nil)
			if err != nil {
				return nil, err
			}
			accBm.AndNot(bm)
		}
	}
	resp := Response{Total: accBm.GetCardinality()}
	if (r.Limit != nil && *r.Limit == 0) || resp.Total == 0 {
		return &resp, nil
	}
	resultIds := make([]uint32, 0)
	if err := s.CreateTimeIndexReader.Scan(accBm, true, func(sortedIds []index.SortId) bool {
		for _, sortId := range sortedIds {
			resultIds = append(resultIds, sortId.Id)
			if r.Limit != nil && len(resultIds) >= *r.Limit {
				return false
			}
		}
		return true
	}); err != nil {
		return nil, err
	}
	resp.IDs = resultIds
	return &resp, nil
}

type TermIndexReader[T index.Term] struct {
	Index   index.TermIndex
	BmStore *store.RedisBmStore
}

func (r *TermIndexReader[T]) Get(fv T) (*roaring.Bitmap, error) {
	return r.BmStore.Get(r.Index.GetIndexKey(), r.Index.MakeValueKey(fv))
}

type SparseU64IndexReader struct {
	Index   index.SparseIndex
	BmStore *store.RedisSortKeyBitmapStore
	FvStore *store.RedisFvStore
}

func (r *SparseU64IndexReader) Scan(baseBm *roaring.Bitmap, reverse bool, proc func([]index.SortId) bool) error {
	// scan bitmaps, sort by fv
	start, end := uint64(0), uint64(0xFFFFFFFFFFFFFFFF)
	if reverse {
		start, end = end, start
	}
	indexKey := r.Index.MakeIndexKey()
	for start != end {
		sortedBms, err := r.BmStore.Scan(indexKey, start, end, reverse, 100)
		if err != nil {
			return err
		}
		if len(sortedBms) == 0 {
			break
		}
		start = sortedBms[len(sortedBms)-1].SortKey
		if start != end {
			if !reverse {
				start += 1
			} else {
				start -= 1
			}
		}
		for _, sortedBm := range sortedBms {
			sortedBm.Bitmap.And(baseBm)
			if sortedBm.Bitmap.GetCardinality() == 0 {
				continue
			}
			sortedIds, err := index.QuerySortIds(r.FvStore, indexKey, sortedBm.Bitmap)
			if err != nil {
				return err
			}
			if reverse {
				slices.Reverse(sortedIds)
			}
			if !proc(sortedIds) {
				return nil
			}
		}
	}
	return nil
}

type NullableValueFilterMode int

const (
	FilterModeEq NullableValueFilterMode = iota
	FilterModeNull
	FilterModeNotNull
)

type NullableValueFilter[T any] struct {
	Mode  NullableValueFilterMode
	Value T
}
