package index

import (
	"fmt"
	"sort"

	"github.com/KKKIIO/inv-index-pg/store"
	"github.com/RoaringBitmap/roaring"
)

type SparseIndex struct {
	TableName string
	FieldName string
}

func (i SparseIndex) MakeIndexKey() string {
	return fmt.Sprintf("sparse:%s:%s", i.TableName, i.FieldName)
}

func QuerySortedIds(fvStore *store.RedisFvStore, fieldKey string, bm *roaring.Bitmap) ([]SortedId, error) {
	ids := make([]uint32, 0)
	for it := bm.Iterator(); it.HasNext(); {
		ids = append(ids, it.Next())
	}
	fvs, err := fvStore.MGet(fieldKey, ids)
	if err != nil {
		return nil, err
	}
	sortIds := make([]SortedId, len(ids))
	for i, id := range ids {
		sortIds[i] = SortedId{Id: id, Score: fvs[i]}
	}
	sort.Slice(sortIds, func(i, j int) bool { return sortIds[i].Score < sortIds[j].Score })
	return sortIds, nil
}

type SortedId struct {
	Id    uint32
	Score uint64
}
