package index

import (
	"fmt"
	"sort"

	"github.com/KKKIIO/inv-index-demo/store"
	"github.com/RoaringBitmap/roaring"
)

type SparseIndex struct {
	TableName string
	FieldName string
}

func (i SparseIndex) MakeIndexKey() string {
	return fmt.Sprintf("sparse:%s:%s", i.TableName, i.FieldName)
}

func QuerySortIds(fvStore *store.RedisFvStore, fieldKey string, bm *roaring.Bitmap) ([]SortId, error) {
	ids := make([]uint32, 0)
	for it := bm.Iterator(); it.HasNext(); {
		ids = append(ids, it.Next())
	}
	fvs, err := fvStore.MGet(fieldKey, ids)
	if err != nil {
		return nil, err
	}
	sortIds := make([]SortId, len(ids))
	for i, id := range ids {
		sortIds[i] = SortId{Id: id, SortKey: fvs[i]}
	}
	sort.Slice(sortIds, func(i, j int) bool {
		if sortIds[i].SortKey == sortIds[j].SortKey { // order by id if sort key is the same for better stability
			return sortIds[i].Id < sortIds[j].Id
		}
		return sortIds[i].SortKey < sortIds[j].SortKey
	})
	return sortIds, nil
}

type SortId struct {
	Id      uint32
	SortKey uint64
}
