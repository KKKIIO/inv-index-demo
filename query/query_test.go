package query

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/KKKIIO/inv-index-demo/store"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func FuzzQuery(f *testing.F) {
	// compare the result of the index query with the result of the sql query
	indexName := "1"
	namespace := fmt.Sprintf("inv-pg-%s", indexName)
	rdb := redis.NewClient(&redis.Options{Addr: "redis:6379"})
	bmStore := &store.RedisBmStore{RDB: rdb, Prefix: namespace + ":bm:"}
	skbmStore := &store.RedisSortKeyBitmapStore{RDB: rdb, Prefix: namespace + ":skbm:"}
	fvStore := &store.RedisFvStore{RDB: rdb, Prefix: namespace + ":fv:"}
	ss := NewOrdersSearchService(bmStore, skbmStore, fvStore)
	db, err := sql.Open("pgx", "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable")
	if err != nil {
		f.Fatal(err)
	}
	defer db.Close()
	f.Add(int8(1), int64(23), int64(42))
	f.Add(int8(0), int64(-1), int64(-3))
	f.Fuzz(func(t *testing.T, orderStatus int8, productID int64, providerID int64) {
		var limit = 50
		r := Request{
			Limit: &limit,
		}
		sqlWheres := []string{}
		if orderStatus > 0 {
			v := int64(orderStatus-1)%3 + 1
			r.OrderStatusEq = &v
			sqlWheres = append(sqlWheres, fmt.Sprintf("order_status = %d", v))
		}
		if productID >= 0 {
			r.ProductIDEq = &productID
			sqlWheres = append(sqlWheres, fmt.Sprintf("product_id = %d", productID))
		}
		if providerID >= 0 {
			r.ProviderIDFilter = &NullableValueFilter[int64]{
				Mode:  FilterModeEq,
				Value: providerID,
			}
			sqlWheres = append(sqlWheres, fmt.Sprintf("provider_id = %d", providerID))
		} else if providerID == -1 {
			r.ProviderIDFilter = &NullableValueFilter[int64]{
				Mode: FilterModeNull,
			}
			sqlWheres = append(sqlWheres, fmt.Sprintf("provider_id IS NULL"))
		} else if providerID == -2 {
			r.ProviderIDFilter = &NullableValueFilter[int64]{
				Mode: FilterModeNotNull,
			}
			sqlWheres = append(sqlWheres, fmt.Sprintf("provider_id IS NOT NULL"))
		}
		// query by index
		indexResp, err := ss.List(r)
		assert.NoError(t, err)
		// query by sql
		var sqlWhere string
		if len(sqlWheres) > 0 {
			sqlWhere = "WHERE " + strings.Join(sqlWheres, " AND ")
		}
		countSqlQuery := fmt.Sprintf("SELECT COUNT(*) FROM orders %s", sqlWhere)
		idSqlQuery := fmt.Sprintf("SELECT id FROM orders %s ORDER BY create_time DESC, id DESC LIMIT %d", sqlWhere, limit)
		t.Log(countSqlQuery)
		t.Log(idSqlQuery)
		var count uint64
		err = db.QueryRow(countSqlQuery).Scan(&count)
		assert.NoError(t, err)
		rows, err := db.Query(idSqlQuery)
		assert.NoError(t, err)
		var ids []uint32
		for rows.Next() {
			var id uint32
			err = rows.Scan(&id)
			assert.NoError(t, err)
			ids = append(ids, id)
		}
		assert.Equal(t, count, indexResp.Total)
		assert.Equal(t, ids, indexResp.IDs)
	})
}
