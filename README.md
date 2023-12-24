# Inverted Index Demo

这是一个简单的倒排索引演示 Demo ，详情见博客[如何快速筛选订单](https://kkkiio.github.io/engineering/2023/11/25/inverted-index.html
)。

为 Postgresql 表中的每个字段建立倒排索引，使用 [Roaring Bitmap](https://github.com/RoaringBitmap/roaring) 作为倒排列表，保存到 Redis 中。

## 运行

在 devcontainer 中打开。

```bash
bash ./make_connector.sh
go run gen_testdata/main.go > testdata.csv
bash ./import_testdata.sh
go run main.go -index 0 -topic-prefix postgres-0
# 在另一个终端中
curl http://localhost:8080/orders?limit=10
```