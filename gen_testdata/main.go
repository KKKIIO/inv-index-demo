package main

import (
	"encoding/csv"
	"flag"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	var count int
	flag.IntVar(&count, "count", 10000, "number of orders to generate")
	flag.Parse()
	if count <= 0 {
		flag.Usage()
		return
	}
	writer := csv.NewWriter(os.Stdout)
	defer writer.Flush()
	g := Generator{Writer: writer, Count: count}
	if err := g.Generate(); err != nil {
		log.Fatal(err)
	}
}

type Generator struct {
	Writer *csv.Writer
	Count  int
}

// Generate inserts random orders into database
func (g *Generator) Generate() error {
	// header: order_id,order_status,product_id,provider_id,create_time
	if err := g.Writer.Write([]string{"id", "order_status", "product_id", "provider_id", "create_time"}); err != nil {
		return err
	}
	for i := 0; i < g.Count; i++ {
		status := rand.Intn(3) + 1
		providerId := ""
		if status != 1 {
			providerId = strconv.Itoa(rand.Intn(10000))
		}
		// create_time is between 2020-01-01 and 2020-12-31
		t := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(rand.Intn(365*24*60*60)) * time.Second)
		if err := g.Writer.Write([]string{
			strconv.Itoa(i + 1),
			strconv.Itoa(status),
			strconv.Itoa(rand.Intn(10000)),
			providerId,
			t.Format(time.RFC3339),
		}); err != nil {
			return err
		}
	}
	return nil
}
