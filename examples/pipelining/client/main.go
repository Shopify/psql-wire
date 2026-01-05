package main

import (
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
)

func main() {
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, "postgres://127.0.0.1:5432/postgres")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close(ctx)

	// Send 3 queries in a batch (pipelined)
	batch := &pgx.Batch{}
	batch.Queue("SELECT 1")
	batch.Queue("SELECT 2")
	batch.Queue("SELECT 3")

	log.Println("Sending 3 pipelined queries...")
	start := time.Now()

	results := conn.SendBatch(ctx, batch)

	// Read all results
	for i := 0; i < 3; i++ {
		_, err := results.Exec()
		if err != nil {
			log.Printf("Query %d error: %v", i+1, err)
		}
	}
	results.Close()

	elapsed := time.Since(start)
	log.Printf("Completed in %v", elapsed)
	log.Println("(With parallel pipelining: ~100ms, without: ~300ms)")
}
