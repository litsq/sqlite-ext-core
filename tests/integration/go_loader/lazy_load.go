package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/mattn/go-sqlite3"
)

// This program verifies that a SQLite extension can be loaded dynamically
// onto an already-open database connection without requiring a restart.
// This is critical for long-running applications that need to add
// functionality at runtime.
func main() {
	extPath := os.Getenv("EXT_PATH")
	if extPath == "" {
		log.Fatal("EXT_PATH environment variable not set")
	}

	numDbs := 3
	connsPerDb := 25
	iterations := 100
	expectedTotal := connsPerDb * iterations

	for i := 0; i < numDbs; i++ {
		dbPath := filepath.Join(os.TempDir(), fmt.Sprintf("test_lazy_load_%d.sqlite", i))
		os.Remove(dbPath)

		// 1. Open with standard "sqlite3" driver (no extension pre-registered)
		db, err := sql.Open("sqlite3", dbPath)
		if err != nil {
			log.Fatalf("Failed to open standard DB %d: %v", i, err)
		}
		db.SetMaxOpenConns(connsPerDb)

		fmt.Printf("DB %d: Pool opened (Standard Driver)\n", i)

		// 2. We must lazy load the extension on the connections.
		// Since we want to test concurrency, we'll spawn goroutines that
		// each ensure the extension is loaded on their specific connection.
		var wg sync.WaitGroup
		var lastCount int
		var mu sync.Mutex

		for j := 0; j < connsPerDb; j++ {
			wg.Add(1)
			go func(dId, cId int) {
				defer wg.Done()

				// Get a dedicated connection from the pool
				conn, err := db.Conn(context.Background())
				if err != nil {
					log.Fatalf("DB %d, Conn %d: Failed to get connection: %v", dId, cId, err)
				}
				defer conn.Close()

				// Dynamically load the extension onto this specific connection
				err = conn.Raw(func(driverConn interface{}) error {
					sqliteConn := driverConn.(*sqlite3.SQLiteConn)
					return sqliteConn.LoadExtension(extPath, "sqlite3_myext_init")
				})
				if err != nil {
					log.Fatalf("DB %d, Conn %d: Lazy Load failed: %v", dId, cId, err)
				}

				// Run the queries
				for k := 0; k < iterations; k++ {
					var rowCount int
					err := conn.QueryRowContext(context.Background(), "SELECT test_counter()").Scan(&rowCount)
					if err != nil {
						log.Fatalf("DB %d, Conn %d: Query failed: %v", dId, cId, err)
					}
					mu.Lock()
					if rowCount > lastCount {
						lastCount = rowCount
					}
					mu.Unlock()
				}
			}(i, j)
		}

		wg.Wait()

		if lastCount != expectedTotal {
			db.Close()
			log.Fatalf("Database %d: Expected count %d, got %d", i, expectedTotal, lastCount)
		}

		fmt.Printf("Database %d: Lazy Load concurrency test (Count %d) [OK]\n", i, lastCount)
		db.Close()
	}
}
