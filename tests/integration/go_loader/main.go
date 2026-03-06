package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/mattn/go-sqlite3"
)

// This program runs an intense, concurrent integration test against the compiled
// Rust SQLite extension. It proves three main things:
//  1. Dynamic Loading via SQL Drivers works seamlessly.
//  2. High Concurrency: Multiple read/write queries can run concurrently without state corruption.
//  3. Database Isolation: Extensions track internal state on a per-database basis,
//     meaning queries on DB A do not interfere with the state of DB B.
func main() {
	extPath := os.Getenv("EXT_PATH")
	if extPath == "" {
		log.Fatal("EXT_PATH environment variable not set")
	}

	// Configuration for the stress test
	numDbs := 3                              // We will test 3 completely separate SQLite databases
	connsPerDb := 25                         // We will hit each database with 25 concurrent connections
	iterations := 100                        // Each connection will run 100 queries
	expectedTotal := iterations * connsPerDb // Expected final state count (2500 per DB)

	var wg sync.WaitGroup
	dbResults := make([]int, numDbs)
	var mu sync.Mutex

	for i := 0; i < numDbs; i++ {
		// Define a deterministic file path for each database so we test isolation bounds
		dbPath := filepath.Join(os.TempDir(), fmt.Sprintf("test_db_%d.sqlite", i))
		os.Remove(dbPath) // Ensure we start with a fresh slate

		// Register a unique SQLite driver for each DB instance that permanently loads the extension.
		// Registering unique drivers prevents the pooling layer from mixing up extensions.
		driverName := fmt.Sprintf("sqlite3_ext_%d", i)
		sql.Register(driverName, &sqlite3.SQLiteDriver{
			Extensions: []string{extPath},
		})

		db, err := sql.Open(driverName, dbPath)
		if err != nil {
			log.Fatalf("Failed to open DB %d: %v", i, err)
		}
		// Force Go to actually open all connections concurrently. If we didn't,
		// Go's driver might serialize them, defeating the test's purpose.
		db.SetMaxOpenConns(connsPerDb)
		defer db.Close()

		fmt.Printf("DB %d: Extension loaded via driver registration\n", i)

		// Spawn 25 strictly concurrent goroutines per DB
		// Spawn 25 strictly concurrent goroutines per DB
		for j := 0; j < connsPerDb; j++ {
			wg.Add(1)
			go func(dId, cId int, d *sql.DB) {
				defer wg.Done()
				lastCount, err := runTestOnConn(d, iterations)
				if err != nil {
					fmt.Printf("Error DB %d, Conn %d: %v\n", dId, cId, err)
					os.Exit(1)
				}
				mu.Lock()
				if lastCount > dbResults[dId] {
					dbResults[dId] = lastCount
				}
				mu.Unlock()
			}(i, j, db)
		}
	}

	wg.Wait()

	fmt.Println("\n--- Integration Test Results ---")
	allPassed := true
	for i, res := range dbResults {
		status := "OK"
		if res != expectedTotal {
			status = fmt.Sprintf("FAILED (Expected %d, got %d)", expectedTotal, res)
			allPassed = false
		}
		fmt.Printf("Database %d: Final Count %d [%s]\n", i, res, status)
	}

	if !allPassed {
		fmt.Println("\nRESULT: FAILED - State was not properly maintained across connections.")
		os.Exit(1)
	} else {
		fmt.Println("\nRESULT: PASSED - State is consistent across 25 concurrent connections per DB.")
	}
}

// runTestOnConn simulates a rapid sequence of scalar function queries.
// It executes `SELECT test_counter();`, triggering the extension's Rust logic.
// runTestOnConn simulates a rapid sequence of scalar function queries.
// It executes `SELECT test_counter();`, triggering the extension's Rust logic.
func runTestOnConn(db *sql.DB, iterations int) (int, error) {
	var lastCount int
	for k := 0; k < iterations; k++ {
		err := db.QueryRow("SELECT test_counter()").Scan(&lastCount)
		if err != nil {
			return 0, err
		}
	}
	return lastCount, nil
}
