package main

import (
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

func main() {
	BenchmarkWithConnectionPool()
	BenchmarkWithoutConnectionPool()
}

type ConnectionPool struct {
	pool    chan *sql.DB
	maxConn int64
	minConn int64
	mu      sync.Mutex
}

func NewConnctionPool(maxConn, minConn int64) *ConnectionPool {

	connectionPool := make(chan *sql.DB, maxConn)
	for i := 0; int64(i) < maxConn; i++ {
		db, err := sql.Open("postgres", "postgresql://localhost:5432/arumugaprakash?application_name=arumugaprakash&sslmode=disable")
		if err != nil {
			log.Fatal(err)
		}
		connectionPool <- db
	}

	return &ConnectionPool{pool: connectionPool, minConn: minConn, maxConn: maxConn}
}

func (cp *ConnectionPool) getConnection() (*sql.DB, error) {

	cp.mu.Lock()
	defer cp.mu.Unlock()
	select {
	case db := <-cp.pool:
		return db, nil
	default:
		return nil, fmt.Errorf("connection pool is empty")
	}
}

func (cp *ConnectionPool) releaseConnection(db *sql.DB) error {

	cp.mu.Lock()
	defer cp.mu.Unlock()

	select {
	case cp.pool <- db:
		return nil
	default:
		db.Close()
		return fmt.Errorf("connection pool is full")
	}

}

func BenchmarkWithoutConnectionPool() {
	fmt.Println("Starting Benchmarking Without Connection Pool")
	startingTime := time.Now()
	for i := 0; i < 1000; i++ {
		db, err := sql.Open("postgres", "postgresql://localhost:5432/arumugaprakash?application_name=arumugaprakash&sslmode=disable")
		if err != nil {
			log.Fatal(err)
		}
		_, err = db.Exec("select * from ksuid_table")
		if err != nil {
			log.Fatal("Error executing query")
		}
		db.Close()
	}
	fmt.Printf("time taken for without pooling %v \n", time.Since(startingTime))
}

func BenchmarkWithConnectionPool() {
	fmt.Println("Starting Benchmarking With Connection Pool")
	pool := NewConnctionPool(10, 5)
	startingTime := time.Now()
	for i := 0; i < 1000; i++ {
		db, err := pool.getConnection()
		if err != nil {
			log.Fatal("Error getting connection")
		}
		_, err = db.Exec("select * from ksuid_table")
		if err != nil {
			log.Fatal("Error executing query")
		}
		pool.releaseConnection(db)
	}
	fmt.Printf("Time taken %v \n", time.Since(startingTime))

}
