package main

import (
	"fmt"
	"log"
	"testing"
)

func BenchmarkCreate(b *testing.B) {
	db, err := OpenDatabase(dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Reset timer and run the benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("benchmarkKey_%d", i))
		value := []byte(fmt.Sprintf("benchmarkKey_%s_%d", GenerateRandomString(50), i))
		Create(db, key, value)
	}
}
