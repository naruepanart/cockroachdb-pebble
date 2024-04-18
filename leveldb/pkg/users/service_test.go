package users

import (
	"abcc/pkg/database"
	"fmt"
	"log"
	"testing"
)

func BenchmarkCreate(b *testing.B) {
	db, err := database.ConnLevelDB()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Reset timer and run the benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("benchmarkKey_%d", i))
		value := []byte(fmt.Sprintf("benchmarkKey_%d", i))
		Create(db, key, value)
	}
}

func BenchmarkFindOne(b *testing.B) {
	db, err := database.ConnLevelDB()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Reset the timer and start the benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("benchmarkKey_%d", i))
		FindOne(db, key)
	}
}
