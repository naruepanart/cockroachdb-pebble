package crud

import (
	"github.com/cockroachdb/pebble"
)

func SetupDB() *pebble.DB {
	// Define the database path
	dbPath := "batch-test-db"
	// Open the Pebble database
	db, _ := pebble.Open(dbPath, &pebble.Options{})
	return db
}
