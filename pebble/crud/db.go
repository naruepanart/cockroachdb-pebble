package crud

import (
	"github.com/cockroachdb/pebble"
)

func SetupDB() *pebble.DB {
	// Define the database path
	dbPath := "../abc-pebble-db"
	// Open the Pebble database
	db, _ := pebble.Open(dbPath, &pebble.Options{})
	return db
}
