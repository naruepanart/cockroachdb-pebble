package crud

import (
	"github.com/cockroachdb/pebble"
)

func ConnPebbleDB() (*pebble.DB, error) {
	// Define the database path
	dbPath := "../abc-pebble-db"
	// Open the Pebble database
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return db, err
}
