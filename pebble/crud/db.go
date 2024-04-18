package crud

import (
	"github.com/cockroachdb/pebble"
)

func ConnPebbleDB() (*pebble.DB, error) {
	dbPath := "abc-pebble-db"
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return db, err
}
