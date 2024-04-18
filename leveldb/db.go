package main

import "github.com/syndtr/goleveldb/leveldb"

func ConnLevelDB() (*leveldb.DB, error) {
	dbPath := "../abc-pebble-db"
	db, err := leveldb.OpenFile(dbPath, nil)
	if err != nil {
		return nil, err
	}
	return db, err
}