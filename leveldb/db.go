package main

import "github.com/syndtr/goleveldb/leveldb"

// OpenDatabase encapsulates the database opening and error handling
func OpenDatabase(path string) (*leveldb.DB, error) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}
	return db, nil
}