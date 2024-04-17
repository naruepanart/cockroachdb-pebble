package main

import (
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"log"
)

const dbPath = "../abc-pebble-db"

func main() {
	db, err := openDatabase(dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	key := []byte("key1")

	// Perform create, read, update, delete operations
	err = createKeyValue(db, key, []byte("value1"))
	if err != nil {
		log.Fatal(err)
	}

	err = readKeyValue(db, key)
	if err != nil {
		log.Fatal(err)
	}

	err = updateKeyValue(db, key, []byte("newValue1"))
	if err != nil {
		log.Fatal(err)
	}

	err = deleteKeyValue(db, key)
	if err != nil {
		log.Fatal(err)
	}
}

// openDatabase encapsulates the database opening and error handling
func openDatabase(path string) (*leveldb.DB, error) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}
	return db, nil
}

// createKeyValue encapsulates creating a key-value pair in the database
func createKeyValue(db *leveldb.DB, key, value []byte) error {
	err := db.Put(key, value, nil)
	if err != nil {
		return err
	}
	fmt.Println("Key-value pair created")
	return nil
}

// readKeyValue encapsulates reading a key-value pair from the database
func readKeyValue(db *leveldb.DB, key []byte) error {
	value, err := db.Get(key, nil)
	if err != nil {
		return err
	}
	fmt.Println(string(key), string(value))
	return nil
}

// updateKeyValue encapsulates updating a key-value pair in the database
func updateKeyValue(db *leveldb.DB, key, value []byte) error {
	err := db.Put(key, value, nil)
	if err != nil {
		return err
	}
	fmt.Println("Key-value pair updated")
	return nil
}

// deleteKeyValue encapsulates deleting a key-value pair from the database
func deleteKeyValue(db *leveldb.DB, key []byte) error {
	err := db.Delete(key, nil)
	if err != nil {
		return err
	}
	fmt.Println("Key-value pair deleted")
	return nil
}
