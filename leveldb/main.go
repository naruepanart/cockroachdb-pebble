package main

import (
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"log"
)

func main() {
	// Define the database path
	dbPath := "../abc-pebble-db"

	// Open the Pebble database
	db, err := leveldb.OpenFile(dbPath, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create
	key := []byte("key1")
	value := []byte("value1")
	err = db.Put(key, value, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Key-value pair created")

	// Read
	v, err := db.Get(key, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(key), string(v))

	// Update
	newValue := []byte("newValue1")
	err = db.Put(key, newValue, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Key-value pair updated")

	// Delete
	err = db.Delete(key, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Key-value pair deleted")
}
