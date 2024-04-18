package main

import (
	"fmt"
	"log"
)

const dbPath = "../abc-pebble-db"

func main() {
	db, err := OpenDatabase(dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	key := []byte("key1")

	// Perform create, read, update, delete operations
	err = Create(db, key, []byte("value1"))
	if err != nil {
		log.Fatal(err)
	}

	v, err := FindOne(db, key)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print(string(v))

	err = Update(db, key, []byte("newValue1"))
	if err != nil {
		log.Fatal(err)
	}

	err = Remove(db, key)
	if err != nil {
		log.Fatal(err)
	}
}
