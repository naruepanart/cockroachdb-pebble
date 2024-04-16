package main

import (
	"abcc/crud"
	"fmt"
	"github.com/cockroachdb/pebble"
	"log"
)

func main() {
	// Define the database path
	dbPath := "abc-pebble-db"

	// Open the Pebble database
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		log.Fatalf("failed to open Pebble database: %v", err)
	}
	defer db.Close() // Ensure the database is closed when the program exits

	// Perform CRUD operations using helper functions

	// Create
	key := []byte("key1")
	value := []byte("value1")
	err = crud.CreateKeyValue(db, key, value)
	if err != nil {
		log.Fatalf("CreateKeyValue error: %v", err)
	}
	fmt.Println("Key-value pair created")

	// for i := 0; i < 100; i++ {
	// 	key := []byte("key" + strconv.Itoa(i))
	// 	value := []byte("value" + strconv.Itoa(i))
	// 	err = crud.CreateKeyValue(db, key, value)
	// 	if err != nil {
	// 		log.Fatalf("CreateKeyValue error: %v", err)
	// 	}
	// 	fmt.Println("Key-value pair created")
	// }

	// Read
	retrievedValue, err := crud.ReadKeyValue(db, key)
	if err != nil {
		log.Fatalf("ReadKeyValue error: %v", err)
	}
	fmt.Printf("Retrieved value for key '%s': %s\n", key, retrievedValue)

	// Update
	newValue := []byte("newValue1")
	err = crud.UpdateKeyValue(db, key, newValue)
	if err != nil {
		log.Fatalf("UpdateKeyValue error: %v", err)
	}
	fmt.Println("Key-value pair updated")

	// Delete
	err = crud.DeleteKeyValue(db, key)
	if err != nil {
		log.Fatalf("DeleteKeyValue error: %v", err)
	}
	fmt.Println("Key-value pair deleted")
}
