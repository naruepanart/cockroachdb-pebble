package main

import (
	"fmt"
	"log"
)

func main() {
	db, err := ConnPebbleDB()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create
	key := []byte("key1")
	value := []byte("value1")
	err = CreateKeyValue(db, key, value)
	if err != nil {
		log.Fatalf("CreateKeyValue error: %v", err)
	}
	fmt.Println("Key-value pair created")

	// for i := 0; i < 100; i++ {
	// 	key := []byte("key" + strconv.Itoa(i))
	// 	value := []byte("value" + strconv.Itoa(i))
	// 	err = CreateKeyValue(db, key, value)
	// 	if err != nil {
	// 		log.Fatalf("CreateKeyValue error: %v", err)
	// 	}
	// 	fmt.Println("Key-value pair created")
	// }

	// Read
	retrievedValue, err := ReadKeyValue(db, key)
	if err != nil {
		log.Fatalf("ReadKeyValue error: %v", err)
	}
	fmt.Printf("Retrieved value for key '%s': %s\n", key, retrievedValue)

	// Update
	newValue := []byte("newValue1")
	err = UpdateKeyValue(db, key, newValue)
	if err != nil {
		log.Fatalf("UpdateKeyValue error: %v", err)
	}
	fmt.Println("Key-value pair updated")

	// Delete
	err = DeleteKeyValue(db, key)
	if err != nil {
		log.Fatalf("DeleteKeyValue error: %v", err)
	}
	fmt.Println("Key-value pair deleted")
}
