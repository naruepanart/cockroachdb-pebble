package main

import (
	"abcc/pkg/database"
	"abcc/pkg/users"
	"fmt"
	"log"
)

func main() {
	db, err := database.ConnLevelDB()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	key := []byte("key1")

	// Perform create, read, update, delete operations
	err = users.Create(db, key, []byte("value1"))
	if err != nil {
		log.Fatal(err)
	}

	v, err := users.FindOne(db, key)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print(string(v))

	err = users.Update(db, key, []byte("newValue1"))
	if err != nil {
		log.Fatal(err)
	}

	err = users.Remove(db, key)
	if err != nil {
		log.Fatal(err)
	}
}
