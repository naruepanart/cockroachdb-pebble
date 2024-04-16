package crud

import (
	"github.com/cockroachdb/pebble"
	"log"
	"testing"
)

func TestCrudFunctions(t *testing.T) {
	// Define the database path
	dbPath := "test-db"

	// Open the Pebble database
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		log.Fatalf("failed to open Pebble database: %v", err)
	}
	defer db.Close() // Ensure the database is closed when the program exits

	// Define key-value pairs for testing
	key1 := []byte("key1")
	value1 := []byte("value1")

	key2 := []byte("key2")
	_ = []byte("value10")

	// Test CreateKeyValue function
	if err := CreateKeyValue(db, key1, value1); err != nil {
		t.Errorf("CreateKeyValue failed: %v", err)
	}

	// Test ReadKeyValue function
	retrievedValue, err := ReadKeyValue(db, key1)
	if err != nil {
		t.Errorf("ReadKeyValue failed: %v", err)
	}
	if string(retrievedValue) != string(value1) {
		t.Errorf("expected value %s, got %s", value1, retrievedValue)
	}

	// Test UpdateKeyValue function
	newValue1 := []byte("new_value1")
	if err := UpdateKeyValue(db, key1, newValue1); err != nil {
		t.Errorf("UpdateKeyValue failed: %v", err)
	}

	retrievedValue, err = ReadKeyValue(db, key1)
	if err != nil {
		t.Errorf("ReadKeyValue failed: %v", err)
	}
	if string(retrievedValue) != string(newValue1) {
		t.Errorf("expected value %s, got %s", newValue1, retrievedValue)
	}

	// Test DeleteKeyValue function
	if err := DeleteKeyValue(db, key1); err != nil {
		t.Errorf("DeleteKeyValue failed: %v", err)
	}

	retrievedValue, err = ReadKeyValue(db, key1)
	if err == nil {
		t.Errorf("expected error when reading deleted key, got none")
	}
	if retrievedValue != nil {
		t.Errorf("expected no value, got %s", retrievedValue)
	}

	// Test handling of non-existing key
	retrievedValue, err = ReadKeyValue(db, key2)
	if err == nil {
		t.Errorf("expected error when reading non-existing key, got none")
	}
	if retrievedValue != nil {
		t.Errorf("expected no value for non-existing key, got %s", retrievedValue)
	}
}
