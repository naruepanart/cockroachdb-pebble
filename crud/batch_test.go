package crud

import (
	"log"
	"testing"
	"github.com/cockroachdb/pebble"
)

func TestBatchCrudFunctions(t *testing.T) {
	// Define the database path
	dbPath := "batch-test-db"

	// Open the Pebble database
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		log.Fatalf("failed to open Pebble database: %v", err)
	}
	defer db.Close() // Ensure the database is closed when the program exits

	// Define key-value pairs for testing
	key1 := []byte("key1")
	value1 := []byte("value1")

	// Test BatchCreateKeyValue function
	if err := BatchCreateKeyValue(db, key1, value1); err != nil {
		t.Errorf("BatchCreateKeyValue failed: %v", err)
	}

	// Test BatchReadKeyValue function
	retrievedValue, err := BatchReadKeyValue(db, key1)
	if err != nil {
		t.Errorf("BatchReadKeyValue failed: %v", err)
	}
	if string(retrievedValue) != string(value1) {
		t.Errorf("expected value %s, got %s", value1, retrievedValue)
	}

	// Test BatchUpdateKeyValue function
	newValue1 := []byte("new_value1")
	if err := BatchUpdateKeyValue(db, key1, newValue1); err != nil {
		t.Errorf("BatchUpdateKeyValue failed: %v", err)
	}

	retrievedValue, err = BatchReadKeyValue(db, key1)
	if err != nil {
		t.Errorf("BatchReadKeyValue failed: %v", err)
	}
	if string(retrievedValue) != string(newValue1) {
		t.Errorf("expected value %s, got %s", newValue1, retrievedValue)
	}

	// Test BatchDeleteKeyValue function
	if err := BatchDeleteKeyValue(db, key1); err != nil {
		t.Errorf("BatchDeleteKeyValue failed: %v", err)
	}

	retrievedValue, err = BatchReadKeyValue(db, key1)
	if err == nil {
		t.Errorf("expected error when reading deleted key, got none")
	}
	if retrievedValue != nil {
		t.Errorf("expected no value, got %s", retrievedValue)
	}

	// Test BatchReadKeyValueWorker function
	keys := []string{"key1", "key2"}
	results, err := BatchReadKeyValueWorker(db, keys)
	if err != nil {
		t.Errorf("BatchReadKeyValueWorker failed: %v", err)
	}

	for key, value := range results {
		// Since key1 is deleted, we expect it not to be present in results
		if key == "key1" {
			if len(value) != 0 {
				t.Errorf("expected key1 to be missing or empty, got %s", value)
			}
		}

		// For key2, since we haven't added it, we expect it to be missing or empty
		if key == "key2" {
			if len(value) != 0 {
				t.Errorf("expected key2 to be missing or empty, got %s", value)
			}
		}
	}
}
