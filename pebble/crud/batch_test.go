package crud

import (
	"github.com/cockroachdb/pebble"
	"log"
	"testing"
)

func TestBatchCrudFunctions(t *testing.T) {
	db, err := ConnPebbleDB()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create a new batch.
	batch := db.NewBatch()
	defer batch.Close() // Ensure the batch is closed at the end of the test.

	// Define key and value to be used for testing.
	key := []byte("test_key")
	value := []byte("test_value")

	// Test BatchCreateKeyValue function.
	if err := BatchCreateKeyValue(batch, key, value); err != nil {
		t.Fatalf("BatchCreateKeyValue failed: %v", err)
	}
	// Apply the batch after creation.
	if err := batch.Commit(pebble.NoSync); err != nil {
		t.Fatalf("Batch commit failed after BatchCreateKeyValue: %v", err)
	}

	// Check if the key-value pair was created successfully.
	gotValue, closer, err := db.Get(key)
	if err != nil {
		t.Fatalf("Error fetching key-value: %v", err)
	}
	defer closer.Close()
	if gotValue == nil || string(gotValue) != string(value) {
		t.Errorf("Expected value %s, but got %s", value, gotValue)
	}

	// Create a new batch for update operation.
	batch = db.NewBatch()
	defer batch.Close()

	// Test BatchUpdateKeyValue function by updating the value.
	newValue := []byte("updated_value")
	if err := BatchUpdateKeyValue(batch, key, newValue); err != nil {
		t.Fatalf("BatchUpdateKeyValue failed: %v", err)
	}
	// Apply the batch after update.
	if err := batch.Commit(pebble.NoSync); err != nil {
		t.Fatalf("Batch commit failed after BatchUpdateKeyValue: %v", err)
	}

	// Check if the key-value pair was updated successfully.
	gotValue, closer, err = db.Get(key)
	if err != nil {
		t.Fatalf("Error fetching key-value: %v", err)
	}
	defer closer.Close()
	if gotValue == nil || string(gotValue) != string(newValue) {
		t.Errorf("Expected updated value %s, but got %s", newValue, gotValue)
	}

	// Create a new batch for delete operation.
	batch = db.NewBatch()
	defer batch.Close()

	// Test BatchDeleteKeyValue function.
	if err := BatchDeleteKeyValue(batch, key); err != nil {
		t.Fatalf("BatchDeleteKeyValue failed: %v", err)
	}
	// Apply the batch after delete.
	if err := batch.Commit(pebble.NoSync); err != nil {
		t.Fatalf("Batch commit failed after BatchDeleteKeyValue: %v", err)
	}
}
