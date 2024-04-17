package crud

import (
	"testing"
)

func TestBatchCrudFunctions(t *testing.T) {
	db := SetupDB()
	defer db.Close()

	// Define key-value pairs for testing
	key1 := []byte("key1")
	value1 := []byte("value1")
	key2 := []byte("key2")
	value2 := []byte("value2")
	nonExistingKey := []byte("non_existing_key")
	newValue1 := []byte("new_value1")

	// Test BatchCreateKeyValue function
	if err := BatchCreateKeyValue(db, key1, value1); err != nil {
		t.Errorf("BatchCreateKeyValue failed: %v", err)
	}
	if err := BatchCreateKeyValue(db, key2, value2); err != nil {
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

	retrievedValue, err = BatchReadKeyValue(db, key2)
	if err != nil {
		t.Errorf("BatchReadKeyValue failed: %v", err)
	}
	if string(retrievedValue) != string(value2) {
		t.Errorf("expected value %s, got %s", value2, retrievedValue)
	}

	// Test reading a non-existing key
	_, err = BatchReadKeyValue(db, nonExistingKey)
	if err == nil {
		t.Errorf("expected error when reading non-existing key, got none")
	}

	// Test BatchUpdateKeyValue function
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

	// Test updating a non-existing key
	err = BatchUpdateKeyValue(db, nonExistingKey, newValue1)
	if err != nil {
		t.Errorf("BatchUpdateKeyValue failed when updating non-existing key: %v", err)
	}

	// Test BatchDeleteKeyValue function
	if err := BatchDeleteKeyValue(db, key1); err != nil {
		t.Errorf("BatchDeleteKeyValue failed: %v", err)
	}

	// Ensure the key is deleted
	retrievedValue, err = BatchReadKeyValue(db, key1)
	if err == nil {
		t.Errorf("expected error when reading deleted key, got none")
	}
	if retrievedValue != nil {
		t.Errorf("expected no value, got %s", retrievedValue)
	}

	// Test deleting a non-existing key
	err = BatchDeleteKeyValue(db, nonExistingKey)
	if err != nil {
		t.Errorf("BatchDeleteKeyValue failed when deleting non-existing key: %v", err)
	}

	// Test BatchReadKeyValueWorker function
	keys := []string{"key1", "key2", "non_existing_key"}
	results, err := BatchReadKeyValueWorker(db, keys)
	if err != nil {
		t.Errorf("BatchReadKeyValueWorker failed: %v", err)
	}
	for key, value := range results {
		// Check that the deleted key is missing
		if key == "key1" {
			if len(value) != 0 {
				t.Errorf("expected key1 to be missing or empty, got %s", value)
			}
		}
		// Check other keys
		if key == "key2" {
			if string(value) != string(value2) {
				t.Errorf("expected value %s, got %s", value2, value)
			}
		}
		// Check non-existing key
		if key == "non_existing_key" {
			if len(value) != 0 {
				t.Errorf("expected non-existing key to be missing or empty, got %s", value)
			}
		}
	}
}
