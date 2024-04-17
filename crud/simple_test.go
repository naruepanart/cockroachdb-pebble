package crud

import (
	"testing"
)

func TestCrudFunctions(t *testing.T) {
	db := SetupDB()
	defer db.Close()

	// Define key-value pairs for testing
	key1 := []byte("key1")
	value1 := []byte("value1")

	nonExistentKey := []byte("non_existent_key")

	// Test CreateKeyValue function
	if err := CreateKeyValue(db, key1, value1); err != nil {
		t.Errorf("CreateKeyValue failed: %v", err)
	}

	// Test creating a key-value pair with an empty key
	if err := CreateKeyValue(db, []byte(""), value1); err == nil {
		t.Errorf("expected error for empty key, got none")
	}

	// Test creating a key-value pair with an empty value
	if err := CreateKeyValue(db, key1, []byte("")); err == nil {
		t.Errorf("expected error for empty value, got none")
	}

	// Test ReadKeyValue function
	retrievedValue, err := ReadKeyValue(db, key1)
	if err != nil {
		t.Errorf("ReadKeyValue failed: %v", err)
	}
	if string(retrievedValue) != string(value1) {
		t.Errorf("expected value %s, got %s", value1, retrievedValue)
	}

	// Test ReadKeyValue function with empty key
	retrievedValue, err = ReadKeyValue(db, []byte(""))
	if err == nil {
		t.Errorf("expected error for empty key, got none")
	}
	if retrievedValue != nil {
		t.Errorf("expected no value for empty key, got %s", retrievedValue)
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

	// Test UpdateKeyValue function with empty key
	if err := UpdateKeyValue(db, []byte(""), newValue1); err == nil {
		t.Errorf("expected error for empty key, got none")
	}

	// Test UpdateKeyValue function with empty value
	if err := UpdateKeyValue(db, key1, []byte("")); err == nil {
		t.Errorf("expected error for empty value, got none")
	}

	// Test DeleteKeyValue function
	if err := DeleteKeyValue(db, key1); err != nil {
		t.Errorf("DeleteKeyValue failed: %v", err)
	}

	// Test handling of non-existing key
	retrievedValue, err = ReadKeyValue(db, nonExistentKey)
	if err == nil {
		t.Errorf("expected error when reading non-existing key, got none")
	}
	if retrievedValue != nil {
		t.Errorf("expected no value for non-existing key, got %s", retrievedValue)
	}

	// Test DeleteKeyValue function with empty key
	if err := DeleteKeyValue(db, []byte("")); err == nil {
		t.Errorf("expected error for empty key, got none")
	}
}
