package crud

import (
	"fmt"
	"github.com/cockroachdb/pebble"
)

// CreateKeyValue adds a new key-value pair to the database.
func CreateKeyValue(db *pebble.DB, key, value []byte) error {
	err := db.Set(key, value, nil)
	if err != nil {
		return fmt.Errorf("failed to create key-value pair: %w", err)
	}
	return nil
}

// ReadKeyValue retrieves the value associated with a key.
func ReadKeyValue(db *pebble.DB, key []byte) ([]byte, error) {
	retrievedValue, closer, err := db.Get(key)
	if err != nil {
		return nil, fmt.Errorf("failed to get value for key: %w", err)
	}
	defer closer.Close() // Ensure the iterator is closed
	return retrievedValue, nil
}

// UpdateKeyValue updates an existing key-value pair with a new value.
func UpdateKeyValue(db *pebble.DB, key, newValue []byte) error {
	err := db.Set(key, newValue, nil)
	if err != nil {
		return fmt.Errorf("failed to update key-value pair: %w", err)
	}
	return nil
}

// DeleteKeyValue removes a key-value pair from the database.
func DeleteKeyValue(db *pebble.DB, key []byte) error {
	err := db.Delete(key, nil)
	if err != nil {
		return fmt.Errorf("failed to delete key-value pair: %w", err)
	}
	return nil
}
