package crud

import (
	"fmt"
	"log"

	"github.com/cockroachdb/pebble"
)

// CreateKeyValue adds a new key-value pair to the Pebble database.
func CreateKeyValue(db *pebble.DB, key, value []byte) error {
	// Check if the key or value is empty
	if len(key) == 0 {
		return fmt.Errorf("key must not be empty")
	}
	if len(value) == 0 {
		return fmt.Errorf("value must not be empty")
	}

	// Set the key-value pair in the database
	if err := db.Set(key, value, nil); err != nil {
		return fmt.Errorf("failed to create key-value pair: %w", err)
	}
	log.Printf("Created key-value pair: %s => %s", key, value)
	return nil
}

// ReadKeyValue retrieves the value associated with a given key from the Pebble database.
func ReadKeyValue(db *pebble.DB, key []byte) ([]byte, error) {
	// Check if the key is empty
	if len(key) == 0 {
		return nil, fmt.Errorf("key must not be empty")
	}

	// Retrieve the value for the given key
	value, closer, err := db.Get(key)
	if err != nil {
		return nil, fmt.Errorf("failed to get value for key: %w", err)
	}
	defer closer.Close() // Ensure the closer is closed properly

	log.Printf("Read value for key %s: %s", key, value)
	return value, nil
}

// UpdateKeyValue updates an existing key-value pair with a new value in the Pebble database.
func UpdateKeyValue(db *pebble.DB, key, newValue []byte) error {
	// Check if the key or value is empty
	if len(key) == 0 {
		return fmt.Errorf("key must not be empty")
	}
	if len(newValue) == 0 {
		return fmt.Errorf("value must not be empty")
	}

	// Update the key-value pair in the database
	if err := db.Set(key, newValue, nil); err != nil {
		return fmt.Errorf("failed to update key-value pair: %w", err)
	}
	log.Printf("Updated key-value pair: %s => %s", key, newValue)
	return nil
}

// DeleteKeyValue removes a key-value pair associated with a given key from the Pebble database.
func DeleteKeyValue(db *pebble.DB, key []byte) error {
	// Check if the key is empty
	if len(key) == 0 {
		return fmt.Errorf("key must not be empty")
	}

	// Delete the key-value pair from the database
	if err := db.Delete(key, nil); err != nil {
		return fmt.Errorf("failed to delete key-value pair: %w", err)
	}
	log.Printf("Deleted key-value pair: %s", key)
	return nil
}
