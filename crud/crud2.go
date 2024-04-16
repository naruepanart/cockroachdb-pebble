package crud

import (
	"fmt"
	"github.com/cockroachdb/pebble"
)

// adds a new key-value pair to the database using a batch operation for improved performance.
func BatchCreateKeyValue(db *pebble.DB, key, value []byte) error {
	batch := db.NewBatch()
	defer batch.Close()

	err := batch.Set(key, value, nil)
	if err != nil {
		return fmt.Errorf("failed to create key-value pair: %w", err)
	}

	// Commit the batch
	if err := batch.Commit(nil); err != nil {
		return fmt.Errorf("failed to commit batch: %w", err)
	}

	return nil
}

// retrieves the value associated with a key and properly handles the closer.
func BatchReadKeyValue(db *pebble.DB, key []byte) ([]byte, error) {
	retrievedValue, closer, err := db.Get(key)
	if err != nil {
		return nil, fmt.Errorf("failed to get value for key: %w", err)
	}
	defer func() {
		if err := closer.Close(); err != nil {
			fmt.Println("Failed to close closer:", err)
		}
	}()

	return retrievedValue, nil
}

// updates an existing key-value pair with a new value using a batch operation for improved performance.
func BatchUpdateKeyValue(db *pebble.DB, key, newValue []byte) error {
	batch := db.NewBatch()
	defer batch.Close()

	err := batch.Set(key, newValue, nil)
	if err != nil {
		return fmt.Errorf("failed to update key-value pair: %w", err)
	}

	// Commit the batch
	if err := batch.Commit(nil); err != nil {
		return fmt.Errorf("failed to commit batch: %w", err)
	}

	return nil
}

// removes a key-value pair from the database using a batch operation for improved performance.
func BatchDeleteKeyValue(db *pebble.DB, key []byte) error {
	batch := db.NewBatch()
	defer batch.Close()

	err := batch.Delete(key, nil)
	if err != nil {
		return fmt.Errorf("failed to delete key-value pair: %w", err)
	}

	// Commit the batch
	if err := batch.Commit(nil); err != nil {
		return fmt.Errorf("failed to commit batch: %w", err)
	}

	return nil
}