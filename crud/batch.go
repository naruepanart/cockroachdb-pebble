package crud

import (
	"fmt"
	"sync"

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
	value, closer, err := db.Get(key)
	if err != nil {
		return nil, fmt.Errorf("failed to get value for key: %w", err)
	}
	defer closer.Close()

	return value, nil
}

// handles multiple read operations concurrently using worker pools.
func BatchReadKeyValueWorker(db *pebble.DB, keys []string) (map[string][]byte, error) {
	results := make(map[string][]byte)
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(len(keys))

	for _, key := range keys {
		go func(key string) {
			defer wg.Done()
			value, closer, err := db.Get([]byte(key))
			if err != nil {
				// Handle error appropriately
				return
			}
			defer closer.Close()

			mu.Lock()
			results[key] = value
			mu.Unlock()
		}(key)
	}

	wg.Wait()

	return results, nil
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
