package crud

import (
	"fmt"
	"github.com/cockroachdb/pebble"
)

func BatchCreateKeyValue(batch *pebble.Batch, key, value []byte) error {
	return batch.Set(key, value, pebble.NoSync)
}

func BatchReadKeyValue(db *pebble.DB, key []byte) ([]byte, error) {
	value, closer, err := db.Get(key)
	if err != nil {
		return nil, fmt.Errorf("failed to get value for key: %w", err)
	}
	defer closer.Close()
	return value, nil
}

func BatchUpdateKeyValue(batch *pebble.Batch, key, value []byte) error {
	return batch.Set(key, value, pebble.NoSync)
}

func BatchDeleteKeyValue(batch *pebble.Batch, key []byte) error {
	return batch.Delete(key, pebble.NoSync)
}
