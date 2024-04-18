package main

import (
	"github.com/cockroachdb/pebble"
)

func BatchCreateKeyValue(batch *pebble.Batch, key, value []byte) error {
	return batch.Set(key, value, pebble.NoSync)
}

func BatchUpdateKeyValue(batch *pebble.Batch, key, value []byte) error {
	return batch.Set(key, value, pebble.NoSync)
}

func BatchDeleteKeyValue(batch *pebble.Batch, key []byte) error {
	return batch.Delete(key, pebble.NoSync)
}
