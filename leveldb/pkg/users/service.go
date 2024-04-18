package users

import "github.com/syndtr/goleveldb/leveldb"

func Create(db *leveldb.DB, key, value []byte) error {
	err := db.Put(key, value, nil)
	if err != nil {
		return err
	}
	return nil
}

func FindOne(db *leveldb.DB, key []byte) ([]byte, error) {
	value, err := db.Get(key, nil)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func Update(db *leveldb.DB, key, value []byte) error {
	err := db.Put(key, value, nil)
	if err != nil {
		return err
	}
	return nil
}

func Remove(db *leveldb.DB, key []byte) error {
	err := db.Delete(key, nil)
	if err != nil {
		return err
	}
	return nil
}
