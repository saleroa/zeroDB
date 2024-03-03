package db

import (
	"bytes"
	"time"
	"zeroDB/global/consts"
	"zeroDB/global/dberror"
	"zeroDB/storage"
)

// hash apis

// Get returns the value associated with field in the hash stored at key.
func (db *DB) HGet(key, field []byte) []byte {
	if err := db.checkKeyValue(key, nil); err != nil {
		return nil
	}
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()
	// all get request need to check whether key expired
	if db.checkExpired(key, consts.Hash) {
		return nil
	}
	return db.hashIndex.indexes.HGet(string(key), string(field))
}

// sets field in the hash stored at key to value.
func (db *DB) HSet(key []byte, field []byte, value []byte) (res int, err error) {
	if err = db.checkKeyValue(key, value); err != nil {
		return
	}
	// If the existed value is the same as the set value, nothing will be done.
	oldVal := db.HGet(key, field)
	if bytes.Compare(oldVal, value) == 0 {
		return
	}

	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	e := storage.NewEntry(key, value, field, consts.Hash, consts.HashHSet)
	if err = db.store(e); err != nil {
		return
	}
	res = db.hashIndex.indexes.HSet(string(key), string(field), value)
	return

}

// sets field in the hash stored at key to value, only if field does not yet exist.
func (db *DB) HSetNx(key, field, value []byte) (res int, err error) {
	if err = db.checkKeyValue(key, value); err != nil {
		return
	}
	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	if res = db.hashIndex.indexes.HSetNx(string(key), string(field), value); res == 1 {
		e := storage.NewEntry(key, value, field, consts.Hash, consts.HashHSet)
		if err = db.store(e); err != nil {
			return
		}
	}
	return
}

// returns all fields and values of the hash stored at key
func (db *DB) HGetAll(key []byte) [][]byte {
	if err := db.checkKeyValue(key, nil); err != nil {
		return nil
	}
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.checkExpired(key, consts.Hash) {
		return nil
	}
	return db.hashIndex.indexes.HGetAll(string(key))
}

// removes the specified fields from the hash stored at key.
func (db *DB) HDel(key []byte, field ...[]byte) (res int, err error) {
	if err = db.checkKeyValue(key, nil); err != nil {
		return
	}
	if field == nil || len(field) == 0 {
		return
	}

	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	for _, f := range field {
		if ok := db.hashIndex.indexes.HDel(string(key), string(f)); ok == 1 {
			//remove it
			e := storage.NewEntry(key, nil, f, consts.Hash, consts.HashHDel)
			if err = db.store(e); err != nil {
				return
			}
			res++
		}
	}
	return
}

// returns if the key is existed in hash.
func (db *DB) HKeyExists(key []byte) (ok bool) {
	if err := db.checkKeyValue(key, nil); err != nil {
		return
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.checkExpired(key, consts.Hash) {
		return
	}
	return db.hashIndex.indexes.HKeyExists(string(key))
}

// returns if field is an existing field in the hash
func (db *DB) HExists(key, field []byte) (ok bool) {
	if err := db.checkKeyValue(key, nil); err != nil {
		return
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.checkExpired(key, consts.Hash) {
		return
	}

	return db.hashIndex.indexes.HExists(string(key), string(field))
}

// returns the number of fields contained in the hash stored at key.
func (db *DB) HLen(key []byte) int {
	if err := db.checkKeyValue(key, nil); err != nil {
		return 0
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.checkExpired(key, consts.Hash) {
		return 0
	}

	return db.hashIndex.indexes.HLen(string(key))
}

// returns all field names in the hash stored at key.
func (db *DB) HKeys(key []byte) (val []string) {
	if err := db.checkKeyValue(key, nil); err != nil {
		return
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.checkExpired(key, consts.Hash) {
		return nil
	}

	return db.hashIndex.indexes.HKeys(string(key))
}

// returns all values in the hash stored at key.
func (db *DB) HVals(key []byte) (val [][]byte) {
	if err := db.checkKeyValue(key, nil); err != nil {
		return
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.checkExpired(key, consts.Hash) {
		return nil
	}

	return db.hashIndex.indexes.HVals(string(key))
}

// HClear clear the key in hash.
func (db *DB) HClear(key []byte) (err error) {
	if err = db.checkKeyValue(key, nil); err != nil {
		return
	}

	if !db.HKeyExists(key) {
		return dberror.ErrKeyNotExist
	}

	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	e := storage.NewEntryNoExtra(key, nil, consts.HashHClear, consts.Hash)
	if err := db.store(e); err != nil {
		return err
	}

	db.hashIndex.indexes.HClear(string(key))
	delete(db.expires[consts.Hash], string(key))
	return
}

// HExpire set expired time for a hash key.
func (db *DB) HExpire(key []byte, duration int64) (err error) {
	if duration <= 0 {
		return dberror.ErrInvalidTTL
	}
	if err = db.checkKeyValue(key, nil); err != nil {
		return
	}
	if !db.HKeyExists(key) {
		return dberror.ErrKeyNotExist
	}

	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	deadline := time.Now().Unix() + duration
	e := storage.NewEntryWithExpire(key, nil, deadline, consts.Hash, consts.Hash)
	if err := db.store(e); err != nil {
		return err
	}

	db.expires[consts.Hash][string(key)] = deadline
	return
}

// return time to live for the key.
func (db *DB) HTTL(key []byte) (ttl int64) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.checkExpired(key, consts.Hash) {
		return
	}

	deadline, exist := db.expires[consts.Hash][string(key)]
	if !exist {
		return
	}
	return deadline - time.Now().Unix()
}
