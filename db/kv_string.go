package db

import (
	"bytes"
	"log"
	"strings"
	"time"
	str "zeroDB/datastructure/string"
	"zeroDB/global/consts"
	"zeroDB/global/dberror"
	"zeroDB/global/utils"
	"zeroDB/storage"
)

// Set set key to hold the string value. If key already holds a value, it is overwritten.
// Any previous time to live associated with the key is discarded on successful Set operation.
func (db *DB) Set(key, value interface{}) error {
	encKey, encVal, err := db.encode(key, value)
	if err != nil {
		return err
	}
	return db.setVal(encKey, encVal)
}

// SetNx is short for "Set if not exists", set key to hold string value if key does not exist.
// In that case, it is equal to Set. When key already holds a value, no operation is performed.
func (db *DB) SetNx(key, value interface{}) (ok bool, err error) {
	encKey, encVal, err := db.encode(key, value)
	if err != nil {
		return false, err
	}
	if exist := db.StrExists(encKey); exist {
		return
	}

	if err = db.Set(encKey, encVal); err == nil {
		ok = true
	}
	return
}

// SetEx set key to hold the string value and set key to timeout after a given number of seconds.
func (db *DB) SetEx(key, value interface{}, duration int64) (err error) {
	if duration <= 0 {
		return dberror.ErrInvalidTTL
	}

	encKey, encVal, err := db.encode(key, value)
	if err != nil {
		return err
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	deadline := time.Now().Unix() + duration
	e := storage.NewEntryWithExpire(encKey, encVal, deadline, consts.String, consts.StringExpire)
	if err = db.store(e); err != nil {
		return
	}

	// set String index info, stored at skip list.
	if err = db.setStrData(e); err != nil {
		return
	}
	// set expired info.
	db.expires[consts.String][string(encKey)] = deadline
	return
}

// Get get the value of key. If the key does not exist an error is returned.
func (db *DB) Get(key, dest interface{}) error {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return err
	}

	if err := db.checkKeyValue(encKey, nil); err != nil {
		return err
	}

	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	val, err := db.getVal(encKey)
	if err != nil {
		return err
	}

	if len(val) > 0 {
		err = utils.DecodeValue(val, dest)
	}
	return err
}

// GetSet set key to value and returns the old value stored at key.
// If the key not exist, return an err.
func (db *DB) GetSet(key, value, dest interface{}) (err error) {
	err = db.Get(key, dest)
	if err != nil && err != dberror.ErrKeyNotExist && err != dberror.ErrKeyExpired {
		return
	}
	return db.Set(key, value)
}

// Append if key already exists and is a string, this command appends the value at the end of the string.
// If key does not exist it is created and set as an empty string, so Append will be similar to Set in this special case.
func (db *DB) Append(key interface{}, value string) (err error) {
	encKey, encVal, err := db.encode(key, value)
	if err != nil {
		return err
	}
	if err := db.checkKeyValue(encKey, encVal); err != nil {
		return err
	}

	var existVal []byte
	err = db.Get(key, &existVal)
	if err != nil && err != dberror.ErrKeyNotExist && err != dberror.ErrKeyExpired {
		return err
	}

	existVal = append(existVal, []byte(value)...)
	return db.Set(encKey, existVal)
}

// StrExists check whether the key exists.
func (db *DB) StrExists(key interface{}) bool {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return false
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return false
	}

	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	exist := db.strIndex.idxList.Exist(encKey)
	if exist && !db.checkExpired(encKey, consts.String) {
		return true
	}
	return false
}

// Remove remove the value stored at key.
func (db *DB) Remove(key interface{}) error {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return err
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return err
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	e := storage.NewEntryNoExtra(encKey, nil, consts.String, consts.StringRem)
	if err := db.store(e); err != nil {
		return err
	}

	db.strIndex.idxList.Remove(encKey)
	delete(db.expires[consts.String], string(encKey))
	return nil
}

// PrefixScan find the value corresponding to all matching keys based on the prefix.
// limit and offset control the range of value.
// if limit is negative, all matched values will return.
func (db *DB) PrefixScan(prefix string, limit, offset int) (val []interface{}, err error) {
	if limit <= 0 {
		return
	}
	if offset < 0 {
		offset = 0
	}
	if err = db.checkKeyValue([]byte(prefix), nil); err != nil {
		return
	}

	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	// Find the first matched key of the prefix.
	e := db.strIndex.idxList.FindPrefix([]byte(prefix))
	if limit > 0 {
		for i := 0; i < offset && e != nil && strings.HasPrefix(string(e.Key()), prefix); i++ {
			e = e.Next()
		}
	}

	for e != nil && strings.HasPrefix(string(e.Key()), prefix) && limit != 0 {
		item := e.Value().(*str.StrData)
		var value interface{}

		if item != nil {
			value = item.Meta.Value
		}

		// Check if the key is expired.
		expired := db.checkExpired(e.Key(), consts.String)
		if !expired {
			val = append(val, value)
			e = e.Next()
		}
		if limit > 0 && !expired {
			limit--
		}
	}
	return
}

// RangeScan find range of values from start to end.
func (db *DB) RangeScan(start, end interface{}) (val []interface{}, err error) {
	startKey, err := utils.EncodeKey(start)
	if err != nil {
		return nil, err
	}
	endKey, err := utils.EncodeKey(end)
	if err != nil {
		return nil, err
	}

	node := db.strIndex.idxList.Get(startKey)

	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	for node != nil && bytes.Compare(node.Key(), endKey) <= 0 {
		if db.checkExpired(node.Key(), consts.String) {
			node = node.Next()
			continue
		}

		var value interface{}

		value = node.Value().(*str.StrData).Meta.Value

		val = append(val, value)
		node = node.Next()
	}
	return
}

// Expire set the expiration time of the key.
func (db *DB) Expire(key interface{}, duration int64) (err error) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return err
	}
	if duration <= 0 {
		return dberror.ErrInvalidTTL
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	var value []byte
	if value, err = db.getVal(encKey); err != nil {
		return
	}

	deadline := time.Now().Unix() + duration
	e := storage.NewEntryWithExpire(encKey, value, deadline, consts.String, consts.StringExpire)
	if err = db.store(e); err != nil {
		return err
	}

	db.expires[consts.String][string(encKey)] = deadline
	return
}

// Persist clear expiration time.
func (db *DB) Persist(key interface{}) (err error) {
	var val interface{}
	if err = db.Get(key, &val); err != nil {
		return
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	encKey, encVal, err := db.encode(key, val)
	if err != nil {
		return err
	}
	e := storage.NewEntryNoExtra(encKey, encVal, consts.String, consts.StringPersist)
	if err = db.store(e); err != nil {
		return
	}

	delete(db.expires[consts.String], string(encKey))
	return
}

// TTL Time to live
func (db *DB) TTL(key interface{}) (ttl int64) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	deadline, exist := db.expires[consts.String][string(encKey)]
	if !exist {
		return
	}
	if expired := db.checkExpired(encKey, consts.String); expired {
		return
	}

	return deadline - time.Now().Unix()
}

func (db *DB) setVal(key, value []byte) (err error) {

	if err = db.checkKeyValue(key, value); err != nil {
		return err
	}

	var existVal []byte
	existVal, err = db.getVal(key)
	if err != nil && err != dberror.ErrKeyExpired && err != dberror.ErrKeyNotExist {
		return
	}

	if bytes.Compare(existVal, value) == 0 {
		return
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	e := storage.NewEntryNoExtra(key, value, consts.String, consts.StringSet)

	if err := db.store(e); err != nil {
		return err
	}

	// clear expire time.
	if _, ok := db.expires[consts.String][string(key)]; ok {
		delete(db.expires[consts.String], string(key))
	}

	// set String index info, stored at skip list.
	if err = db.setStrData(e); err != nil {
		return
	}
	log.Println(1)
	return
}

// set key and value in memmory, by KeyValueMem strategy

func (db *DB) setStrData(e *storage.Entry) error {
	activeFile, err := db.getActiveFile(consts.String)
	if err != nil {
		return err
	}
	// string indexes, stored in skiplist.
	idx := &str.StrData{
		Meta: &storage.Meta{
			Key:   e.Meta.Key,
			Value: e.Meta.Value,
		},
		FileId: activeFile.Id,
		Offset: activeFile.Offset - int64(e.Size()),
	}
	db.strIndex.idxList.Put(idx.Meta.Key, idx)
	return nil
}

func (db *DB) getVal(key []byte) ([]byte, error) {
	// Get index info from a skip list in memory.
	node := db.strIndex.idxList.Get(key)
	if node == nil {
		return nil, dberror.ErrKeyNotExist
	}

	idx := node.Value().(*str.StrData)
	if idx == nil {
		return nil, dberror.ErrNilStrData
	}

	// Check if the key is expired.
	if db.checkExpired(key, consts.String) {
		return nil, dberror.ErrKeyExpired
	}

	return idx.Meta.Value, nil
}
