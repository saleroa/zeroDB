package db

import (
	"bytes"
	"strconv"
	"strings"
	"time"
	"zeroDB/datastructure/list"
	"zeroDB/global/consts"
	"zeroDB/global/dberror"
	"zeroDB/storage"
)

// insert all the specified values at the head of the list stored at key.
// If key does not exist, it is created as empty list before performing the push operations.
func (db *DB) LPush(key []byte, values ...[]byte) (res int, err error) {
	if err = db.checkKeyValue(key, values...); err != nil {
		return
	}

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	for _, val := range values {
		e := storage.NewEntryNoExtra(key, val, consts.List, consts.ListLPush)
		if err = db.store(e); err != nil {
			return
		}

		res = db.listIndex.indexes.LPush(string(key), val)
	}
	return
}

// RPush insert all the specified values at the tail of the list stored at key.
// If key does not exist, it is created as empty list before performing the push operation.
func (db *DB) RPush(key []byte, values ...[]byte) (res int, err error) {
	if err = db.checkKeyValue(key, values...); err != nil {
		return
	}

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	for _, val := range values {
		e := storage.NewEntryNoExtra(key, val, consts.List, consts.ListRPush)
		if err = db.store(e); err != nil {
			return
		}

		res = db.listIndex.indexes.RPush(string(key), val)
	}
	return
}

// LPop removes and returns the first elements of the list stored at key.
func (db *DB) LPop(key []byte) ([]byte, error) {
	if err := db.checkKeyValue(key, nil); err != nil {
		return nil, err
	}

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.checkExpired(key, consts.List) {
		return nil, dberror.ErrKeyExpired
	}

	val := db.listIndex.indexes.LPop(string(key))
	if val != nil {
		e := storage.NewEntryNoExtra(key, val, consts.List, consts.ListLPop)
		if err := db.store(e); err != nil {
			return nil, err
		}
	}
	return val, nil
}

// Removes and returns the last elements of the list stored at key.
func (db *DB) RPop(key []byte) ([]byte, error) {
	if err := db.checkKeyValue(key, nil); err != nil {
		return nil, err
	}

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.checkExpired(key, consts.List) {
		return nil, dberror.ErrKeyExpired
	}

	val := db.listIndex.indexes.RPop(string(key))
	if val != nil {
		e := storage.NewEntryNoExtra(key, val, consts.List, consts.ListRPop)
		if err := db.store(e); err != nil {
			return nil, err
		}
	}
	return val, nil
}

// LIndex returns the element at index index in the list stored at key.
// The index is zero-based, so 0 means the first element, 1 the second element and so on.
// Negative indices can be used to designate elements starting at the tail of the list.
// Here, -1 means the last element, -2 means the penultimate and so forth.
func (db *DB) LIndex(key []byte, idx int) []byte {
	if err := db.checkKeyValue(key, nil); err != nil {
		return nil
	}

	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	return db.listIndex.indexes.LIndex(string(key), idx)
}

// LRem removes the first count occurrences of elements equal to element from the list stored at key.
// The count argument influences the operation in the following ways:
// count > 0: Remove elements equal to element moving from head to tail.
// count < 0: Remove elements equal to element moving from tail to head.
// count = 0: Remove all elements equal to element.
func (db *DB) LRem(key, value []byte, count int) (int, error) {
	if err := db.checkKeyValue(key, value); err != nil {
		return 0, nil
	}

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.checkExpired(key, consts.List) {
		return 0, dberror.ErrKeyExpired
	}

	res := db.listIndex.indexes.LRem(string(key), value, count)
	if res > 0 {
		c := strconv.Itoa(count)
		e := storage.NewEntry(key, value, []byte(c), consts.List, consts.ListLRem)
		if err := db.store(e); err != nil {
			return res, err
		}
	}
	return res, nil
}

// LInsert inserts element in the list stored at key either before or after the reference value pivot.
func (db *DB) LInsert(key string, option list.InsertOption, pivot, val []byte) (count int, err error) {
	if err = db.checkKeyValue([]byte(key), val); err != nil {
		return
	}

	if strings.Contains(string(pivot), consts.ExtraSeparator) {
		return 0, dberror.ErrExtraContainsSeparator
	}

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	count = db.listIndex.indexes.LInsert(key, option, pivot, val)
	if count != -1 {
		var buf bytes.Buffer
		buf.Write(pivot)
		buf.Write([]byte(consts.ExtraSeparator))
		opt := strconv.Itoa(int(option))
		buf.Write([]byte(opt))

		e := storage.NewEntry([]byte(key), val, buf.Bytes(), consts.List, consts.ListLInsert)
		if err = db.store(e); err != nil {
			return
		}
	}
	return
}

// LSet sets the list element at index to element.
// returns whether is successful.
func (db *DB) LSet(key []byte, idx int, val []byte) (ok bool, err error) {
	if err := db.checkKeyValue(key, val); err != nil {
		return false, err
	}

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if ok = db.listIndex.indexes.LSet(string(key), idx, val); ok {
		i := strconv.Itoa(idx)
		e := storage.NewEntry(key, val, []byte(i), consts.List, consts.ListLSet)
		if err := db.store(e); err != nil {
			return false, err
		}
	}
	return
}

// LTrim trim an existing list so that it will contain only the specified range of elements specified.
// Both start and stop are zero-based indexes, where 0 is the first element of the list (the head), 1 the next element and so on.
func (db *DB) LTrim(key []byte, start, end int) error {
	if err := db.checkKeyValue(key, nil); err != nil {
		return err
	}

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.checkExpired(key, consts.List) {
		return dberror.ErrKeyExpired
	}

	if res := db.listIndex.indexes.LTrim(string(key), start, end); res {
		var buf bytes.Buffer
		buf.Write([]byte(strconv.Itoa(start)))
		buf.Write([]byte(consts.ExtraSeparator))
		buf.Write([]byte(strconv.Itoa(end)))

		e := storage.NewEntry(key, nil, buf.Bytes(), consts.List, consts.ListLTrim)
		if err := db.store(e); err != nil {
			return err
		}
	}
	return nil
}

// LRange returns the specified elements of the list stored at key.
// The offsets start and stop are zero-based indexes, with 0 being the first element of the list (the head of the list), 1 being the next element and so on.
// These offsets can also be negative numbers indicating offsets starting at the end of the list.
// For example, -1 is the last element of the list, -2 the penultimate, and so on.
func (db *DB) LRange(key []byte, start, end int) ([][]byte, error) {
	if err := db.checkKeyValue(key, nil); err != nil {
		return nil, err
	}

	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	return db.listIndex.indexes.LRange(string(key), start, end), nil
}

// LLen returns the length of the list stored at key.
// If key does not exist, it is interpreted as an empty list and 0 is returned.
func (db *DB) LLen(key []byte) int {
	if err := db.checkKeyValue(key, nil); err != nil {
		return 0
	}

	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	return db.listIndex.indexes.LLen(string(key))
}

// LKeyExists check if the key of a List exists.
func (db *DB) LKeyExists(key []byte) (ok bool) {
	if err := db.checkKeyValue(key, nil); err != nil {
		return
	}

	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	if db.checkExpired(key, consts.List) {
		return false
	}

	ok = db.listIndex.indexes.LKeyExists(string(key))
	return
}

// LValExists check if the val exists in a specified List stored at key.
func (db *DB) LValExists(key []byte, val []byte) (ok bool) {
	if err := db.checkKeyValue(key, val); err != nil {
		return
	}

	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	if db.checkExpired(key, consts.List) {
		return false
	}

	ok = db.listIndex.indexes.LValExists(string(key), val)
	return
}

// LClear clear a specified key.
func (db *DB) LClear(key []byte) (err error) {
	if err = db.checkKeyValue(key, nil); err != nil {
		return
	}

	if !db.LKeyExists(key) {
		return dberror.ErrKeyNotExist
	}

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	e := storage.NewEntryNoExtra(key, nil, consts.List, consts.ListLClear)
	if err = db.store(e); err != nil {
		return err
	}

	db.listIndex.indexes.LClear(string(key))
	delete(db.expires[consts.List], string(key))
	return
}

// LExpire set expired time for a specified key of List.
func (db *DB) LExpire(key []byte, duration int64) (err error) {
	if duration <= 0 {
		return dberror.ErrInvalidTTL
	}
	if !db.LKeyExists(key) {
		return dberror.ErrKeyNotExist
	}

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	deadline := time.Now().Unix() + duration
	e := storage.NewEntryWithExpire(key, nil, deadline, consts.List, consts.ListLExpire)
	if err = db.store(e); err != nil {
		return err
	}

	db.expires[consts.List][string(key)] = deadline
	return
}

// LTTL return time to live.
func (db *DB) LTTL(key []byte) (ttl int64) {
	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	if db.checkExpired(key, consts.List) {
		return
	}

	deadline, exist := db.expires[consts.List][string(key)]
	if !exist {
		return
	}
	return deadline - time.Now().Unix()
}
