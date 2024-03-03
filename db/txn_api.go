package db

import (
	"bytes"
	"encoding/binary"
	"time"

	"zeroDB/global/consts"
	"zeroDB/global/dberror"
	"zeroDB/global/utils"
	"zeroDB/storage"
)

// Set see db_str.go:Set
func (tx *Txn) Set(key, value interface{}) (err error) {
	encKey, encVal, err := tx.db.encode(key, value)
	if err != nil {
		return err
	}
	if err = tx.db.checkKeyValue(encKey, encVal); err != nil {
		return
	}

	e := storage.NewEntryWithTxn(encKey, encVal, nil, consts.String, consts.StringSet, tx.id)
	if err = tx.putEntry(e); err != nil {
		return
	}
	return
}

// SetNx see db_str.go:SetNx
func (tx *Txn) SetNx(key, value interface{}) (ok bool, err error) {
	encKey, encVal, err := tx.db.encode(key, value)
	if err != nil {
		return false, err
	}
	if err = tx.db.checkKeyValue(encKey, encVal); err != nil {
		return
	}

	if tx.StrExists(encKey) {
		return
	}
	if err = tx.Set(key, value); err == nil {
		ok = true
	}
	return
}

// SetEx see db_str.go:SetEx
func (tx *Txn) SetEx(key, value interface{}, duration int64) (err error) {
	encKey, encVal, err := tx.db.encode(key, value)
	if err != nil {
		return err
	}

	if err = tx.db.checkKeyValue(encKey, encVal); err != nil {
		return
	}
	if duration <= 0 {
		return dberror.ErrInvalidTTL
	}

	deadline := time.Now().Unix() + duration
	e := storage.NewEntryWithTxn(encKey, encVal, nil, consts.String, consts.StringExpire, tx.id)
	e.Timestamp = uint64(deadline)
	if err = tx.putEntry(e); err != nil {
		return
	}
	return
}

// Get see db_str.go:Get
func (tx *Txn) Get(key, dest interface{}) (err error) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return err
	}

	var val []byte
	if e, ok := tx.strEntries[string(encKey)]; ok {
		if e.GetMark() == consts.StringRem {
			err = dberror.ErrKeyNotExist
			return
		}
		if e.GetMark() == consts.StringExpire && e.Timestamp < uint64(time.Now().Unix()) {
			return
		}
		val = e.Meta.Value
	} else {
		val, err = tx.db.getVal(encKey)
	}

	if len(val) > 0 {
		err = utils.DecodeValue(val, dest)
	}
	return
}

// GetSet see db_str.go:GetSet
func (tx *Txn) GetSet(key, value, dest interface{}) (err error) {
	err = tx.Get(key, dest)
	if err != nil && err != dberror.ErrKeyNotExist && err != dberror.ErrKeyExpired {
		return
	}
	return tx.Set(key, value)
}

// Append see db_str.go:Append
func (tx *Txn) Append(key interface{}, value string) (err error) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return err
	}

	if e, ok := tx.strEntries[string(encKey)]; ok && e.GetMark() != consts.StringRem {
		e.Meta.Value = append(e.Meta.Value, value...)
		return
	}

	var existVal []byte
	err = tx.Get(key, &existVal)
	if err != nil && err != dberror.ErrKeyNotExist && err != dberror.ErrKeyExpired {
		return err
	}
	existVal = append(existVal, []byte(value)...)

	return tx.Set(key, existVal)
}

// StrExists see db_str.go:StrExists
func (tx *Txn) StrExists(key interface{}) (ok bool) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return false
	}

	if e, ok := tx.strEntries[string(encKey)]; ok && e.GetMark() != consts.StringRem {
		return true
	}
	if tx.db.checkExpired(encKey, consts.String) {
		return false
	}

	ok = tx.db.strIndex.idxList.Exist(encKey)
	return
}

// Remove see db_str.go:Remove
func (tx *Txn) Remove(key interface{}) (err error) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return err
	}
	if err = tx.db.checkKeyValue(encKey, nil); err != nil {
		return
	}

	if _, ok := tx.strEntries[string(encKey)]; ok {
		delete(tx.strEntries, string(encKey))
		return
	}

	e := storage.NewEntryWithTxn(encKey, nil, nil, consts.String, consts.StringRem, tx.id)
	if err = tx.putEntry(e); err != nil {
		return
	}
	return
}

// LPush see db_list.go:LPush
func (tx *Txn) LPush(key interface{}, values ...interface{}) (err error) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return err
	}

	for _, v := range values {
		var encVal []byte
		if encVal, err = utils.EncodeValue(v); err != nil {
			return
		}
		if err = tx.db.checkKeyValue(encKey, encVal); err != nil {
			return
		}

		e := storage.NewEntryWithTxn(encKey, encVal, nil, consts.List, consts.ListLPush, tx.id)
		if err = tx.putEntry(e); err != nil {
			return
		}
	}
	return
}

// RPush see db_list.go:RPush
func (tx *Txn) RPush(key interface{}, values ...interface{}) (err error) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return err
	}

	for _, v := range values {
		var encVal []byte
		if encVal, err = utils.EncodeValue(v); err != nil {
			return
		}
		if err = tx.db.checkKeyValue(encKey, encVal); err != nil {
			return
		}

		e := storage.NewEntryWithTxn(encKey, encVal, nil, consts.List, consts.ListRPush, tx.id)
		if err = tx.putEntry(e); err != nil {
			return
		}
	}
	return
}

// HSet see db_hash.go:HSet
func (tx *Txn) HSet(key, field, value interface{}) (err error) {
	encKey, encVal, err := tx.db.encode(key, value)
	if err != nil {
		return err
	}
	if err = tx.db.checkKeyValue(encKey, encVal); err != nil {
		return
	}

	encField, err := utils.EncodeValue(field)
	if err != nil {
		return err
	}

	// compare to the old val.
	oldVal, err := tx.hGetVal(key, field)
	if err != nil {
		return err
	}
	if bytes.Compare(encVal, oldVal) == 0 {
		return
	}

	e := storage.NewEntryWithTxn(encKey, encVal, encField, consts.Hash, consts.HashHSet, tx.id)
	if err = tx.putEntry(e); err != nil {
		return
	}

	eKey := tx.encodeKey(encVal, encField, consts.Hash)
	tx.keysMap[eKey] = len(tx.writeEntries) - 1
	return
}

// HSetNx see db_hash.go:HSetNx
func (tx *Txn) HSetNx(key, field, value interface{}) (err error) {
	if oldVal, err := tx.hGetVal(key, field); err == nil && len(oldVal) > 0 {
		return err
	}

	encKey, encVal, err := tx.db.encode(key, value)
	if err != nil {
		return err
	}
	if err = tx.db.checkKeyValue(encKey, encVal); err != nil {
		return
	}

	encField, err := utils.EncodeValue(field)
	if err != nil {
		return err
	}

	e := storage.NewEntryWithTxn(encKey, encVal, encField, consts.Hash, consts.HashHSet, tx.id)
	if err = tx.putEntry(e); err != nil {
		return
	}

	eKey := tx.encodeKey(encKey, encField, consts.Hash)
	tx.keysMap[eKey] = len(tx.writeEntries) - 1
	return
}

// HGet see db_hash.go:HGet
func (tx *Txn) HGet(key, field, dest interface{}) (err error) {
	val, err := tx.hGetVal(key, field)
	if err != nil {
		return err
	}
	if len(val) > 0 {
		err = utils.DecodeValue(val, dest)
	}
	return
}

func (tx *Txn) hGetVal(key, field interface{}) (val []byte, err error) {
	encKey, encField, err := tx.db.encode(key, field)
	if err != nil {
		return nil, err
	}

	eKey := tx.encodeKey(encKey, encField, consts.Hash)
	if idx, ok := tx.keysMap[eKey]; ok {
		entry := tx.writeEntries[idx]
		//only set and del will be putted into keysmap.
		//it it set ,if not del. just get the set val to return
		if entry.GetMark() == consts.HashHDel {
			return
		}

		val = entry.Meta.Value
		return
	}
	if tx.db.checkExpired(encKey, consts.Hash) {
		return
	}

	val = tx.db.hashIndex.indexes.HGet(string(encKey), string(encField))
	return
}

// HDel see db_hash.go:HDel
func (tx *Txn) HDel(key interface{}, fields ...interface{}) (err error) {
	if key == nil || len(fields) == 0 {
		return
	}

	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return err
	}
	if tx.db.checkExpired(encKey, consts.Hash) {
		return
	}

	for _, field := range fields {
		var encField []byte
		if encField, err = utils.EncodeValue(field); err != nil {
			return
		}

		eKey := tx.encodeKey(encKey, encField, consts.Hash)
		// del don't need do twice , set then del don't need to set.
		if idx, ok := tx.keysMap[eKey]; ok {
			tx.skipIds[idx] = struct{}{}
		}

		e := storage.NewEntryWithTxn(encKey, nil, encField, consts.Hash, consts.HashHDel, tx.id)
		if err = tx.putEntry(e); err != nil {
			return
		}
		tx.keysMap[eKey] = len(tx.writeEntries) - 1
	}
	return
}

// HExists see db_hash.go:HExists
func (tx *Txn) HExists(key, field interface{}) (ok bool) {
	encKey, encFiled, err := tx.db.encode(key, field)
	if err != nil {
		return false
	}

	eKey := tx.encodeKey(encKey, encFiled, consts.Hash)
	if idx, exist := tx.keysMap[eKey]; exist {
		if tx.writeEntries[idx].GetMark() == consts.HashHDel {
			return
		}
		return true
	}

	if tx.db.checkExpired(encKey, consts.Hash) {
		return
	}
	ok = tx.db.hashIndex.indexes.HExists(string(encKey), string(encFiled))
	return
}

// SAdd see db_set.go:SAdd
func (tx *Txn) SAdd(key interface{}, members ...interface{}) (err error) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return err
	}

	for _, mem := range members {
		var encMem []byte
		if encMem, err = utils.EncodeValue(mem); err != nil {
			return
		}
		if err = tx.db.checkKeyValue(encKey, encMem); err != nil {
			return
		}

		if !tx.SIsMember(key, mem) {
			e := storage.NewEntryWithTxn(encKey, encMem, nil, consts.Set, consts.SetSAdd, tx.id)
			if err = tx.putEntry(e); err != nil {
				return
			}

			encKey := tx.encodeKey(encKey, encMem, consts.Set)
			tx.keysMap[encKey] = len(tx.writeEntries) - 1
		}
	}
	return
}

// SIsMember see db_set.go:SIsMember
func (tx *Txn) SIsMember(key, member interface{}) (ok bool) {
	encKey, encMem, err := tx.db.encode(key, member)
	if err != nil {
		return
	}

	eKey := tx.encodeKey(encKey, encMem, consts.Set)
	if idx, exist := tx.keysMap[eKey]; exist {
		entry := tx.writeEntries[idx]
		if entry.GetMark() == consts.SetSRem {
			return
		}
		if bytes.Compare(entry.Meta.Value, encMem) == 0 {
			return true
		}
	}
	if tx.db.checkExpired(encKey, consts.Set) {
		return
	}

	ok = tx.db.setIndex.indexes.SIsMember(string(encKey), encMem)
	return
}

// SRem see db_set.go:SRem
func (tx *Txn) SRem(key interface{}, members ...[]interface{}) (err error) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return err
	}

	if tx.db.checkExpired(encKey, consts.Set) {
		return
	}
	for _, mem := range members {
		var encMem []byte
		if encMem, err = utils.EncodeValue(mem); err != nil {
			return
		}

		eKey := tx.encodeKey(encKey, encMem, consts.Set)
		if idx, ok := tx.keysMap[eKey]; ok {
			tx.skipIds[idx] = struct{}{}
		}
		e := storage.NewEntryWithTxn(encKey, encMem, nil, consts.Set, consts.SetSRem, tx.id)
		if err = tx.putEntry(e); err != nil {
			return
		}
		tx.keysMap[eKey] = len(tx.writeEntries) - 1
	}
	return
}

// ZScore see db_zset.go/ZAdd
func (tx *Txn) ZAdd(key interface{}, score float64, member interface{}) (err error) {
	encKey, encMember, err := tx.db.encode(key, member)
	if err != nil {
		return err
	}

	ok, oldScore, err := tx.ZScore(key, member)
	if err != nil {
		return err
	}
	if ok && oldScore == score {
		return
	}

	extra := []byte(utils.Float64ToStr(score))
	e := storage.NewEntryWithTxn(encKey, encMember, extra, consts.ZSet, consts.ZSetZAdd, tx.id)
	if err = tx.putEntry(e); err != nil {
		return
	}

	eKey := tx.encodeKey(encKey, encMember, consts.ZSet)
	tx.keysMap[eKey] = len(tx.writeEntries) - 1
	return
}

// ZScore see db_zset.go/ZScore
func (tx *Txn) ZScore(key, member interface{}) (exist bool, score float64, err error) {
	encKey, encMember, err := tx.db.encode(key, member)
	if err != nil {
		return false, 0, err
	}

	eKey := tx.encodeKey(encKey, encMember, consts.ZSet)
	if idx, ok := tx.keysMap[eKey]; ok {
		entry := tx.writeEntries[idx]
		if entry.GetMark() == consts.ZSetZRem {
			return
		}
		score, err = utils.StrToFloat64(string(entry.Meta.Extra))
		if err != nil {
			return
		}
	}
	if tx.db.checkExpired(encKey, consts.ZSet) {
		err = dberror.ErrKeyExpired
		return
	}

	exist, score = tx.db.zsetIndex.indexes.ZScore(string(encKey), string(encMember))
	return
}

// ZRem see db_zset.go/ZRem
func (tx *Txn) ZRem(key, member interface{}) (err error) {
	encKey, encMember, err := tx.db.encode(key, member)
	if err != nil {
		return err
	}
	if tx.db.checkExpired(encKey, consts.ZSet) {
		return
	}

	eKey := tx.encodeKey(encKey, encMember, consts.ZSet)
	if idx, ok := tx.keysMap[eKey]; ok {
		tx.skipIds[idx] = struct{}{}
	}

	e := storage.NewEntryWithTxn(encKey, encMember, nil, consts.ZSet, consts.ZSetZRem, tx.id)
	if err = tx.putEntry(e); err != nil {
		return
	}
	tx.keysMap[eKey] = len(tx.writeEntries) - 1
	return
}

func (tx *Txn) encodeKey(key, extra []byte, dType consts.DataType) string {
	keyLen, extraLen := len(key), len(extra)
	buf := make([]byte, keyLen+extraLen+2)

	binary.BigEndian.PutUint16(buf[:2], dType)
	copy(buf[2:keyLen+2], key)
	if extraLen > 0 {
		copy(buf[keyLen:keyLen+extraLen+2], extra)
	}
	return string(buf)
}
