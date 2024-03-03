package db

import (
	"log"
	"sync/atomic"
	"time"
	"zeroDB/global/consts"
	"zeroDB/global/dberror"
	"zeroDB/global/utils"
	"zeroDB/storage"
)

// 检查key和value是否过大，过大无效
func (db *DB) checkKeyValue(key []byte, value ...[]byte) error {
	keySize := uint32(len(key))
	if keySize == 0 {
		return dberror.ErrEmptyKey
	}

	config := db.config
	if keySize > config.MaxKeySize {
		return dberror.ErrKeyTooLarge
	}
	for _, v := range value {
		if uint32(len(v)) > config.MaxValueSize {
			return dberror.ErrValueTooLarge
		}
	}
	return nil
}

// 检查key是否过期，过期则删除
func (db *DB) checkExpired(key []byte, dataType consts.DataType) (expired bool) {
	deadline, exist := db.expires[dataType][string(key)]
	if !exist {
		return
	}
	//已经过期
	if time.Now().Unix() > deadline {
		expired = true
		var e *storage.Entry
		switch dataType {

		case consts.String:
			// 将删除信息记入entry
			e = storage.NewEntryNoExtra(key, nil, consts.String, consts.StringRem)
			//删除内存中的记录
			db.strIndex.idxList.Remove(key)
		case consts.Set:
			e = storage.NewEntryNoExtra(key, nil, consts.Set, consts.SetSClear)
			db.setIndex.indexes.SClear(string(key))
		case consts.ZSet:
			e = storage.NewEntryNoExtra(key, nil, consts.ZSet, consts.ZSetZClear)
			db.zsetIndex.indexes.ZClear(string(key))
		case consts.List:
			e = storage.NewEntryNoExtra(key, nil, consts.List, consts.ListLClear)
			db.listIndex.indexes.LClear(string(key))
		case consts.Hash:
			e = storage.NewEntryNoExtra(key, nil, consts.Hash, consts.HashHClear)
			db.hashIndex.indexes.HClear(string(key))
		}
		if err := db.store(e); err != nil {
			log.Println("checkExpired: store entry err :", err)
			return
		}
		//删除key的过期信息
		delete(db.expires[dataType], string(key))
	}
	return
}

// 将entry写进dbfile里,并根据配置持久化处理
func (db *DB) store(e *storage.Entry) error {
	// sync the db file if file size is not enough, and open a new db file.
	config := db.config
	activeFile, err := db.getActiveFile(e.GetType())
	if err != nil {
		return err
	}

	if activeFile.Offset+int64(e.Size()) > config.BlockSize {
		if err := activeFile.Sync(); err != nil {
			return err
		}

		// save the old db file as arched file.
		activeFileId := activeFile.Id
		db.archFiles[e.GetType()][activeFileId] = activeFile

		newDbFile, err := storage.NewDBFile(config.DirPath, activeFileId+1, e.GetType())
		if err != nil {
			return err
		}
		activeFile = newDbFile
	}

	// 将entry写进dbfile
	if err := activeFile.Write(e); err != nil {
		return err
	}
	db.activeFile.Store(e.GetType(), activeFile)

	// 根据配置持久化处理dbfile
	if config.Sync {
		if err := activeFile.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// 将 key，value 转化成 []byte
func (db *DB) encode(key, value interface{}) (encKey, encVal []byte, err error) {
	if encKey, err = utils.EncodeKey(key); err != nil {
		return
	}
	if encVal, err = utils.EncodeValue(value); err != nil {
		return
	}
	return
}

// 判断 db 是否关闭
func (db *DB) isClosed() bool {
	return atomic.LoadUint32(&db.closed) == 1
}

// 持久化文件
func (db *DB) Sync() (err error) {
	if db == nil || db.activeFile == nil {
		return nil
	}

	db.activeFile.Range(func(key, value interface{}) bool {
		if dbFile, ok := value.(*storage.DBFile); ok {
			if err = dbFile.Sync(); err != nil {
				return false
			}
		}
		return true
	})
	if err != nil {
		return
	}
	return
}
