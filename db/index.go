package db

import (
	"io"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"zeroDB/datastructure/list"
	str "zeroDB/datastructure/string"
	"zeroDB/global/consts"
	"zeroDB/global/utils"
	"zeroDB/storage"
)

// 将一个string的entry构建成内存中的 string index
// idx 才是储存string数据的部分，加载其中的信息到string index
func (db *DB) buildStringIndex(idx *str.StrData, entry *storage.Entry) {
	if db.strIndex == nil || idx == nil {
		return
	}

	switch entry.GetMark() {
	case consts.StringSet:
		db.strIndex.idxList.Put(idx.Meta.Key, idx)
	case consts.StringRem:
		db.strIndex.idxList.Remove(idx.Meta.Key)
	case consts.StringExpire:
		if entry.Timestamp < uint64(time.Now().Unix()) {
			db.strIndex.idxList.Remove(idx.Meta.Key)
		} else {
			db.expires[consts.String][string(idx.Meta.Key)] = int64(entry.Timestamp)
			db.strIndex.idxList.Put(idx.Meta.Key, idx)
		}
	case consts.StringPersist:
		db.strIndex.idxList.Put(idx.Meta.Key, idx)
		delete(db.expires[consts.String], string(idx.Meta.Key))
	}
}

// 将一个list的entry构建成内存中的 list index
func (db *DB) buildListIndex(entry *storage.Entry) {
	if db.listIndex == nil || entry == nil {
		return
	}

	key := string(entry.Meta.Key)
	switch entry.GetMark() {
	case consts.ListLPush:
		db.listIndex.indexes.LPush(key, entry.Meta.Value)
	case consts.ListLPop:
		db.listIndex.indexes.LPop(key)
	case consts.ListRPush:
		db.listIndex.indexes.RPush(key, entry.Meta.Value)
	case consts.ListRPop:
		db.listIndex.indexes.RPop(key)
	case consts.ListLRem:
		if count, err := strconv.Atoi(string(entry.Meta.Extra)); err == nil {
			db.listIndex.indexes.LRem(key, entry.Meta.Value, count)
		}
	case consts.ListLInsert:
		extra := string(entry.Meta.Extra)
		s := strings.Split(extra, consts.ExtraSeparator)
		if len(s) == 2 {
			pivot := []byte(s[0])
			if opt, err := strconv.Atoi(s[1]); err == nil {
				db.listIndex.indexes.LInsert(string(entry.Meta.Key), list.InsertOption(opt), pivot, entry.Meta.Value)
			}
		}
	case consts.ListLSet:
		if i, err := strconv.Atoi(string(entry.Meta.Extra)); err == nil {
			db.listIndex.indexes.LSet(key, i, entry.Meta.Value)
		}
	case consts.ListLTrim:
		extra := string(entry.Meta.Extra)
		s := strings.Split(extra, consts.ExtraSeparator)
		if len(s) == 2 {
			start, _ := strconv.Atoi(s[0])
			end, _ := strconv.Atoi(s[1])

			db.listIndex.indexes.LTrim(string(entry.Meta.Key), start, end)
		}
	case consts.ListLExpire:
		if entry.Timestamp < uint64(time.Now().Unix()) {
			db.listIndex.indexes.LClear(key)
		} else {
			db.expires[consts.List][key] = int64(entry.Timestamp)
		}
	case consts.ListLClear:
		db.listIndex.indexes.LClear(key)
	}
}

// 将一个hash的entry构建成内存中的 hash index
func (db *DB) buildHashIndex(entry *storage.Entry) {
	if db.hashIndex == nil || entry == nil {
		return
	}

	key := string(entry.Meta.Key)
	switch entry.GetMark() {
	case consts.HashHSet:
		db.hashIndex.indexes.HSet(key, string(entry.Meta.Extra), entry.Meta.Value)
	case consts.HashHDel:
		db.hashIndex.indexes.HDel(key, string(entry.Meta.Extra))
	case consts.HashHClear:
		db.hashIndex.indexes.HClear(key)
	case consts.HashHExpire:
		if entry.Timestamp < uint64(time.Now().Unix()) {
			db.hashIndex.indexes.HClear(key)
		} else {
			db.expires[consts.Hash][key] = int64(entry.Timestamp)
		}
	}
}

// 将一个set的entry构建成内存中的 set index
func (db *DB) buildSetIndex(entry *storage.Entry) {
	if db.hashIndex == nil || entry == nil {
		return
	}

	key := string(entry.Meta.Key)
	switch entry.GetMark() {
	case consts.SetSAdd:
		db.setIndex.indexes.SAdd(key, entry.Meta.Value)
	case consts.SetSRem:
		db.setIndex.indexes.SRem(key, entry.Meta.Value)
	case consts.SetSMove:
		extra := entry.Meta.Extra
		db.setIndex.indexes.SMove(key, string(extra), entry.Meta.Value)
	case consts.SetSClear:
		db.setIndex.indexes.SClear(key)
	case consts.SetSExpire:
		if entry.Timestamp < uint64(time.Now().Unix()) {
			db.setIndex.indexes.SClear(key)
		} else {
			db.expires[consts.Set][key] = int64(entry.Timestamp)
		}
	}
}

// 将一个zset的entry构建成内存中的 zset index
func (db *DB) buildZsetIndex(entry *storage.Entry) {
	if db.hashIndex == nil || entry == nil {
		return
	}

	key := string(entry.Meta.Key)
	switch entry.GetMark() {
	case consts.ZSetZAdd:
		if score, err := utils.StrToFloat64(string(entry.Meta.Extra)); err == nil {
			db.zsetIndex.indexes.ZAdd(key, score, string(entry.Meta.Value))
		}
	case consts.ZSetZRem:
		db.zsetIndex.indexes.ZRem(key, string(entry.Meta.Value))
	case consts.ZSetZClear:
		db.zsetIndex.indexes.ZClear(key)
	case consts.ZSetZExpire:
		if entry.Timestamp < uint64(time.Now().Unix()) {
			db.zsetIndex.indexes.ZClear(key)
		} else {
			db.expires[consts.ZSet][key] = int64(entry.Timestamp)
		}
	}
}

// 从dbfile中加载所有数据类型的index
func (db *DB) loadIdxFromFiles() error {
	if db.archFiles == nil && db.activeFile == nil {
		return nil
	}

	wg := sync.WaitGroup{}
	wg.Add(consts.DataStructureNum)
	for dataType := 0; dataType < consts.DataStructureNum; dataType++ {
		go func(dType uint16) {
			defer wg.Done()

			// archived files
			var fileIds []int
			dbFile := make(map[uint32]*storage.DBFile)
			for k, v := range db.archFiles[dType] {
				dbFile[k] = v
				fileIds = append(fileIds, int(k))
			}

			// active file
			activeFile, err := db.getActiveFile(dType)
			if err != nil {
				log.Fatalf("active file is nil, the db can not open.[%+v]", err)
				return
			}
			dbFile[activeFile.Id] = activeFile
			fileIds = append(fileIds, int(activeFile.Id))

			// load the db files in the order of created time.
			sort.Ints(fileIds)
			for i := 0; i < len(fileIds); i++ {
				fid := uint32(fileIds[i])
				df := dbFile[fid]
				var offset int64 = 0

				for offset <= db.config.BlockSize {
					if e, err := df.Read(offset); err == nil {
						idx := &str.StrData{
							Meta:   e.Meta,
							FileId: fid,
							Offset: offset,
						}
						offset += int64(e.Size())

						if len(e.Meta.Key) > 0 {
							if err := db.buildIndex(e, idx, true); err != nil {
								log.Fatalf("a fatal err occurred, the db can not open.[%+v]", err)
							}

							//save tx ids which are in actice files.
							if i == len(fileIds)-1 && e.TxId != 0 {
								db.txnMeta.ActiveTxIds.Store(e.TxId, struct{}{})
							}
						}
					} else {
						if err == io.EOF {
							break
						}
						log.Fatalf("a fatal err occurred, the db can not open.[%+v]", err)
					}
				}
			}
		}(uint16(dataType))
	}
	wg.Wait()
	return nil
}

// 为不同类型数据建立内存索引 index
func (db *DB) buildIndex(entry *storage.Entry, idx *str.StrData, isOpen bool) (err error) {

	// uncommitted entry is invalid.
	if entry.TxId != 0 && isOpen {
		if _, ok := db.txnMeta.CommittedTxIds[entry.TxId]; !ok {
			return
		}
	}
	switch entry.GetType() {
	case consts.String:
		db.buildStringIndex(idx, entry)
	case consts.List:
		db.buildListIndex(entry)
	case consts.Hash:
		db.buildHashIndex(entry)
	case consts.Set:
		db.buildSetIndex(entry)
	case consts.ZSet:
		db.buildZsetIndex(entry)
	}
	return
}
