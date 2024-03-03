package db

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"zeroDB/datastructure/hash"
	"zeroDB/datastructure/list"
	"zeroDB/datastructure/set"
	str "zeroDB/datastructure/string"
	"zeroDB/datastructure/zset"
	"zeroDB/global/config"
	"zeroDB/global/consts"
	"zeroDB/global/dberror"
	"zeroDB/global/utils"
	"zeroDB/storage"
)

type (
	// HashIdx
	HashIdx struct {
		mu      *sync.RWMutex
		indexes *hash.Hash
	}
	ListIdx struct {
		mu      *sync.RWMutex
		indexes *list.List
	}
	SetIdx struct {
		mu      *sync.RWMutex
		indexes *set.Set
	}
	ZsetIdx struct {
		mu      *sync.RWMutex
		indexes *zset.SortedSet
	}
	StrIdx struct {
		mu      *sync.RWMutex
		idxList *str.SkipList
	}

	DB struct {
		activeFile   *sync.Map
		archFiles    ArchivedFiles // The archived files.
		strIndex     *StrIdx       // String indexes
		listIndex    *ListIdx      // List indexes
		hashIndex    *HashIdx      // Hash indexes
		setIndex     *SetIdx       // Set indexes
		zsetIndex    *ZsetIdx      // Sorted set indexes
		config       config.Config
		mu           sync.RWMutex
		lockMgr      *LockMgr // lockMgr controls isolation of read and write.
		txnMeta      *TxnMeta // Txn meta info used in transaction.
		expires      Expires
		isReclaiming bool // Indicates whether the db is reclaiming, see Reclaim
		closed       uint32
	}
	//存档的文件，只读不写
	ArchivedFiles map[consts.DataType]map[uint32]*storage.DBFile

	//储存key的过期消息
	Expires map[consts.DataType]map[string]int64
)

// create a new hash index
func newHashIdx() *HashIdx {
	return &HashIdx{indexes: hash.New(), mu: new(sync.RWMutex)}
}

// create a new zset index.
func newZsetIdx() *ZsetIdx {
	return &ZsetIdx{indexes: zset.New(), mu: new(sync.RWMutex)}
}

// create new set index.
func newSetIdx() *SetIdx {
	return &SetIdx{indexes: set.New(), mu: new(sync.RWMutex)}
}

// create new list index.
func newListIdx() *ListIdx {
	return &ListIdx{
		indexes: list.New(), mu: new(sync.RWMutex),
	}
}

// create new string index.
func newStrIdx() *StrIdx {
	return &StrIdx{
		idxList: str.NewSkipList(), mu: new(sync.RWMutex),
	}
}

// 开启一个db实例. 用后必须关闭
func Open(config config.Config) (*DB, error) {
	//创建文件储存的路径。如果不存在
	if !utils.Exist(config.DirPath) {
		if err := os.MkdirAll(config.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	//从磁盘中加载文件
	archFiles, activeFileIds, err := storage.Build(config.DirPath, config.BlockSize)
	if err != nil {
		return nil, err
	}

	// set active files for writing.
	activeFiles := new(sync.Map)
	for dataType, fileId := range activeFileIds {
		file, err := storage.NewDBFile(config.DirPath, fileId, dataType)
		if err != nil {
			return nil, err
		}
		activeFiles.Store(dataType, file)
	}

	// load txn meta info for transaction.
	txnMeta, err := LoadTxnMeta(config.DirPath + consts.DbTxMetaSaveFile)
	if err != nil {
		return nil, err
	}
	//创建db实例
	db := &DB{
		activeFile: activeFiles,
		archFiles:  archFiles,
		config:     config,
		strIndex:   newStrIdx(),
		listIndex:  newListIdx(),
		hashIndex:  newHashIdx(),
		setIndex:   newSetIdx(),
		zsetIndex:  newZsetIdx(),
		expires:    make(Expires),
		txnMeta:    txnMeta,
	}
	//初始化内存中的过期map
	for i := 0; i < consts.DataStructureNum; i++ {
		db.expires[uint16(i)] = make(map[string]int64)
	}
	db.lockMgr = newLockMgr(db)

	//以dbfile中的文件创建内存中的数据索引
	if err := db.loadIdxFromFiles(); err != nil {
		return nil, err
	}

	return db, nil
}

// Close db and save relative configs.
func (db *DB) Close() (err error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err = db.saveConfig(); err != nil {
		return err
	}

	// close and sync the active file.
	db.activeFile.Range(func(key, value interface{}) bool {
		if dbFile, ok := value.(*storage.DBFile); ok {
			if err = dbFile.Close(true); err != nil {
				return false
			}
		}
		return true
	})
	if err != nil {
		return
	}

	// close the archived files.
	for _, archFile := range db.archFiles {
		for _, file := range archFile {
			if err = file.Sync(); err != nil {
				return err
			}
		}
	}

	atomic.StoreUint32(&db.closed, 1)
	return
}

// save config before closing db.
func (db *DB) saveConfig() (err error) {
	path := db.config.DirPath + consts.ConfigSaveFile
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)

	b, err := json.Marshal(db.config)
	_, err = file.Write(b)
	err = file.Close()

	return
}

func (db *DB) getActiveFile(dType consts.DataType) (file *storage.DBFile, err error) {
	value, ok := db.activeFile.Load(dType)
	if !ok || value == nil {
		return nil, dberror.ErrActiveFileIsNil
	}

	var typeOk bool
	if file, typeOk = value.(*storage.DBFile); !typeOk {
		return nil, dberror.ErrActiveFileIsNil
	}
	return
}

// Reclaim 对dbfile进行重写，回收消耗的多余的磁盘空间
// Reclaim 会遍历所有存档的dbfile，扎到 valid entry ， 将其写进dbfile
// reclaim 需要的时间取决于entry的数量， 最好选在低流量时使用
func (db *DB) Reclaim() (err error) {
	// if single reclaiming is in progress, the reclaim operation can`t be executed.
	// if db.isSingleReclaiming {
	// 	return ErrDBisReclaiming
	// }
	var reclaimable bool
	for _, archFiles := range db.archFiles {
		if len(archFiles) >= db.config.ReclaimThreshold {
			reclaimable = true
			break
		}
	}
	if !reclaimable {
		return dberror.ErrReclaimUnreached
	}

	// 创建临时文件储存新的dbfile
	reclaimPath := db.config.DirPath + consts.ReclaimPath
	if err := os.MkdirAll(reclaimPath, os.ModePerm); err != nil {
		return err
	}
	defer os.RemoveAll(reclaimPath)

	db.mu.Lock()
	defer func() {
		db.isReclaiming = false
		db.mu.Unlock()
	}()
	db.isReclaiming = true

	// 接下来就是reclaim的操作了

	// processing the different types of files in different goroutines.
	newArchivedFiles := sync.Map{}
	reclaimedTypes := sync.Map{}

	wg := sync.WaitGroup{}
	wg.Add(consts.DataStructureNum)
	for i := 0; i < consts.DataStructureNum; i++ {
		go func(dType uint16) {
			defer func() {
				wg.Done()
			}()
			// 如果某类型的 archvied filed < ReclaimThreshold , 直接将其存进 newarchviedfiles，退出此函数
			if len(db.archFiles[dType]) < db.config.ReclaimThreshold {
				newArchivedFiles.Store(dType, db.archFiles[dType])
				return
			}

			var (
				df        *storage.DBFile
				fileId    uint32
				archFiles = make(map[uint32]*storage.DBFile)
				fileIds   []int
			)

			// 把此类型的所有 archived file 的 id 存到 fileIds里，并从小到大排序
			for _, file := range db.archFiles[dType] {
				fileIds = append(fileIds, int(file.Id))
			}
			sort.Ints(fileIds)

			for _, fid := range fileIds {
				file := db.archFiles[dType][uint32(fid)]
				var offset int64 = 0
				var reclaimEntries []*storage.Entry

				// 读取dbfile中的所有entry，找到valid entry.
				for {
					if e, err := file.Read(offset); err == nil {
						if db.validEntry(e, offset, file.Id) {
							reclaimEntries = append(reclaimEntries, e)
						}
						offset += int64(e.Size())
					} else {
						if err == io.EOF {
							break
						}
						log.Fatalf("err occurred when read the entry: %+v", err)
						return
					}
				}

				// 将有效的 entry 写入到新的 dbfile
				for _, entry := range reclaimEntries {
					if df == nil || int64(entry.Size())+df.Offset > db.config.BlockSize {
						df, err = storage.NewDBFile(reclaimPath, fileId, dType)
						if err != nil {
							log.Fatalf("err occurred when create new db file: %+v", err)
							return
						}
						//add dbfile into new archfiles
						archFiles[fileId] = df
						fileId += 1
					}

					if err = df.Write(entry); err != nil {
						log.Fatalf("err occurred when write the entry: %+v", err)
						return
					}

					// Since the str types value will be read from db file, so should update the index info.
					if dType == consts.String {
						item := db.strIndex.idxList.Get(entry.Meta.Key)
						idx := item.Value().(*str.StrData)
						idx.Offset = df.Offset - int64(entry.Size())
						idx.FileId = fileId
						db.strIndex.idxList.Put(idx.Meta.Key, idx)
					}
				}
			}
			reclaimedTypes.Store(dType, struct{}{})
			newArchivedFiles.Store(dType, archFiles)
		}(uint16(i))
	}
	wg.Wait()
	// 已经 reclaime dbfile 了
	dbArchivedFiles := make(ArchivedFiles)
	for i := 0; i < consts.DataStructureNum; i++ {
		dType := uint16(i)
		value, ok := newArchivedFiles.Load(dType)
		if !ok {
			log.Printf("one type of data(%d) is missed after reclaiming.", dType)
			return
		}
		dbArchivedFiles[dType] = value.(map[uint32]*storage.DBFile)
	}

	// 删除之前的 dbfile
	for dataType, files := range db.archFiles {
		if _, exist := reclaimedTypes.Load(dataType); exist {
			for _, f := range files {
				// close file before remove it.
				if err = f.File.Close(); err != nil {
					log.Println("close old db file err: ", err)
					return
				}
				if err = os.Remove(f.File.Name()); err != nil {
					log.Println("remove old db file err: ", err)
					return
				}
			}
		}
	}

	// 复制临时 reclaim directory 作为 new db files.
	for dataType, files := range dbArchivedFiles {
		if _, exist := reclaimedTypes.Load(dataType); exist {
			for _, f := range files {
				name := storage.PathSeparator + fmt.Sprintf(storage.DBFileFormatNames[dataType], f.Id)
				os.Rename(reclaimPath+name, db.config.DirPath+name)
			}
		}
	}

	db.archFiles = dbArchivedFiles

	// 移除 txn meta file ，创建一个新的
	if err = db.txnMeta.txnFile.File.Close(); err != nil {
		log.Println("close txn file err: ", err)
		return
	}
	if err = os.Remove(db.config.DirPath + consts.DbTxMetaSaveFile); err == nil {
		var txnMeta *TxnMeta
		activeTxIds := db.txnMeta.ActiveTxIds
		txnMeta, err = LoadTxnMeta(db.config.DirPath + consts.DbTxMetaSaveFile)
		if err != nil {
			return err
		}

		db.txnMeta = txnMeta
		// write active tx ids.
		activeTxIds.Range(func(key, value interface{}) bool {
			if txId, ok := key.(uint64); ok {
				if err = db.MarkCommit(txId); err != nil {
					return false
				}
			}
			return true
		})
	}
	return
}

// validEntry 检查 entry 是否有效，过期了的会被筛除
// expired entry will be filtered.
func (db *DB) validEntry(e *storage.Entry, offset int64, fileId uint32) bool {
	if e == nil {
		return false
	}

	// uncommitted entry is invalid.
	if e.TxId != 0 {
		if _, ok := db.txnMeta.CommittedTxIds[e.TxId]; !ok {
			return false
		}
		e.TxId = 0
	}

	mark := e.GetMark()
	switch e.GetType() {
	case consts.String:
		deadline, exist := db.expires[consts.String][string(e.Meta.Key)]
		now := time.Now().Unix()

		if mark == consts.StringExpire {
			if exist && deadline > now {
				return true
			}
		}
		if mark == consts.StringSet || mark == consts.StringPersist {
			// check expired.
			if exist && deadline <= now {
				return false
			}

			// check the data position.
			node := db.strIndex.idxList.Get(e.Meta.Key)
			if node == nil {
				return false
			}
			indexer := node.Value().(*str.StrData)
			if bytes.Compare(indexer.Meta.Key, e.Meta.Key) == 0 {
				if indexer != nil && indexer.FileId == fileId && indexer.Offset == offset {
					return true
				}
			}
		}
	case consts.List:
		if mark == consts.ListLExpire {
			deadline, exist := db.expires[consts.List][string(e.Meta.Key)]
			if exist && deadline > time.Now().Unix() {
				return true
			}
		}
		if mark == consts.ListLPush || mark == consts.ListRPush || mark == consts.ListLInsert || mark == consts.ListLSet {
			if db.LValExists(e.Meta.Key, e.Meta.Value) {
				return true
			}
		}
	case consts.Hash:
		if mark == consts.HashHExpire {
			deadline, exist := db.expires[consts.Hash][string(e.Meta.Key)]
			if exist && deadline > time.Now().Unix() {
				return true
			}
		}
		if mark == consts.HashHSet {
			if val := db.HGet(e.Meta.Key, e.Meta.Extra); string(val) == string(e.Meta.Value) {
				return true
			}
		}
	case consts.Set:
		if mark == consts.SetSExpire {
			deadline, exist := db.expires[consts.Set][string(e.Meta.Key)]
			if exist && deadline > time.Now().Unix() {
				return true
			}
		}
		if mark == consts.SetSMove {
			if db.SIsMember(e.Meta.Extra, e.Meta.Value) {
				return true
			}
		}
		if mark == consts.SetSAdd {
			if db.SIsMember(e.Meta.Key, e.Meta.Value) {
				return true
			}
		}
	case consts.ZSet:
		if mark == consts.ZSetZExpire {
			deadline, exist := db.expires[consts.ZSet][string(e.Meta.Key)]
			if exist && deadline > time.Now().Unix() {
				return true
			}
		}
		if mark == consts.ZSetZAdd {
			if val, err := utils.StrToFloat64(string(e.Meta.Extra)); err == nil {
				ok, score := db.ZScore(e.Meta.Key, e.Meta.Value)
				if ok && score == val {
					return true
				}
			}
		}
	}
	return false
}
