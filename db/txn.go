package db

import (
	"encoding/binary"
	"io"
	"os"
	"sync"
	str "zeroDB/datastructure/string"
	"zeroDB/global/consts"
	"zeroDB/global/dberror"
	"zeroDB/storage"
)

const (
	txIdLen = 8
)

type (
	// Txn is a Transaction.
	// read-write transaction call Txn, read-only transaction call TxnView.
	Txn struct {
		id uint64
		db *DB
		wg *sync.WaitGroup

		// strEntries is for String. Save entries wait writeEntry to write.
		strEntries map[string]*storage.Entry

		// writeEntries is for List, Hash, Set, ZSet.
		writeEntries []*storage.Entry

		// skipIds save entries`s index that don`t need be processed.
		//skipIds work with Keysmap.
		skipIds map[int]struct{}

		// stored keys in it to avoid duplicate entries.
		// just del and set operate will be in.
		keysMap map[string]int

		// save and get data structures in the transaction.
		dsState    uint16
		isFinished bool
	}

	// TxnMeta represents some transaction info while tx is running.
	TxnMeta struct {
		// MaxTxId the max tx id now.
		MaxTxId uint64

		// ActiveTxIds committed tx ids in active files.
		// they will be write into txnfile.
		ActiveTxIds *sync.Map

		// save committed entrys, used to check uncommited entrys.
		CommittedTxIds map[uint64]struct{}

		// a file for saving committed tx ids.
		// ids in it will be writed into ComittedTxids.
		txnFile *TxnFile
	}

	// TxnFile a single file in disk to save committed transaction ids.
	TxnFile struct {
		File   *os.File // file.
		Offset int64    // write offset.
	}
)

// Txn execute a transaction which read and write.
func (db *DB) Txn(fn func(tx *Txn) error) (err error) {
	if db.isClosed() {
		return dberror.ErrDBIsClosed
	}
	txn := db.NewTransaction()

	if err = fn(txn); err != nil {
		txn.Rollback()
		return
	}

	if err = txn.Commit(); err != nil {
		txn.Rollback()
		return
	}
	return
}

// TxnView execute a transaction which read only.
func (db *DB) TxnView(fn func(tx *Txn) error) (err error) {
	if db.isClosed() {
		return dberror.ErrDBIsClosed
	}
	txn := db.NewTransaction()
	//lock indexes of these data types.
	dTypes := txn.getDTypes()
	unlockFunc := txn.db.lockMgr.RLock(dTypes...)
	defer unlockFunc()

	if err = fn(txn); err != nil {
		txn.Rollback()
		return
	}
	txn.finished()
	return
}

// NewTransaction create a new transaction, don`t support concurrent execution of transactions now.
// So you can only open a read-write transaction at the same time.
// For read-only transactions, you can execute multiple, and any write operations will be omitted.
func (db *DB) NewTransaction() *Txn {
	db.mu.Lock()
	defer func() {
		db.txnMeta.MaxTxId += 1
		db.mu.Unlock()
	}()

	return &Txn{
		id:         db.txnMeta.MaxTxId + 1,
		db:         db,
		wg:         new(sync.WaitGroup),
		strEntries: make(map[string]*storage.Entry),
		keysMap:    make(map[string]int),
		skipIds:    make(map[int]struct{}),
	}
}

// Commit commit the transaction.
func (tx *Txn) Commit() (err error) {
	if tx.db.isClosed() {
		return dberror.ErrDBIsClosed
	}
	defer tx.finished()

	if len(tx.strEntries) == 0 && len(tx.writeEntries) == 0 {
		return
	}

	dTypes := tx.getDTypes()
	// write lock lock indexes
	unlockFunc := tx.db.lockMgr.Lock(dTypes...)
	defer unlockFunc()

	// write entry into db files.
	var indexes []*str.StrData
	if len(tx.strEntries) > 0 && len(tx.writeEntries) > 0 {
		tx.wg.Add(2)
		go func() {
			defer tx.wg.Done()
			if indexes, err = tx.writeStrEntries(); err != nil {
				return
			}
		}()

		go func() {
			defer tx.wg.Done()
			if err = tx.writeOtherEntries(); err != nil {
				return
			}
		}()
		tx.wg.Wait()
		if err != nil {
			return err
		}
	} else {
		if indexes, err = tx.writeStrEntries(); err != nil {
			return
		}
		if err = tx.writeOtherEntries(); err != nil {
			return
		}
	}

	// sync the db file for transaction durability.
	if tx.db.config.Sync {
		if err := tx.db.Sync(); err != nil {
			return err
		}
	}

	// mark the transaction is committed.
	if err = tx.db.MarkCommit(tx.id); err != nil {
		return
	}

	// build indexes.
	for _, idx := range indexes {
		if err = tx.db.buildIndex(tx.strEntries[string(idx.Meta.Key)], idx, false); err != nil {
			return
		}
	}
	for _, entry := range tx.writeEntries {
		if err = tx.db.buildIndex(entry, nil, false); err != nil {
			return
		}
	}
	return
}

// Rollback finished current transaction.
func (tx *Txn) Rollback() {
	tx.finished()
}

// MarkCommit write the tx id into txn file.
func (db *DB) MarkCommit(txId uint64) (err error) {
	buf := make([]byte, txIdLen)
	binary.BigEndian.PutUint64(buf[:], txId)

	offset := db.txnMeta.txnFile.Offset
	_, err = db.txnMeta.txnFile.File.WriteAt(buf, offset)
	if err != nil {
		return
	}
	db.txnMeta.txnFile.Offset += int64(len(buf))

	if db.config.Sync {
		if err = db.txnMeta.txnFile.File.Sync(); err != nil {
			return
		}
	}
	return
}

// LoadTxnMeta load txn meta info, committed tx id.
func LoadTxnMeta(path string) (txnMeta *TxnMeta, err error) {
	txnMeta = &TxnMeta{
		CommittedTxIds: make(map[uint64]struct{}),
		ActiveTxIds:    new(sync.Map),
	}

	var (
		file    *os.File
		maxTxId uint64
		stat    os.FileInfo
	)
	if file, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644); err != nil {
		return
	}
	if stat, err = file.Stat(); err != nil {
		return
	}
	txnMeta.txnFile = &TxnFile{
		File:   file,
		Offset: stat.Size(),
	}

	if txnMeta.txnFile.Offset > 0 {
		var offset int64
		for {
			buf := make([]byte, txIdLen)
			_, err = file.ReadAt(buf, offset)
			if err != nil {
				if err == io.EOF {
					err = nil
					break
				}
				return
			}
			txId := binary.BigEndian.Uint64(buf)
			if txId > maxTxId {
				maxTxId = txId
			}
			txnMeta.CommittedTxIds[txId] = struct{}{}
			offset += txIdLen
		}
	}
	txnMeta.MaxTxId = maxTxId
	return
}

func (tx *Txn) finished() {
	tx.strEntries = nil
	tx.writeEntries = nil

	tx.skipIds = nil
	tx.keysMap = nil

	tx.isFinished = true
	return
}

func (tx *Txn) writeStrEntries() (indexes []*str.StrData, err error) {
	// if write enrty is empty,return nil
	if len(tx.strEntries) == 0 {
		return
	}

	for _, entry := range tx.strEntries {
		if err = tx.db.store(entry); err != nil {
			return
		}
		activeFile, err := tx.db.getActiveFile(consts.String)
		if err != nil {
			return nil, err
		}
		// generate index.
		indexes = append(indexes, &str.StrData{
			Meta: &storage.Meta{
				Key: entry.Meta.Key,
			},
			FileId: activeFile.Id,
			Offset: activeFile.Offset - int64(entry.Size()),
		})
	}
	return
}

func (tx *Txn) writeOtherEntries() (err error) {
	// if write enrty is empty,return nil
	if len(tx.writeEntries) == 0 {
		return
	}

	for i, entry := range tx.writeEntries {
		if _, ok := tx.skipIds[i]; ok {
			continue
		}
		if err = tx.db.store(entry); err != nil {
			return
		}
	}
	return
}

func (tx *Txn) putEntry(e *storage.Entry) (err error) {
	if e == nil {
		return
	}
	if tx.db.isClosed() {
		return dberror.ErrDBIsClosed
	}
	if tx.isFinished {
		return dberror.ErrTxIsFinished
	}

	switch e.GetType() {
	case consts.String:
		tx.strEntries[string(e.Meta.Key)] = e
	default:
		tx.writeEntries = append(tx.writeEntries, e)
	}
	tx.setDsState(e.GetType())
	return
}

func (tx *Txn) setDsState(dType consts.DataType) {
	tx.dsState = tx.dsState | (1 << dType)
}

func (tx *Txn) getDTypes() (dTypes []uint16) {
	// string
	if (tx.dsState&(1<<consts.String))>>consts.String == 1 {
		dTypes = append(dTypes, consts.String)
	}
	// list
	if (tx.dsState&(1<<consts.List))>>consts.List == 1 {
		dTypes = append(dTypes, consts.List)
	}
	// hash
	if (tx.dsState&(1<<consts.Hash))>>consts.Hash == 1 {
		dTypes = append(dTypes, consts.Hash)
	}
	// set
	if (tx.dsState&(1<<consts.Set))>>consts.Set == 1 {
		dTypes = append(dTypes, consts.Set)
	}
	// zset
	if (tx.dsState&(1<<consts.ZSet))>>consts.ZSet == 1 {
		dTypes = append(dTypes, consts.ZSet)
	}
	return
}
