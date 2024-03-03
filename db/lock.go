package db

import (
	"sync"
	"zeroDB/global/consts"
)

// LockMgr 管理不同数据类型的读写
// 被用于完成事务操作
type LockMgr struct {
	locks map[consts.DataType]*sync.RWMutex
}

func newLockMgr(db *DB) *LockMgr {
	locks := make(map[consts.DataType]*sync.RWMutex)
	// 储存不同数据的锁
	locks[consts.String] = db.strIndex.mu
	locks[consts.List] = db.listIndex.mu
	locks[consts.Hash] = db.hashIndex.mu
	locks[consts.Set] = db.setIndex.mu
	locks[consts.ZSet] = db.zsetIndex.mu

	return &LockMgr{locks: locks}
}

// 数据写锁
func (lm *LockMgr) Lock(dTypes ...consts.DataType) func() {
	for _, t := range dTypes {
		lm.locks[t].Lock()
	}

	unLockFunc := func() {
		for _, t := range dTypes {
			lm.locks[t].Unlock()
		}
	}
	return unLockFunc
}

// 数据读锁
func (lm *LockMgr) RLock(dTypes ...consts.DataType) func() {
	for _, t := range dTypes {
		lm.locks[t].RLock()
	}

	unLockFunc := func() {
		for _, t := range dTypes {
			lm.locks[t].RUnlock()
		}
	}
	return unLockFunc
}
