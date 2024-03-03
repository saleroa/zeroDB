package dberror

import "errors"

var (
	ErrEmptyKey = errors.New("zerokv: the key is empty")

	ErrKeyNotExist = errors.New("zerokv: key not exist")

	ErrKeyTooLarge = errors.New("zerokv: key exceeded the max length")

	ErrValueTooLarge = errors.New("zerokv: value exceeded the max length")

	ErrNilStrData = errors.New("zerokv: StrData is nil")

	ErrCfgNotExist = errors.New("zerokv: the config file not exist")

	ErrReclaimUnreached = errors.New("zerokv: unused space not reach the threshold")

	ErrExtraContainsSeparator = errors.New("zerokv: extra contains separator \\0")

	ErrInvalidTTL = errors.New("zerokv: invalid ttl")

	ErrKeyExpired = errors.New("zerokv: key is expired")

	ErrDBisReclaiming = errors.New("zerokv: can`t do reclaim and single reclaim at the same time")

	ErrDBIsClosed = errors.New("zerokv: db is closed, reopen it")

	ErrTxIsFinished = errors.New("zerokv: transaction is finished, create a new one")

	ErrActiveFileIsNil = errors.New("zerokv: active file is nil")
)
