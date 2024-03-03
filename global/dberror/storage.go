package dberror

import "errors"

var (
	ErrInvalidEntry = errors.New("storage/entry: invalid entry")

	// entry 的 crc 校验失败
	ErrInvalidCrc = errors.New("storage/file: invalid crc")

	ErrEmptyEntry = errors.New("storage/file: entry or the Key of entry is empty")
)
