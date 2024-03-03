package consts

// 数据类型
type DataType = uint16

// 五种数据类型，String, List, Hash, Set, Sorted Set .
const (
	String DataType = iota
	List
	Hash
	Set
	ZSet
)

// string 的操作类型，会写入entry里面。诸如此类
const (
	StringSet uint16 = iota
	StringRem
	StringExpire
	StringPersist
)

// list操作
const (
	ListLPush uint16 = iota
	ListRPush
	ListLPop
	ListRPop
	ListLRem
	ListLInsert
	ListLSet
	ListLTrim
	ListLClear
	ListLExpire
)

// hash操作
const (
	HashHSet uint16 = iota
	HashHDel
	HashHClear
	HashHExpire
)

// set操作
const (
	SetSAdd uint16 = iota
	SetSRem
	SetSMove
	SetSClear
	SetSExpire
)

// zset操作
const (
	ZSetZAdd uint16 = iota
	ZSetZRem
	ZSetZClear
	ZSetZExpire
)
