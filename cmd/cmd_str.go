package cmd

import (
	"errors"
	"fmt"
	"strconv"
	"zeroDB/db"

	"github.com/tidwall/redcon"
)

// ErrSyntaxIncorrect incorrect err
var ErrSyntaxIncorrect = errors.New("syntax err")
var okResult = redcon.SimpleString("OK")

func newWrongNumOfArgsError(cmd string) error {
	return fmt.Errorf("wrong number of arguments for '%s' command", cmd)
}

func set(db *db.DB, args []string) (res interface{}, err error) {
	if len(args) != 2 {
		err = newWrongNumOfArgsError("set")
		return
	}

	key, value := args[0], args[1]
	if err = db.Set([]byte(key), []byte(value)); err == nil {
		res = okResult
	}
	return
}

func get(db *db.DB, args []string) (res interface{}, err error) {
	if len(args) != 1 {
		err = newWrongNumOfArgsError("get")
		return
	}

	key := args[0]
	var val string
	err = db.Get([]byte(key), &val)
	res = val
	return
}

func setNx(db *db.DB, args []string) (res interface{}, err error) {
	if len(args) != 2 {
		err = newWrongNumOfArgsError("setnx")
		return
	}

	key, value := args[0], args[1]
	result, err := db.SetNx([]byte(key), []byte(value))

	if err == nil {
		res = result
	}
	return
}

func getSet(db *db.DB, args []string) (res interface{}, err error) {
	if len(args) != 2 {
		err = newWrongNumOfArgsError("getset")
		return
	}

	var val string
	key, value := args[0], args[1]
	err = db.GetSet([]byte(key), []byte(value), &val)
	res = val
	return
}

func appendStr(db *db.DB, args []string) (res interface{}, err error) {
	if len(args) != 2 {
		err = newWrongNumOfArgsError("append")
		return
	}

	key, value := args[0], args[1]
	if err = db.Append([]byte(key), value); err == nil {
		res = okResult
	}
	return
}

func strExists(db *db.DB, args []string) (res interface{}, err error) {
	if len(args) != 1 {
		err = newWrongNumOfArgsError("strexists")
		return
	}
	if exists := db.StrExists([]byte(args[0])); exists {
		res = redcon.SimpleInt(1)
	} else {
		res = redcon.SimpleInt(0)
	}
	return
}

func remove(db *db.DB, args []string) (res interface{}, err error) {
	if len(args) != 1 {
		err = newWrongNumOfArgsError("remove")
		return
	}
	if err = db.Remove([]byte(args[0])); err == nil {
		res = okResult
	}
	return
}

func prefixScan(db *db.DB, args []string) (res interface{}, err error) {
	if len(args) != 3 {
		err = newWrongNumOfArgsError("prefixscan")
		return
	}
	limit, err := strconv.Atoi(args[1])
	if err != nil {
		err = ErrSyntaxIncorrect
		return
	}
	offset, err := strconv.Atoi(args[2])
	if err != nil {
		err = ErrSyntaxIncorrect
		return
	}

	res, err = db.PrefixScan(args[0], limit, offset)
	return
}

func rangeScan(db *db.DB, args []string) (res interface{}, err error) {
	if len(args) != 2 {
		err = newWrongNumOfArgsError("rangescan")
		return
	}

	res, err = db.RangeScan([]byte(args[0]), []byte(args[1]))
	return
}

func expire(db *db.DB, args []string) (res interface{}, err error) {
	if len(args) != 2 {
		err = ErrSyntaxIncorrect
		return
	}
	seconds, err := strconv.Atoi(args[1])
	if err != nil {
		err = ErrSyntaxIncorrect
		return
	}
	if err = db.Expire([]byte(args[0]), int64(seconds)); err == nil {
		res = okResult
	}
	return
}

func persist(db *db.DB, args []string) (res interface{}, err error) {
	if len(args) != 1 {
		err = newWrongNumOfArgsError("persist")
		return
	}
	db.Persist([]byte(args[0]))
	res = okResult
	return
}

func ttl(db *db.DB, args []string) (res interface{}, err error) {
	if len(args) != 1 {
		err = newWrongNumOfArgsError("ttl")
	}

	ttlVal := db.TTL([]byte(args[0]))
	res = strconv.FormatInt(int64(ttlVal), 10)
	return
}

func init() {
	addExecCommand("set", set)
	addExecCommand("get", get)
	addExecCommand("setnx", setNx)
	addExecCommand("getset", getSet)
	addExecCommand("append", appendStr)
	addExecCommand("strexists", strExists)
	addExecCommand("remove", remove)
	addExecCommand("prefixscan", prefixScan)
	addExecCommand("rangescan", rangeScan)
	addExecCommand("expire", expire)
	addExecCommand("persist", persist)
	addExecCommand("ttl", ttl)
}
