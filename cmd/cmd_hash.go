package cmd

import (
	"zeroDB/db"

	"github.com/tidwall/redcon"
)

func hSet(db *db.DB, args []string) (res interface{}, err error) {
	if len(args) != 3 {
		err = newWrongNumOfArgsError("hset")
		return
	}
	var count int
	if count, err = db.HSet([]byte(args[0]), []byte(args[1]), []byte(args[2])); err == nil {
		res = redcon.SimpleInt(count)
	}
	return
}

func hSetNx(db *db.DB, args []string) (res interface{}, err error) {
	if len(args) != 3 {
		err = newWrongNumOfArgsError("hsetnx")
		return
	}
	var ok int
	if ok, err = db.HSetNx([]byte(args[0]), []byte(args[1]), []byte(args[2])); err == nil {
		if ok == 1 {
			res = redcon.SimpleInt(1)
		} else {
			res = redcon.SimpleInt(0)
		}
	}
	return
}

func hGet(db *db.DB, args []string) (res interface{}, err error) {
	if len(args) != 2 {
		err = ErrSyntaxIncorrect
		return
	}
	val := db.HGet([]byte(args[0]), []byte(args[1]))
	if len(val) == 0 {
		res = nil
	} else {
		res = string(val)
	}
	return
}

func hGetAll(db *db.DB, args []string) (res interface{}, err error) {
	if len(args) != 1 {
		err = newWrongNumOfArgsError("hgetall")
		return
	}
	res = db.HGetAll([]byte(args[0]))
	return
}

func hDel(db *db.DB, args []string) (res interface{}, err error) {
	if len(args) <= 1 {
		err = newWrongNumOfArgsError("hdel")
		return
	}

	var fields [][]byte
	for _, f := range args[1:] {
		fields = append(fields, []byte(f))
	}
	var count int
	if count, err = db.HDel([]byte(args[0]), fields...); err == nil {
		res = redcon.SimpleInt(count)
	}
	return
}

func hExists(db *db.DB, args []string) (res interface{}, err error) {
	if len(args) != 2 {
		err = newWrongNumOfArgsError("hexists")
		return
	}
	exists := db.HExists([]byte(args[0]), []byte(args[1]))
	if exists {
		res = redcon.SimpleInt(1)
	} else {
		res = redcon.SimpleInt(0)
	}
	return
}

func hLen(db *db.DB, args []string) (res interface{}, err error) {
	if len(args) != 1 {
		err = newWrongNumOfArgsError("hlen")
		return
	}
	count := db.HLen([]byte(args[0]))
	res = redcon.SimpleInt(count)
	return
}

func hKeys(db *db.DB, args []string) (res interface{}, err error) {
	if len(args) != 1 {
		err = ErrSyntaxIncorrect
		return
	}
	res = db.HKeys([]byte(args[0]))
	return
}

func hVals(db *db.DB, args []string) (res interface{}, err error) {
	if len(args) != 1 {
		err = newWrongNumOfArgsError("hvals")
		return
	}
	res = db.HVals([]byte(args[0]))
	return
}

func init() {
	addExecCommand("hset", hSet)
	addExecCommand("hsetnx", hSetNx)
	addExecCommand("hget", hGet)
	addExecCommand("hgetall", hGetAll)
	addExecCommand("hdel", hDel)
	addExecCommand("hexists", hExists)
	addExecCommand("hlen", hLen)
	addExecCommand("hkeys", hKeys)
	addExecCommand("hvals", hVals)
}
