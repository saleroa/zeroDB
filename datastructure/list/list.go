package list

import (
	"container/list"
	"reflect"
)

// list 文件中，对于 list 为空的判断要测试
// 对于返回 bool 也可以加以修改，对所有文件

type InsertOption uint8

const (
	// Before insert before pivot.
	Before InsertOption = iota
	// After insert after pivot.
	After
)

type (
	List struct {
		// record saves the List of a specified key.
		Record Record

		// values saves the values of a List, help checking if a value exists in List.
		Values map[string]map[string]int
	}

	// Record list record to save.
	Record map[string]*list.List
)

// New a new list
func New() *List {
	return &List{
		make(Record),
		make(map[string]map[string]int),
	}
}

// push to front of this key
func (lis *List) LPush(key string, val ...[]byte) int {
	return lis.push(true, key, val...)
}

// push to back of this key
func (lis *List) RPush(key string, val ...[]byte) int {
	return lis.push(false, key, val...)
}

// pop val from this key, front
func (lis *List) LPop(key string) []byte {
	return lis.pop(true, key)
}

// pop val from this key, back
func (lis *List) RPop(key string) []byte {
	return lis.pop(false, key)
}

// return val by index
func (lis *List) LIndex(key string, index int) []byte {
	var val []byte
	e := lis.index(key, index)
	if e != nil {
		val = e.Value.([]byte)
	}
	return val
}

// remove element equal val, stratage differ by count
func (lis *List) LRem(key string, val []byte, count int) int {
	item := lis.Record[key]
	if item == nil {
		return 0
	}
	var ele []*list.Element
	if count == 0 {
		for p := item.Front(); p != nil; p = p.Next() {
			if reflect.DeepEqual(p.Value, val) {
				ele = append(ele, p)
			}
		}
	} else if count > 0 {
		for p := item.Front(); p != nil && len(ele) < count; p = p.Next() {
			if reflect.DeepEqual(p.Value, val) {
				ele = append(ele, p)
			}
		}

	} else {
		// count < 0
		for p := item.Back(); p != nil && len(ele) < -count; p = p.Prev() {
			if reflect.DeepEqual(p.Value, val) {
				ele = append(ele, p)
			}
		}
	}
	for _, v := range ele {
		item.Remove(v)
	}
	// change value
	length := len(ele)
	ele = nil
	if lis.Values[key] != nil {
		count := lis.Values[key][string(val)] - length
		if count <= 0 {
			delete(lis.Values[key], string(val))
		} else {
			lis.Values[key][string(val)] = count
		}
	}
	return length
}

// insert after|before pivot
func (lis *List) LInsert(key string, option InsertOption, pivot, val []byte) int {
	e := lis.find(key, pivot)
	if e == nil {
		return -1
	}
	item := lis.Record[key]
	if option == Before {
		item.InsertBefore(val, e)
	}
	if option == After {
		item.InsertAfter(val, e)
	}
	if lis.Values[key] == nil {
		lis.Values[key] = make(map[string]int)
	}
	lis.Values[key][string(val)] += 1
	return item.Len()
}

// set value as val,by index
func (lis *List) LSet(key string, index int, val []byte) bool {
	e := lis.index(key, index)
	if e == nil {
		return false
	}
	if lis.Values[key] == nil {
		lis.Values[key] = make(map[string]int)
	}

	// update count
	if e.Value != nil {
		v := string(e.Value.([]byte))
		count := lis.Values[key][v] - 1
		if count <= 0 {
			delete(lis.Values[key], v)
		} else {
			lis.Values[key][v] = count
		}

	}
	e.Value = val
	lis.Values[key][string(val)] += 1
	return true
}

// return a list of value by index from start to end
func (lis *List) LRange(key string, start, end int) [][]byte {
	var val [][]byte
	item := lis.Record[key]
	if item == nil {
		return val
	}
	length := item.Len()
	// return valid start,end
	start, end = lis.handleIndex(length, start, end)
	if start > length || start >= length {
		return val
	}
	mid := length >> 1
	// traverse from left to rigth
	if end <= mid || end-mid < mid-start {
		flag := 0
		for p := item.Front(); p != nil && flag <= end; p, flag = p.Next(), flag+1 {
			if flag >= start {
				val = append(val, p.Value.([]byte))
			}
		}
	} else {
		// traverse from rigth to left
		flag := length - 1
		for p := item.Back(); p != nil && flag >= start; p, flag = p.Prev(), flag-1 {
			if flag <= end {
				val = append(val, p.Value.([]byte))
			}
		}
		// reverse , if from right to left
		if len(val) > 0 {
			for i, j := 0, len(val)-1; i < j; i, j = i+1, j-1 {
				val[i], val[j] = val[j], val[i]
			}
		}

	}
	return val
}

func (lis *List) LTrim(key string, start, end int) bool {
	item := lis.Record[key]
	if item == nil {
		return false
	}
	length := item.Len()
	start, end = lis.handleIndex(length, start, end)
	if start <= 0 && end >= length-1 {
		return false
	}
	if start > end || start >= length {
		lis.Record[key] = nil
		lis.Values[key] = nil
		return true
	}
	startEle, endEle := lis.index(key, start), lis.index(key, end)
	//depend on the number of needed element, two strategy
	if end-start+1 < (length >> 1) {
		// more element need to be removed
		newList := list.New()
		newValueMap := make(map[string]int)
		for p := startEle; p != endEle.Next(); p = p.Next() {
			newList.PushBack(p)
			if p.Value != nil {
				newValueMap[string(p.Value.([]byte))] += 1
			}
		}
		item = nil
		lis.Record[key] = newList
		lis.Values[key] = newValueMap
	} else {
		// more element need to be keept
		var ele []*list.Element
		for p := item.Front(); p != startEle; p = p.Next() {
			ele = append(ele, p)
		}
		for p := item.Back(); p != endEle; p = p.Prev() {
			ele = append(ele, p)
		}
		for _, e := range ele {
			item.Remove(e)
			if lis.Values[key] != nil && e.Value != nil {
				v := string(e.Value.([]byte))
				count := lis.Values[key][v] - 1
				if count <= 0 {
					delete(lis.Values[key], v)
				} else {
					lis.Values[key][v] = count
				}

			}
		}
		ele = nil
	}
	return true
}

// return the number of member of the key
func (lis *List) LLen(key string) int {
	length := 0
	if lis.Record[key] != nil {
		length = lis.Record[key].Len()
	}

	return length
}

// clear a specified key for List.
func (lis *List) LClear(key string) {
	delete(lis.Record, key)
	delete(lis.Values, key)
}

// check if the key exists
func (lis *List) LKeyExists(key string) (ok bool) {
	_, ok = lis.Record[key]
	return
}

// check if the val exist at key.
func (lis *List) LValExists(key string, val []byte) (ok bool) {
	if lis.Values[key] != nil {
		cnt := lis.Values[key][string(val)]
		ok = cnt > 0
	}
	return
}

//################################

// push val to the key, front or back
func (lis *List) push(front bool, key string, val ...[]byte) int {
	if lis.Record[key] == nil {
		lis.Record[key] = list.New()
	}
	if lis.Values[key] == nil {
		lis.Values[key] = make(map[string]int)
	}
	for _, v := range val {
		if front {
			lis.Record[key].PushFront(v)
		} else {
			lis.Record[key].PushBack(v)
		}
		lis.Values[key][string(v)] += 1
	}
	return lis.Record[key].Len()
}

// pop val from this key, front or back
func (lis *List) pop(front bool, key string) []byte {
	item := lis.Record[key]
	var val []byte
	if item != nil && item.Len() > 0 {
		var e *list.Element
		if front {
			e = item.Front()
		} else {
			e = item.Back()
		}

		val = e.Value.([]byte)
		item.Remove(e)
		//update value count
		if lis.Values[key] != nil {
			count := lis.Values[key][string(val)] - 1
			if count <= 0 {
				delete(lis.Values[key], string(val))
			} else {
				lis.Values[key][string(val)] = count
			}
		}
	}
	return val
}

// check whether index valid
func (lis *List) validIndex(key string, index int) (bool, int) {
	item := lis.Record[key]
	if item == nil {
		return false, index
	}
	length := item.Len()
	if index < 0 {
		index += length
	}
	return index < length && index > 0, index
}

// get element by the index
func (lis *List) index(key string, index int) *list.Element {
	ok, index := lis.validIndex(key, index)
	if !ok {
		return nil
	}
	item := lis.Record[key]
	var e *list.Element
	if item != nil && item.Len() > 0 {
		if index <= (item.Len() >> 2) {
			val := item.Front()
			for i := 0; i < index; i++ {
				val = val.Next()
			}
			e = val
		} else {
			val := item.Back()
			for i := item.Len() - 1; i > index; i-- {
				val = val.Prev()
			}
			e = val
		}
	}
	return e
}

// find element by value
func (lis *List) find(key string, val []byte) *list.Element {
	item := lis.Record[key]
	var e *list.Element
	if item != nil {
		for p := item.Front(); p != nil; p = p.Next() {
			if reflect.DeepEqual(p.Value, val) {
				e = p
				break
			}
		}
	}
	return e
}

// handel the value of start and end, ensure them could be use
func (lis *List) handleIndex(length, start, end int) (int, int) {
	if start < 0 {
		start += length
	}
	if end < 0 {
		end += length
	}
	if start < 0 {
		start = 0
	}
	if end >= length {
		end = length - 1
	}
	return start, end
}
