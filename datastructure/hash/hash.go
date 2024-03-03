package hash

type (
	Hash struct {
		Record record
	}
	// hash data structure
	record map[string]map[string][]byte
)

// new a hash data structure
func New() *Hash {
	return &Hash{make(record)}
}

// set key and field,if field exist ,overwrite it
func (h *Hash) HSet(key string, field string, value []byte) (res int) {

	if !h.exist(key) {
		h.Record[key] = make(map[string][]byte)
	}
	if h.Record[key][field] != nil {
		// field overwritten
		h.Record[key][field] = value
	} else {
		// create field
		h.Record[key][field] = value
		res = 1
	}
	return res
}

// set if field not exist.
func (h *Hash) HSetNx(key string, field string, value []byte) (res int) {
	if !h.exist(key) {
		h.Record[key] = make(map[string][]byte)
	}
	if _, exist := h.Record[key][field]; !exist {
		h.Record[key][field] = value
		res = 1
		return
	}
	return
}

// return data associated with specific key,field
func (h *Hash) HGet(key string, field string) []byte {
	if !h.exist(key) {
		return nil
	}
	return h.Record[key][field]
}

// return all field and value associated with specific key
func (h *Hash) HGetAll(key string) (res [][]byte) {
	if !h.exist(key) {
		return nil
	}
	for k, v := range h.Record[key] {
		res = append(res, []byte(k), v)
	}
	return
}

// delete the field if exists
func (h *Hash) HDel(key string, field string) (res int) {
	if !h.exist(key) {
		return
	}
	if _, exist := h.Record[key][field]; exist {
		delete(h.Record[key], field)
		res = 1
		return
	}
	return
}

// check whether the key exists
func (h *Hash) HKeyExists(key string) bool {
	return h.exist(key)
}

// check whether the field exists
func (h *Hash) HExists(key, field string) (exist bool) {
	if !h.exist(key) {
		return
	}
	_, exist = h.Record[key][field]
	return
}

// return the number of fields at this key
func (h *Hash) HLen(key string) (res int) {
	if !h.exist(key) {
		return
	}
	res = len(h.Record[key])
	return
}

// return all fields at this key
func (h *Hash) HKeys(key string) (res []string) {
	if !h.exist(key) {
		return
	}
	for k := range h.Record[key] {
		res = append(res, k)
	}
	return
}

// return all values at this key
func (h *Hash) HVals(key string) (res [][]byte) {
	if !h.exist(key) {
		return
	}
	for _, v := range h.Record[key] {
		res = append(res, v)
	}
	return
}

// clear the key in hash.
func (h *Hash) HClear(key string) {
	if !h.exist(key) {
		return
	}
	delete(h.Record, key)
}

func (h *Hash) exist(key string) bool {
	_, exist := h.Record[key]
	return exist
}
