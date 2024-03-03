package set

var existFlag = struct{}{}

type (
	Set struct {
		Record record
	}
	// set data structure
	// set a struct make operate easier
	record map[string]map[string]struct{}
)

// new a set
func New() *Set {
	return &Set{make(record)}
}

// add member to set at this key
func (s *Set) SAdd(key string, member []byte) (res int) {
	if !s.exist(key) {
		s.Record[key] = make(map[string]struct{})
	}
	s.Record[key][string(member)] = existFlag
	res = len(s.Record[key])
	return
}

// pop and delete one or more random member
func (s *Set) SPop(key string, count int) [][]byte {
	var res [][]byte
	if !s.exist(key) || count <= 0 {
		return res
	}
	for k := range s.Record[key] {
		delete(s.Record[key], k)
		count--
		if count == 0 {
			break
		}
	}
	return res
}

// return whether member is a member of the key
func (s *Set) SIsMember(key string, member []byte) bool {
	return s.fieldExist(key, string(member))
}

// return a random element from the set at this key
// when called with just the key argument
func (s *Set) SRandMember(key string, count int) [][]byte {
	var val [][]byte
	if !s.exist(key) || count == 0 {
		return val
	}

	if count > 0 {
		for k := range s.Record[key] {
			val = append(val, []byte(k))
			if len(val) == count {
				break
			}
		}
	} else {
		count = -count
		randomVal := func() []byte {
			for k := range s.Record[key] {
				return []byte(k)
			}
			return nil
		}

		for count > 0 {
			val = append(val, randomVal())
			count--
		}
	}
	return val
}

// Remove the specified member from the set at this key
func (s *Set) SRem(key string, member []byte) (res bool) {
	if !s.exist(key) {
		return
	}
	if _, ok := s.Record[key][string(member)]; ok {
		delete(s.Record[key], string(member))
		res = true
		return
	}
	return res
}

// move member from src to dst
func (s *Set) SMove(src, dst string, member []byte) bool {
	if !s.fieldExist(src, string(member)) {
		return false
	}

	if !s.exist(dst) {
		s.Record[dst] = make(map[string]struct{})
	}

	delete(s.Record[src], string(member))
	s.Record[dst][string(member)] = existFlag

	return true
}

// return the number of members at this key
func (s *Set) SCard(key string) int {
	if !s.exist(key) {
		return 0
	}
	return len(s.Record[key])
}

// return all the members at this key
func (s *Set) SMembers(key string) (val [][]byte) {
	if !s.exist(key) {
		return
	}
	for k := range s.Record[key] {
		val = append(val, []byte(k))
	}
	return
}

// return all members of key1 ∪ key2 ..
func (s *Set) SUnion(keys ...string) (val [][]byte) {
	m := make(map[string]bool)
	for _, k := range keys {
		if s.exist(k) {
			for v := range s.Record[k] {
				m[v] = true
			}
		}
	}

	for v := range m {
		val = append(val, []byte(v))
	}
	return
}

// return all members of key1 - key2 ..
func (s *Set) SDiff(keys ...string) (val [][]byte) {
	if len(keys) == 0 || !s.exist(keys[0]) {
		return
	}

	for v := range s.Record[keys[0]] {
		flag := true
		for i := 1; i < len(keys); i++ {
			if s.SIsMember(keys[i], []byte(v)) {
				flag = false
				break
			}
		}
		if flag {
			val = append(val, []byte(v))
		}
	}
	return
}

// clear the specified key
func (s *Set) SClear(key string) {
	if s.exist(key) {
		delete(s.Record, key)
	}
}

// check if the key exist
func (s *Set) SKeyExists(key string) (ok bool) {
	return s.exist(key)
}

// check whether the key is exist
func (s *Set) exist(key string) (exist bool) {
	_, exist = s.Record[key]
	return
}

// check whether the member is exist
func (s *Set) fieldExist(key string, field string) (exist bool) {
	fields, exist := s.Record[key]
	if !exist {
		return exist
	}
	_, exist = fields[field]
	return
}
