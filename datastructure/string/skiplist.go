package string

import (
	"bytes"
	"math"
	"math/rand"
	"time"
)

const (
	// the max level of the skl indexes, can be adjusted according to the actual situation.
	MaxLevel    int     = 18
	Probability float64 = 1 / math.E
)

// iterate the skl node, ends when the return value is false.
type handleEle func(e *Element) bool

type (
	// Node the skip list node.
	Node struct {
		next []*Element
	}

	// Element element is the data stored.
	Element struct {
		Node
		key   []byte
		value interface{}
	}

	// SkipList define the skip list.
	SkipList struct {
		Node
		maxLevel       int
		Len            int
		randSource     rand.Source
		probability    float64
		probTable      []float64
		prevNodesCache []*Node
	}
)

// NewSkipList create a new skip list.
func NewSkipList() *SkipList {
	return &SkipList{
		Node:           Node{next: make([]*Element, MaxLevel)},
		prevNodesCache: make([]*Node, MaxLevel),
		maxLevel:       MaxLevel,
		randSource:     rand.New(rand.NewSource(time.Now().UnixNano())),
		probability:    Probability,
		probTable:      probabilityTable(Probability, MaxLevel),
	}
}

// Key the key of the Element.
func (e *Element) Key() []byte {
	return e.key
}

// Value the value of the Element.
func (e *Element) Value() interface{} {
	return e.value
}

// SetValue set the element value.
func (e *Element) SetValue(val interface{}) {
	e.value = val
}

// 返回第一层级该节点的 next
func (e *Element) Next() *Element {
	return e.next[0]
}

// 返回第一个节点
func (t *SkipList) Front() *Element {
	return t.next[0]
}

// 插入节点，如果存在则替代
func (t *SkipList) Put(key []byte, value interface{}) *Element {
	var element *Element
	//保存每层经过的最后一个节点
	prev := t.backNodes(key)
	//如果第一层的下一个节点和要插入的key相等，就直接修改
	// <= ，这个小于条件无法达成
	if element = prev[0].next[0]; element != nil && bytes.Compare(element.key, key) <= 0 {
		element.value = value
		return element
	}
	//否则重新创建并插入
	element = &Element{
		Node: Node{
			// 插入的层数随机
			//并不是每个节点的next都有max层，对应有多少个索引
			next: make([]*Element, t.randomLevel()),
		},
		key:   key,
		value: value,
	}

	for i := range element.next {
		//插入到每层最后经历的节点之后
		element.next[i] = prev[i].next[i]
		prev[i].next[i] = element
	}

	t.Len++
	return element
}

// 根据key查询value，如果不存在就返回nil
func (t *SkipList) Get(key []byte) *Element {
	var prev = &t.Node
	var next *Element

	//从最高层开始向下查询
	for i := t.maxLevel - 1; i >= 0; i-- {
		next = prev.next[i]
		//next 不为空
		//要求key要大于next.key才能后移，当停止移动后，后面一个肯定大于等于key
		for next != nil && bytes.Compare(key, next.key) > 0 {
			prev = &next.Node
			next = next.next[i]
		}
	}
	//所以这里的 < 不能被达成，因为next.key只会大于等于
	if next != nil && bytes.Compare(next.key, key) <= 0 {
		return next
	}

	return nil
}

// Exist check if exists the key in skl.
func (t *SkipList) Exist(key []byte) bool {
	return t.Get(key) != nil
}

// Remove element by the key.
func (t *SkipList) Remove(key []byte) *Element {
	prev := t.backNodes(key)

	if element := prev[0].next[0]; element != nil && bytes.Compare(element.key, key) <= 0 {
		for k, v := range element.next {
			//k代表的是层级，v代表的是节点
			//将pre的后节点换成next，删除当前
			prev[k].next[k] = v
		}

		t.Len--
		return element
	}
	return nil
}

// Foreach iterate all elements in the skip list.
func (t *SkipList) Foreach(fun handleEle) {
	for p := t.Front(); p != nil; p = p.Next() {
		if ok := fun(p); !ok {
			break
		}
	}
}

// 从最高层开始查询key，成梯形向下查找，记录每层经过的最后一个节点
func (t *SkipList) backNodes(key []byte) []*Node {
	var prev = &t.Node
	var next *Element

	prevs := t.prevNodesCache

	for i := t.maxLevel - 1; i >= 0; i-- {
		next = prev.next[i]

		for next != nil && bytes.Compare(key, next.key) > 0 {
			prev = &next.Node
			next = next.next[i]
		}

		prevs[i] = prev
	}

	return prevs
}

// FindPrefix find the first element that matches the prefix.
func (t *SkipList) FindPrefix(prefix []byte) *Element {
	var prev = &t.Node
	var next *Element

	for i := t.maxLevel - 1; i >= 0; i-- {
		next = prev.next[i]

		for next != nil && bytes.Compare(prefix, next.key) > 0 {
			prev = &next.Node
			next = next.next[i]
		}
	}

	if next == nil {
		next = t.Front()
	}

	return next
}

// generate random index level.
func (t *SkipList) randomLevel() (level int) {
	r := float64(t.randSource.Int63()) / (1 << 63)

	level = 1
	for level < t.maxLevel && r < t.probTable[level] {
		level++
	}
	return
}

func probabilityTable(probability float64, maxLevel int) (table []float64) {
	for i := 1; i <= maxLevel; i++ {
		prob := math.Pow(probability, float64(i-1))
		table = append(table, prob)
	}
	return table
}
