package skipList

import (
	"key-value-engine/structs/record"
	"math/rand"
)

type Node struct {
	value *record.Record
	next  []*Node
}

func newNode(record *record.Record, level int) *Node {
	return &Node{
		value: record,
		next:  make([]*Node, level),
	}
}

func (s *SkipList) Head() *Node {
	return s.head
}

func (n *Node) GetNext() []*Node {
	return n.next
}

func (n *Node) GetValue() *record.Record {
	return n.value
}

type SkipList struct {
	maxHeight int
	head      *Node
	height    int
}

/*
Initialize Skip List

	-accepts the maximum height we allow
*/
func MakeSkipList(maxHeight int) *SkipList {
	return &SkipList{
		maxHeight: maxHeight,
		head:      newNode(nil, maxHeight),
		height:    1,
	}
}

func (s *SkipList) Insert(val *record.Record) {
	update := make([]*Node, s.maxHeight)
	current := s.head

	// Update/delete
	exists, existingNode := s.FindNode(val.GetKey())

	if exists {
		// Replace the existing value with the new one
		existingNode.value = val
		return
	}

	for i := s.height - 1; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].value.GetKey() < val.GetKey() {
			current = current.next[i]
		}
		update[i] = current
	}

	//finding what level to assign to my new node
	level := s.roll()
	if level > s.height {
		for i := s.height; i < level; i++ {
			update[i] = s.head
		}
		s.height = level
	}

	newNode := newNode(val, level)

	// updating pointers to include new addition
	for i := 0; i < level; i++ {
		newNode.next[i] = update[i].next[i]
		update[i].next[i] = newNode
	}

}

func (s *SkipList) Find(key string) bool {
	current := s.head

	for i := s.height - 1; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].value.GetKey() < key {
			current = current.next[i]
		}
	}

	if current.next[0] != nil && current.next[0].value.GetKey() == key {
		return !current.next[0].value.IsTombstone() //current.next[0].value
	}

	return false
}

func (s *SkipList) FindNode(key string) (bool, *Node) {
	current := s.head

	for i := s.height - 1; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].value.GetKey() < key {
			current = current.next[i]
		}
	}

	if current.next[0] != nil && current.next[0].value.GetKey() == key {
		return true, current.next[0]
	}

	return false, nil
}

func (s *SkipList) Delete(key string) {
	update := make([]*Node, s.maxHeight)
	current := s.head

	for i := s.height - 1; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].value.GetKey() < key {
			current = current.next[i]
		}
		update[i] = current
	}

	target := current.next[0]
	if target != nil && target.value.GetKey() == key {
		for i := 0; i < s.height; i++ {
			if update[i].next[i] != target {
				break
			}
			update[i].next[i] = target.next[i]
		}

		// Update the height of the skiplist if necessary
		for s.height > 1 && s.head.next[s.height-1] == nil {
			s.height--
		}
	}
}

func (s *SkipList) GetSortedList() []*record.Record {
	var sortedList []*record.Record
	current := s.head.next[0]

	for current != nil {
		sortedList = append(sortedList, current.value)
		current = current.next[0]
	}

	return sortedList
}

func (s *SkipList) GetRangeSortedList(minRange, maxRange string) []*record.Record {
	var sortedList []*record.Record
	current := s.head.next[0]

	for current != nil {
		if current.value.GetKey() >= minRange && current.value.GetKey() <= maxRange {
			sortedList = append(sortedList, current.value)
		}
		current = current.next[0]
	}

	return sortedList
}

func (s *SkipList) roll() int {
	level := 1
	// possible ret values from rand are 0 and 1
	// we stop when we get a 0
	for ; rand.Int31n(2) == 1; level++ {
		if level >= s.maxHeight {
			return level
		}
	}
	return level
}
