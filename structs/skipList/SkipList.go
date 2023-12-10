package main

import (
	"math/rand"
)

type Node struct {
	value int
	next  []*Node
}

func newNode(value int, level int) *Node {
	return &Node{
		value: value,
		next:  make([]*Node, level),
	}
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
		head:      newNode(-1, maxHeight),
		height:    1,
	}
}

func (s *SkipList) Insert(value int) {
	update := make([]*Node, s.maxHeight)
	current := s.head

	for i := s.height - 1; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].value < value {
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

	newNode := newNode(value, level)

	// updating pointers to include new addition
	for i := 0; i < level; i++ {
		newNode.next[i] = update[i].next[i]
		update[i].next[i] = newNode
	}

}

func (s *SkipList) Search(value int) bool {
	current := s.head

	for i := s.height - 1; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].value < value {
			current = current.next[i]
		}
	}

	if current.next[0] != nil && current.next[0].value == value {
		return true
	}

	return false
}

func (s *SkipList) Delete(value int) {
	update := make([]*Node, s.maxHeight)
	current := s.head

	for i := s.height - 1; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].value < value {
			current = current.next[i]
		}
		update[i] = current
	}

	target := current.next[0]
	if target != nil && target.value == value {
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
