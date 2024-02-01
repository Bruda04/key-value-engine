// skipList/skipListIterator.go
package skipList

import "key-value-engine/structs/record"
import "key-value-engine/structs/iterator" // Update this path accordingly

type SkipListIterator struct {
	skipList    *SkipList
	currentNode *Node
	minRange    string
	maxRange    string
}

func (s *SkipList) NewSkipListIterator(minRange, maxRange string) iterator.Iterator {
	it := &SkipListIterator{
		skipList: s,
		minRange: minRange,
		maxRange: maxRange,
	}

	// Initialize the iterator to the first valid node
	it.seekToFirst()

	return it
}

func (it *SkipListIterator) seekToFirst() {
	it.currentNode = it.skipList.head.next[0]

	for it.currentNode != nil && it.currentNode.value.GetKey() < it.minRange {
		it.currentNode = it.currentNode.next[0]
	}
}

func (it *SkipListIterator) Valid() bool {
	return it.currentNode != nil && it.currentNode.value.GetKey() <= it.maxRange
}

func (it *SkipListIterator) Next() {
	it.currentNode = it.currentNode.next[0]

	for it.currentNode != nil && it.currentNode.value.GetKey() < it.minRange {
		it.currentNode = it.currentNode.next[0]
	}
}

func (it *SkipListIterator) Get() *record.Record {
	if it.Valid() {
		return it.currentNode.value
	}
	return nil
}
