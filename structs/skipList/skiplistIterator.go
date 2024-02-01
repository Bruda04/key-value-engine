// skipList/skipListIterator.go
package skipList

import (
	"key-value-engine/structs/record"
	"strings"
)
import "key-value-engine/structs/iterator" // Update this path accordingly

type SkipListIterator struct {
	skipList      *SkipList
	currentNode   *Node
	minRange      string
	maxRange      string
	prefix        string
	rangeIterator bool
}

func (s *SkipList) NewSkipListRangeIterator(minRange, maxRange string) iterator.Iterator {
	it := &SkipListIterator{
		skipList:      s,
		minRange:      minRange,
		maxRange:      maxRange,
		rangeIterator: true,
	}

	// Initialize the iterator to the first valid node
	it.seekToFirst()

	return it
}

func (s *SkipList) NewSkipListPrefixIterator(prefix string) iterator.Iterator {
	it := &SkipListIterator{
		skipList:      s,
		prefix:        prefix,
		rangeIterator: false,
	}

	// Initialize the iterator to the first valid node
	it.seekToFirst()

	return it
}

func (it *SkipListIterator) seekToFirst() {
	it.currentNode = it.skipList.head.next[0]

	for it.currentNode != nil && it.checkCondition() {
		it.currentNode = it.currentNode.next[0]
	}

}

func (it *SkipListIterator) Valid() bool {
	return it.currentNode != nil && it.checkStopCondition()
}

func (it *SkipListIterator) Next() {
	it.currentNode = it.currentNode.next[0]

	for it.currentNode != nil && it.checkCondition() {
		it.currentNode = it.currentNode.next[0]
	}

}

func (it *SkipListIterator) Get() *record.Record {
	if it.Valid() {
		return it.currentNode.value
	}
	return nil
}

func (it *SkipListIterator) checkStopCondition() bool {
	if it.rangeIterator {
		return it.currentNode.value.GetKey() <= it.maxRange
	} else {
		return strings.HasPrefix(it.currentNode.value.GetKey(), it.prefix)
	}
}

func (it *SkipListIterator) checkCondition() bool {
	if it.rangeIterator {
		return it.currentNode.value.GetKey() < it.minRange
	} else {
		return !strings.HasPrefix(it.currentNode.value.GetKey(), it.prefix)
	}
}
