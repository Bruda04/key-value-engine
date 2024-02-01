package btree

import (
	"key-value-engine/structs/iterator"
	"key-value-engine/structs/record"
	"strings"
)

// BTreeIterator is a custom iterator for your B-tree.
// BTreeIterator represents an iterator for traversing a B-tree in sorted order.
type BTreeIterator struct {
	btree         *BTree
	node          *bTreeNode
	index         int
	keys          []*record.Record
	position      int
	minRange      string
	maxRange      string
	prefix        string
	rangeIterator bool
}

// NewBTreeRangeIterator creates a new iterator for the given B-tree.
func (bt *BTree) NewBTreeRangeIterator(minRange, maxRange string) iterator.Iterator {
	keys := make([]*record.Record, 0)
	keys = bt.innerSorted(nil, keys)

	index := 0
	for index < len(keys) && keys[index].GetKey() < minRange {
		index++
	}

	return &BTreeIterator{
		btree:         bt,
		node:          nil,
		index:         0,
		keys:          keys,
		position:      index,
		minRange:      minRange,
		maxRange:      maxRange,
		rangeIterator: true,
	}
}

// NewBTreePrefixIterator creates a new iterator for the given B-tree.
func (bt *BTree) NewBTreePrefixIterator(prefix string) iterator.Iterator {
	keys := make([]*record.Record, 0)
	keys = bt.GetSorted()
	keys = bt.innerSorted(nil, keys)

	index := 0
	for index < len(keys) && !strings.HasPrefix(keys[index].GetKey(), prefix) {
		index++
	}

	return &BTreeIterator{
		btree:         bt,
		node:          nil,
		index:         0,
		keys:          keys,
		position:      index,
		prefix:        prefix,
		rangeIterator: false,
	}
}

func (iter *BTreeIterator) Valid() bool {
	return iter.position < len(iter.keys) && iter.checkStopCondition()
}

func (iter *BTreeIterator) Next() {
	iter.position++
}

func (iter *BTreeIterator) Get() *record.Record {
	return iter.keys[iter.position]
}

func (iter *BTreeIterator) checkStopCondition() bool {
	if iter.rangeIterator {
		return iter.keys[iter.position].GetKey() <= iter.maxRange
	} else {
		return strings.HasPrefix(iter.keys[iter.position].GetKey(), iter.prefix)
	}

}
