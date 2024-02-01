// btree_iterator.go
package btree

import (
	"key-value-engine/structs/iterator"
	"key-value-engine/structs/record"
)

// BTreeIterator is a custom iterator for your B-tree.
type BTreeIterator struct {
	btree    *BTree
	current  *record.Record
	minRange string
	maxRange string
}

// NewBTreeIterator creates a new iterator for the given B-tree.
func (bt *BTree) NewBTreeIterator(minRange, maxRange string) iterator.Iterator {
	it := &BTreeIterator{
		btree:    bt,
		current:  nil,
		minRange: minRange,
		maxRange: maxRange,
	}
	it.seekToFirst()
	return it
}

// seekToFirst moves the iterator to the first valid element.
func (it *BTreeIterator) seekToFirst() {
	node := it.btree.root
	for !node.leaf {
		node = node.children[0]
	}
	it.findFirstInRange(node)
}

// findFirstInRange finds the first valid element within the specified range in a leaf node.
func (it *BTreeIterator) findFirstInRange(node *bTreeNode) {
	for _, key := range node.keys {
		if key.GetKey() >= it.minRange && key.GetKey() <= it.maxRange {
			it.current = key
			return
		}
	}
	it.current = nil
}

// Valid checks if the iterator is in a valid state.
func (it *BTreeIterator) Valid() bool {
	return it.current != nil && it.current.GetKey() <= it.maxRange
}

// Next moves the iterator to the next element.
func (it *BTreeIterator) Next() {
	if it.current == nil {
		return
	}

	node := it.btree.root
	for !node.leaf {
		i := 0
		for i < len(node.keys) && it.current.GetKey() > node.keys[i].GetKey() {
			i++
		}
		if i < len(node.keys) && it.current.GetKey() == node.keys[i].GetKey() {
			node = node.children[i+1]
		} else {
			node = node.children[i]
		}
	}

	i := 0
	for i < len(node.keys) && it.current.GetKey() >= node.keys[i].GetKey() {
		i++
	}

	if i < len(node.keys) {
		it.current = node.keys[i]
	} else {
		it.current = nil
	}
}

// Get returns the record at the current iterator position.
func (it *BTreeIterator) Get() *record.Record {
	return it.current
}
