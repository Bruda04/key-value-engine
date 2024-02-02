package btree

import (
	"errors"
	"key-value-engine/structs/record"
)

type BTree struct {
	m    int
	root *bTreeNode
}

/*
MakeBTree initializes and returns a new B-tree with the specified degree 'm'.

Parameters:
  - m: An integer representing the degree of the B-tree.

Returns:
  - *BTree: Pointer to the newly created B-tree.
*/
func MakeBTree(m int) (*BTree, error) {
	if m < 4 {
		return nil, errors.New("m must be at least 4 for a valid B-tree")
	}
	return &BTree{
		m:    m,
		root: makebTreeNode(true, m),
	}, nil
}

/*
Find searches for a key 'recKey' in the B-tree and returns a boolean indicating whether the key is found,
along with a pointer to the associated Record if found.

Receiver:
  - bt: Pointer to a BTree, representing the B-tree to search in.

Parameters:
  - recKey: A string representing the key to search for.

Returns:
  - bool: True if the key is found; otherwise, false.
*/
func (bt *BTree) Find(recKey string) (bool, *record.Record) {
	tmpNode := bt.root

	for {
		i := 0
		for i < len(tmpNode.keys) && recKey > tmpNode.keys[i].GetKey() {
			i++
		}

		if i < len(tmpNode.keys) && recKey == tmpNode.keys[i].GetKey() {
			return !tmpNode.keys[i].IsTombstone(), tmpNode.keys[i]
		} else if tmpNode.leaf {
			return false, nil
		} else {
			tmpNode = tmpNode.children[i]
		}
	}
}

/*
Insert adds a key (Record) into the B-tree.

Receiver:
  - bt: Pointer to a BTree, representing the B-tree.

Parameters:
  - rec: Pointer to a Record, representing the key to be inserted.
*/
func (bt *BTree) Insert(rec *record.Record) {
	if bt.tryUpdate(rec) {
		return
	}

	node := bt.root
	if node.isFull() {
		newRoot := makebTreeNode(false, bt.m)
		newRoot.insertChild(bt.root)
		node = node.split(newRoot, rec)
		bt.root = newRoot
	}
	bt.insertNonFull(node, rec)
}

/*
insertNonFull inserts a key (Record) into a non-full B-tree node.

Receiver:
  - bt: Pointer to a BTree, representing the B-tree.

Parameters:
  - n: Pointer to a bTreeNode, representing the non-full B-tree node.
  - rec: Pointer to a Record, representing the key to be inserted.
*/
func (bt *BTree) insertNonFull(n *bTreeNode, rec *record.Record) {
	for !n.leaf {
		i := len(n.keys) - 1

		// Find the appropriate position for insertion in the current node's keys
		for i > 0 && rec.GetKey() < n.keys[i].GetKey() {
			i--
		}
		if rec.GetKey() > n.keys[i].GetKey() {
			i++
		}

		// Move to the next child node based on the identified position
		next := n.children[i]

		// If the next child is full, perform a split operation
		if next.isFull() {
			n = next.split(n, rec)
		} else {
			n = next
		}
	}
	n.insertKey(rec)
}

/*
tryUpdate attempts to update a Record in the B-tree with the given key.

Receiver:
  - bt: Pointer to a BTree, representing the B-tree.

Parameters:
  - rec: Pointer to a Record, representing the key to be updated.

Returns:
  - bool: True if the update is successful; otherwise, false.

Implementation Details:

	The function searches for the key in the B-tree and attempts to update the associated
	Record with the provided Record. If the key is found, the associated Record is updated,
	and the function returns true. If the key is not found, the function returns false.
*/
func (bt *BTree) tryUpdate(rec *record.Record) bool {
	recKey := rec.GetKey()
	tmpNode := bt.root

	for {
		i := 0
		for i < len(tmpNode.keys) && recKey > tmpNode.keys[i].GetKey() {
			i++
		}

		if i < len(tmpNode.keys) && recKey == tmpNode.keys[i].GetKey() {
			tmpNode.keys[i] = rec
			return true
		} else if tmpNode.leaf {
			return false
		} else {
			tmpNode = tmpNode.children[i]
		}
	}
}

/*
GetSorted returns a sorted slice of Records from the B-tree.

Receiver:
  - bt: Pointer to a BTree, representing the B-tree.

Returns:
  - []*Record: A sorted slice of Records.
*/
func (bt *BTree) GetSorted() []*record.Record {
	ret := make([]*record.Record, 0)
	ret = bt.innerSorted(nil, ret)

	return ret
}

/*
innerSorted recursively traverses the B-tree in sorted order and appends Records
to the provided slice.

Receiver:
  - bt: Pointer to a BTree, representing the B-tree.

Parameters:
  - n: Pointer to a bTreeNode, representing the current node in traversal.
  - slice: A slice of Records to which the sorted Records are appended.

Returns:
  - []*Record: The updated slice containing sorted Records.
*/
func (bt *BTree) innerSorted(n *bTreeNode, slice []*record.Record) []*record.Record {
	if n == nil {
		n = bt.root
	}

	if n.leaf {
		for i := 0; i < len(n.keys); i++ {
			slice = append(slice, n.keys[i])
		}
	}

	for i := 0; i < len(n.children); i++ {
		slice = bt.innerSorted(n.children[i], slice)
		if i < len(n.keys) {
			slice = append(slice, n.keys[i])
		}
	}

	return slice
}
