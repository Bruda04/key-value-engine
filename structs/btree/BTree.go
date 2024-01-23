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
  - *Record: Pointer to the associated Record if found; otherwise, nil.
*/
func (bt *BTree) Find(recKey string) (bool, *record.Record) {
	tmpNode := bt.root

	for {
		i := 0
		for i < len(tmpNode.keys) && recKey > tmpNode.keys[i].GetKey() {
			i++
		}

		if i < len(tmpNode.keys) && recKey == tmpNode.keys[i].GetKey() {
			return true, tmpNode.keys[i]
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
