package btree

import "key-value-engine/structs/record"

type bTreeNode struct {
	leaf     bool
	keys     []*record.Record
	children []*bTreeNode
	m        int
}

func makebTreeNode(leaf bool, m int) *bTreeNode {
	return &bTreeNode{
		leaf:     leaf,
		keys:     []*record.Record{},
		children: []*bTreeNode{},
		m:        m,
	}
}

/*
isFull checks whether the B-tree node is full, i.e., it has the maximum allowed number
of keys.

Receiver:
  - n: Pointer to a bTreeNode, representing the B-tree node under consideration.

Returns:
  - bool: True if the node is full; otherwise, false.
*/
func (n *bTreeNode) isFull() bool {
	return len(n.keys) == n.m-1
}

/*
size returns the number of keys in the B-tree node.

Receiver:
  - n: Pointer to a bTreeNode, representing the B-tree node.

Returns:
  - int: The number of keys in the B-tree node.
*/
func (n *bTreeNode) size() int {
	return len(n.keys)
}

/*
insertChild inserts a child B-tree node into the current node while maintaining the
sorted order of children based on the keys.

Receiver:
  - n: Pointer to a bTreeNode, representing the current B-tree node.

Parameters:
  - node: Pointer to a bTreeNode, representing the child node to be inserted.

Note:

	This method assumes that the B-tree node is not full before the insertion.
*/
func (n *bTreeNode) insertChild(node *bTreeNode) {
	i := len(n.children) - 1
	for i >= 0 && n.children[i].keys[0].GetKey() > node.keys[0].GetKey() {
		i--
	}
	i++

	if len(n.children) == i || len(n.children) == 0 {
		n.children = append(n.children, node)
		return
	}
	n.children = append(n.children[:i+1], n.children[i:]...)
	n.children[i] = node
}

/*
insertKey inserts a key into the B-tree node while maintaining the sorted order of keys.

Receiver:
  - n: Pointer to a bTreeNode, representing the current B-tree node.

Parameters:
  - key: Pointer to a Record, representing the key to be inserted.

Note:

	This method assumes that the B-tree node is not full before the insertion.
*/
func (n *bTreeNode) insertKey(key *record.Record) {
	i := 0
	for i < len(n.keys) && key.GetKey() > n.keys[i].GetKey() {
		i++
	}

	if len(n.keys) == i || len(n.keys) == 0 {
		n.keys = append(n.keys, key)
		return
	}
	n.keys = append(n.keys[:i+1], n.keys[i:]...)
	n.keys[i] = key
}

/*
split performs a split operation on the current B-tree node, creating a new node and updating
the parent node accordingly. It is used during the insertion process to maintain the balance
and order of the B-tree.

Receiver:
  - n: Pointer to a bTreeNode, representing the current B-tree node.

Parameters:
  - parent: Pointer to a bTreeNode, representing the parent node of the current node.
  - record: Pointer to a Record, used for updating the parent node during the split.

Returns:
  - *bTreeNode: Pointer to the new B-tree node created as a result of the split.

Note:

	This method assumes that the B-tree node is full before the split operation.
*/
func (n *bTreeNode) split(parent *bTreeNode, record *record.Record) *bTreeNode {
	newNode := makebTreeNode(true, n.m)

	middle := n.size() / 2
	splitRecord := n.keys[middle]
	parent.insertKey(splitRecord)

	if middle+1 <= len(n.children) {
		newNode.children = append(newNode.children, n.children[middle+1:]...)

		n.children = n.children[:middle+1]
	}

	newNode.keys = append(newNode.keys, n.keys[middle+1:]...)

	n.keys = n.keys[:middle]

	if len(newNode.children) > 0 {
		newNode.leaf = false
	}

	parent.insertChild(newNode)
	if record.GetKey() < splitRecord.GetKey() {
		return n
	} else {
		return newNode
	}
}
