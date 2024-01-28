package merkleTree

import (
	"crypto/sha1"
	"errors"
	"math"
)

const (
	HASHVALUESIZE = 20
)

type MerkleTree struct {
	root *Node
}

type Node struct {
	data  []byte
	left  *Node
	right *Node
}

/*
MakeMerkleTree creates a Merkle tree from the provided dataBlock.

Parameters:
- dataBlock: A 2D byte slice representing the data to be included in the Merkle tree
In our project, that 2D byte slice will be SSTable's data file records

Return:
- A pointer to the MerkleTree structure representing the root of the Merkle tree.
*/
func MakeMerkleTree(dataBlock [][]byte) *MerkleTree {
	var treeNodes []*Node
	for _, value := range dataBlock {
		treeNodes = append(treeNodes, NewLeafNode(value))
	}

	for len(treeNodes) > 1 {
		var newTreeNodes []*Node
		for i := 0; i < len(treeNodes); i += 2 {
			if i+1 < len(treeNodes) {
				parentNode := AddNode(treeNodes[i], treeNodes[i+1])
				newTreeNodes = append(newTreeNodes, parentNode)
			} else {
				parentNode := AddNode(treeNodes[i], nil)
				newTreeNodes = append(newTreeNodes, parentNode)
			}
		}
		treeNodes = newTreeNodes
	}
	rootNode := treeNodes[0]

	return &MerkleTree{
		root: rootNode,
	}
}

/*
NewLeafNode hashes the provided data and creates a leaf node with the hashed data.

Parameters:
- data: A byte slice representing the data to be hashed and stored in the leaf node.

Return:
- A pointer to the Node structure representing the new leaf node.
*/
func NewLeafNode(data []byte) *Node {
	hashedData := Hash(data)
	return &Node{
		data:  hashedData,
		left:  nil,
		right: nil,
	}
}

/*
AddNode combines the data of the child nodes, hashes it, and creates a parent node.

Parameters:
- leftChild: A pointer to the left child node.
- rightChild: A pointer to the right child node. Can be nil for odd-sized nodes.

Return:
- A pointer to the Node structure representing the new parent node.
*/
func AddNode(leftChild *Node, rightChild *Node) *Node {
	var childrenData, hashedParentData []byte
	if rightChild == nil {
		hashedParentData = Hash(leftChild.data)
	} else {
		childrenData = append(leftChild.data, rightChild.data...)
		hashedParentData = Hash(childrenData)
	}
	return &Node{
		data:  hashedParentData,
		left:  leftChild,
		right: rightChild,
	}
}

/*
IncorrectElements checks the validity of elements in the provided data set and returns the path to the incorrect one.

Parameters:
- dataSet: A 2D byte slice representing the data set to be checked.

Return:
- A 2D byte slice containing paths to the incorrect elements.
*/
func (tree *MerkleTree) IncorrectElements(dataSet [][]byte) [][]byte {
	var incorrectEl [][]byte
	for _, data := range dataSet {
		validity, _ := tree.CheckValidityOfNode(data)
		if !validity {
			incorrectEl = append(incorrectEl, data)
		}
	}
	return incorrectEl
}

/*
CheckValidityOfNode checks the validity of a node in the Merkle tree and generates the path for validation.

Parameters:
- data: A byte slice representing the data of the node to be checked.

Return:
- A boolean indicating the validity of the node.
- A 2D byte slice containing the path to the node.
*/
func (tree *MerkleTree) CheckValidityOfNode(data []byte) (bool, [][]byte) {
	hashedData := Hash(data)
	valid, path := tree.generatePath(tree.root, hashedData, [][]byte{tree.root.data})
	return valid, path
}

/*
generatePath recursively generates the path for validating a node in the Merkle tree.

Parameters:
- node: A pointer to the current node being processed.
- data: A byte slice representing the data of the target node.
- path: A 2D byte slice representing the current path.

Return:
- A boolean indicating the validity of the target node.
- A 2D byte slice containing the path to the target node.
*/
func (tree *MerkleTree) generatePath(node *Node, data []byte, path [][]byte) (bool, [][]byte) {
	if node == nil {
		return false, path
	}

	if node.left == nil && node.right == nil {
		if bytesEqual(node.data, data) {
			return true, path
		}
		return false, path
	}

	leftValid, leftPath := tree.generatePath(node.left, data, append(path, node.right.data))
	if leftValid {
		return true, leftPath
	}
	if node.right != nil {
		rightValid, rightPath := tree.generatePath(node.right, data, append(path, node.left.data))
		if rightValid {
			return true, rightPath
		}
	}
	return false, [][]byte{}
}

/*
Hash computes the SHA1 hash of the provided data.

Parameters:
- data: A byte slice representing the data to be hashed.

Return:
- A byte slice representing the SHA1 hash of the input data.
*/
func Hash(data []byte) []byte {
	hash := sha1.Sum(data)
	return hash[:]
}

/*
bytesEqual checks if two byte slices are equal.

Parameters:
- a: A byte slice for comparison.
- b: A byte slice for comparison.

Return:
- A boolean indicating whether the two byte slices are equal.
*/
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

/*
SerializeMerkleTree serializes a MerkleTree to a binary-formatted byte slice.

Parameters:
- tree: A pointer to the MerkleTree structure to be serialized.

Return:
- A byte slice representing the serialized MerkleTree.
- An error if serialization fails.
*/
func SerializeMerkleTree(tree *MerkleTree) ([]byte, error) {
	// Serialize the tree data into a byte slice
	rootData, err := serializeNode(tree.root)
	if err != nil {
		return nil, err
	}

	// Construct the serialized binary data
	data := make([]byte, len(rootData))
	copy(data, rootData)

	return data, nil
}

/*
serializeNode serializes a Node to a binary-formatted byte slice.

Parameters:
- node: A pointer to the Node structure to be serialized.

Return:
- A byte slice representing the serialized Node.
- An error if serialization fails.
*/
func serializeNode(node *Node) ([]byte, error) {
	if node == nil {
		return nil, nil
	}
	// Serialize node data

	nodeData := make([]byte, HASHVALUESIZE)
	copy(nodeData, node.data)

	leftData, err := serializeNode(node.left)
	if err != nil {
		return nil, err
	}

	rightData, err := serializeNode(node.right)
	if err != nil {
		return nil, err
	}

	// Construct the serialized binary data
	data := make([]byte, HASHVALUESIZE+len(leftData)+len(rightData))
	copy(data[:HASHVALUESIZE], nodeData)
	copy(data[HASHVALUESIZE:HASHVALUESIZE+len(leftData)], leftData)
	copy(data[HASHVALUESIZE+len(leftData):], rightData)

	return data, nil
}

/*
DeserializeMerkleTree deserializes a MerkleTree from a binary-formatted byte slice.

Parameters:
- data: A byte slice representing the serialized MerkleTree.

Return:
- A pointer to the MerkleTree structure representing the deserialized MerkleTree.
- An error if deserialization fails.
*/
func DeserializeMerkleTree(data []byte) (*MerkleTree, error) {
	treeHeight := int(math.Ceil(math.Log2(float64(len(data) / HASHVALUESIZE))))

	// Check if there is enough data for the root
	if len(data) < HASHVALUESIZE {
		return nil, errors.New("insufficient data for MerkleTree root deserialization")
	}

	// Initialize global index
	index := 0

	// Create a list of bytes for tree reconstruction
	var dataSlices [][]byte
	for i := 0; i < len(data)/HASHVALUESIZE; i++ {
		dataSlices = append(dataSlices, data[i*HASHVALUESIZE:(i+1)*HASHVALUESIZE])
	}

	// Deserialize the root node
	root, _, err := deserializeNode(dataSlices, &index, treeHeight-1)
	if err != nil {
		return nil, err
	}

	return &MerkleTree{
		root: root,
	}, nil
}

/*
deserializeNode deserializes a Node from a binary-formatted byte slice.

Parameters:
- dataSlices: A 2D byte slice representing the serialized data.
- index: A pointer to the global index used during deserialization.
- treeLevel: An integer representing the level of the current node in the MerkleTree.

Return:
- A pointer to the Node structure representing the deserialized node.
- An integer representing the new value of the global index.
- An error if deserialization fails.
*/
func deserializeNode(dataSlices [][]byte, index *int, treeLevel int) (*Node, int, error) {
	if *index >= len(dataSlices) {
		return nil, treeLevel + 1, nil
	}
	if treeLevel == 0 {
		return &Node{data: dataSlices[*index]}, treeLevel + 1, nil
	}

	nodeData := make([]byte, HASHVALUESIZE)
	copy(nodeData, dataSlices[*index])
	*index++

	// Recursive deserialization of left and right subtrees
	left, _, err := deserializeNode(dataSlices, index, treeLevel-1)
	if err != nil {
		return nil, 0, err
	}

	*index++

	right, _, err := deserializeNode(dataSlices, index, treeLevel-1)
	if err != nil {
		return nil, 0, err
	}

	return &Node{
		data:  nodeData,
		left:  left,
		right: right,
	}, *index, nil
}
