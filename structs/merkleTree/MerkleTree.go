package merkleTree

import (
	"crypto/sha1"
	"encoding/hex"
)

type MerkleTree struct {
	Root *Node
}

type Node struct {
	Data  [20]byte
	Left  *Node
	Right *Node
}

/*
MakeMerkleTree creates a Merkle tree from the provided dataBlock.

Parameters:
- dataBlock: Array of strings of data.
Return Value:
- *MerkleTree: Pointer to the generated MerkleTree structure.
*/
func MakeMerkleTree(dataBlock []string) *MerkleTree {
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
				newTreeNodes = append(newTreeNodes, treeNodes[i])
			}
		}

		treeNodes = newTreeNodes
	}
	rootNode := treeNodes[0]

	return &MerkleTree{
		Root: rootNode,
	}
}

/*
NewLeafNode Hashes the provided data and creates a leaf node with the hashed data.

Parameters:
- dataStr: String representing the data for the leaf node.
Return Value:
- *Node: Pointer to the newly created leaf node.
*/
func NewLeafNode(dataStr string) *Node {
	data := Hash([]byte(dataStr))
	return &Node{
		Data:  data,
		Left:  nil,
		Right: nil,
	}
}

/*
AddNode Combines the data of the child nodes, hashes it, and creates a parent node.

Parameters:
- leftChild: Pointer to the left child node.
- rightChild: Pointer to the right child node.
Return Value:
- *Node: Pointer to the newly created parent node.
*/
func AddNode(leftChild *Node, rightChild *Node) *Node {
	childrenData := append(leftChild.Data[:], rightChild.Data[:]...)
	hashedParentData := Hash(childrenData)
	return &Node{
		Data:  hashedParentData,
		Left:  leftChild,
		Right: rightChild,
	}
}

/*
IncorrectElements Checks the validity of elements in the provided data set and returns the path to the incorrect one.

Parameters:
- dataSet: Slice of strings representing the data set to be checked.
Return Value:
- []string: Slice containing incorrect elements found in the Merkle tree.
*/
func (tree *MerkleTree) IncorrectElements(dataSet []string) []string {
	var incorrectEl []string
	for _, data := range dataSet {
		validity, _ := tree.CheckValidityOfNode(data)
		if !validity {
			incorrectEl = append(incorrectEl, data)
		}
	}
	return incorrectEl
}

/*
CheckValidityOfNode Checks the validity of a node in the Merkle tree and generates the path for validation.

Parameters:
- dataStr: String representing the data to be checked for validity.
Return Value:
- bool: Boolean indicating whether the node is valid or not.
- []string: Slice containing the path of the node for validation.
*/
func (tree *MerkleTree) CheckValidityOfNode(dataStr string) (bool, []string) {
	data := Hash([]byte(dataStr))
	valid, path := tree.generatePath(tree.Root, data, []string{})
	return valid, path
}

/*
generatePath Recursively generates the path for validating a node in the Merkle tree.

Parameters:
- node: Pointer to the current node being evaluated.
- data: [20]byte representing the data to be validated.
- path: Slice containing the path information for validation.
Return Value:
- bool: Boolean indicating whether the validation is successful or not.
- []string: Slice containing the path for validation.
*/
func (tree *MerkleTree) generatePath(node *Node, data [20]byte, path []string) (bool, []string) {
	if node == nil {
		return false, path
	}

	if node.Left == nil && node.Right == nil {
		if node.Data == data {
			return true, path
		}
		return false, path
	}

	leftValid, leftPath := tree.generatePath(node.Left, data, append(path, node.Right.String()))
	if leftValid {
		return true, leftPath
	}

	rightValid, rightPath := tree.generatePath(node.Right, data, append(path, node.Left.String()))
	if rightValid {
		return true, rightPath
	}
	return false, []string{}
}

/*
String Converts the node's data into a hexadecimal string.

Parameters:
- n: Pointer to the node.
Return Value:
- string: Hexadecimal string representation of the node's data.
*/
func (n *Node) String() string {
	return hex.EncodeToString(n.Data[:])
}

/*
Hash computes the SHA1 hash of the provided data.

Parameters:
- data: Byte slice representing the data to be hashed.
Return Value:
- [20]byte: SHA1 hash of the provided data.
*/
func Hash(data []byte) [20]byte {
	return sha1.Sum(data)
}
