// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package merkledb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/ava-labs/avalanchego/utils/maybe"
	"github.com/ava-labs/avalanchego/utils/perms"
	"os"
	"path/filepath"
)

const (
	diskAddressSize          = 16
	fileName                 = "merkle.db"
	rootKeyDiskAddressOffset = 1
)

var (
	ErrFailedToFindNode = errors.New("Failed to find node.")
)

// [offset:offset+size]
type diskAddress struct {
	offset int64
	size   int64
}

func (r diskAddress) end() int64 {
	return r.offset + r.size
}

func (r diskAddress) bytes() [16]byte {
	var bytes [16]byte
	binary.BigEndian.PutUint64(bytes[:8], uint64(r.offset))
	binary.BigEndian.PutUint64(bytes[8:], uint64(r.size))
	return bytes
}

func (r *diskAddress) decode(diskAddressBytes []byte) {
	r.offset = int64(binary.BigEndian.Uint64(diskAddressBytes))
	r.size = int64(binary.BigEndian.Uint64(diskAddressBytes[8:]))
}

type diskBranchNode struct {
	value    maybe.Maybe[[]byte]
	children map[byte]*diskChild
}

type diskChild struct {
	child   child
	address diskAddress
}

// convert dbNode to disk format
type rawDisk struct {
	// [0] = shutdownType
	// [1,17] = diskAddress of root key
	// [18,] = node store
	file *os.File

	hasher Hasher
}

func newRawDisk(dir string, hasher Hasher) (*rawDisk, error) {
	file, err := os.OpenFile(filepath.Join(dir, fileName), os.O_RDWR|os.O_CREATE, perms.ReadWrite)
	if err != nil {
		return nil, err
	}
	return &rawDisk{file: file, hasher: hasher}, nil
}

func (r *rawDisk) getShutdownType() ([]byte, error) {
	var shutdownType [1]byte
	_, err := r.file.ReadAt(shutdownType[:], 0)
	if err != nil {
		return nil, err
	}
	return shutdownType[:], nil
}

func (r *rawDisk) setShutdownType(shutdownType []byte) error {
	if len(shutdownType) != 1 {
		return fmt.Errorf("invalid shutdown type with length %d", len(shutdownType))
	}
	_, err := r.file.WriteAt(shutdownType, 0)
	return err
}

func (r *rawDisk) clearIntermediateNodes() error {
	return errors.New("clear intermediate nodes and rebuild not supported for raw disk")
}

func (r *rawDisk) Compact(start, limit []byte) error {
	return errors.New("not implemented")
}

func (r *rawDisk) HealthCheck(ctx context.Context) (interface{}, error) {
	return struct{}{}, nil
}

func (r *rawDisk) closeWithRoot(root maybe.Maybe[*node]) error {
	return errors.New("not implemented")
}

func (r *rawDisk) getRootKey() ([]byte, error) {
	rootKeyBytes := make([]byte, 16)
	_, err := r.file.ReadAt(rootKeyBytes, rootKeyDiskAddressOffset)
	if err != nil {
		return nil, err
	}
	return rootKeyBytes, nil
}

func (r *rawDisk) writeChanges(ctx context.Context, changes *changeSummary) error {
	return errors.New("not implemented")
}

func (r *rawDisk) Clear() error {
	return r.file.Truncate(0)
}

func (r *rawDisk) getNode(key Key, hasValue bool) (*node, error) {
	// read the root node
	var err error
	diskAddressBytes := make([]byte, 16)
	_, err = r.file.ReadAt(diskAddressBytes, rootKeyDiskAddressOffset)
	if err != nil {
		return nil, err
	}

	diskAddr := &diskAddress{}
	diskAddr.decode(diskAddressBytes)
	merkleRootNode, err := r.readNodeFromDisk(diskAddr)
	if err != nil {
		return nil, err
	}
	//if !key.HasPrefix(merkleRootNode) {
	//	return nil
	//}
	var (
		// all node paths start at the root
		currentNode    = merkleRootNode
		currentNodeKey = ToKey([]byte{})
		tokenSize      = 16 // TODO: configure branch factor
	)
	//if err := visitNode(currentNode); err != nil {
	//	return err
	//}
	// while the entire path hasn't been matched

	for currentNodeKey.length < key.length {
		// confirm that a child exists and grab its ID before attempting to load it
		nextChildEntry, hasChild := currentNode.children[key.Token(currentNodeKey.length, tokenSize)]

		if !hasChild || !key.iteratedHasPrefix(nextChildEntry.child.compressedKey, currentNodeKey.length+tokenSize, tokenSize) {
			// there was no child along the path or the child that was there doesn't match the remaining path
			return nil, fmt.Errorf("%w: No node at key %x", ErrFailedToFindNode, key.Bytes())
		}
		// grab the next node along the path
		// get child node
		childNode, err := r.readNodeFromDisk(&nextChildEntry.address)
		if err != nil {
			return nil, err
		}

		currentNode = childNode
		currentNodeKey = key.Take(currentNodeKey.length + tokenSize + nextChildEntry.child.compressedKey.length)
	}

	return convertDiskBranchNodeToNode(key, currentNode, r.hasher), nil
}

func convertDiskBranchNodeToNode(key Key, dbn *diskBranchNode, hasher Hasher) *node {
	nodeChildren := make(map[byte]*child, len(dbn.children))
	for childByte, dChild := range dbn.children {
		nodeChildren[childByte] = &dChild.child
	}
	n := &node{
		dbNode: dbNode{
			value:    dbn.value,
			children: nodeChildren,
		},
		key: key,
	}
	n.setValueDigest(hasher)
	return n
}

func (r *rawDisk) readNodeFromDisk(address *diskAddress) (*diskBranchNode, error) {
	bytes := make([]byte, int(address.size))

	_, err := r.file.ReadAt(bytes, address.offset)
	if err != nil {
		return nil, err
	}

	dbn := &diskBranchNode{}
	err = decodeDiskBranchNode(bytes, dbn)
	if err != nil {
		return nil, err
	}

	return dbn, nil
}

func (r *rawDisk) writeDiskAtNode(offset int64, branchNodeBytes []byte) error {
	_, err := r.file.WriteAt(branchNodeBytes, offset)
	if err != nil {
		return err
	}
	return nil
}

func (r *rawDisk) cacheSize() int {
	return 0 // TODO add caching layer
}
