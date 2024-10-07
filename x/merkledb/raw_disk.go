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
}

func newRawDisk(dir string) (*rawDisk, error) {
	file, err := os.OpenFile(filepath.Join(dir, fileName), os.O_RDWR|os.O_CREATE, perms.ReadWrite)
	if err != nil {
		return nil, err
	}
	return &rawDisk{file: file}, nil
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
	return nil, errors.New("not implemented")
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

func (r *rawDisk) cacheSize() int {
	return 0 // TODO add caching layer
}
