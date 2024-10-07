package merkledb

import (
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/maybe"
	"github.com/ava-labs/avalanchego/utils/perms"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	testShutdownByte = []byte{0x00}
	testDbFilename   = "test.db"
)

var (
	testRootKeyAddress = []byte{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88}
)

func TestEncodeDiskBranchNode(t *testing.T) {
	r := require.New(t)

	child1 := &diskChild{
		child: child{
			compressedKey: Key{
				length: 0,
				value:  "",
			},
			id:       ids.GenerateTestID(),
			hasValue: false,
		},
		address: diskAddress{
			offset: 10,
			size:   100,
		},
	}
	branchNode1 := &diskBranchNode{
		value: maybe.Maybe[[]byte]{},
		children: map[byte]*diskChild{
			0x0: child1,
		},
	}

	diskChildBytes := encodeDiskBranchNode(branchNode1)

	actualBranchNode := &diskBranchNode{}
	err := decodeDiskBranchNode(diskChildBytes, actualBranchNode)
	r.NoError(err)

	r.Equal(actualBranchNode, branchNode1)
}

func TestReadNodeFromDisk(t *testing.T) {
	r := require.New(t)

	addr := diskAddress{
		offset: 10,
		size:   100,
	}
	child1 := &diskChild{
		child: child{
			compressedKey: Key{
				length: 0,
				value:  "",
			},
			id:       ids.GenerateTestID(),
			hasValue: false,
		},
		address: diskAddress{
			offset: 10,
			size:   100,
		},
	}
	branchNode1 := &diskBranchNode{
		value: maybe.Maybe[[]byte]{},
		children: map[byte]*diskChild{
			0x0: child1,
		},
	}

	disk, err := newRawDiskForTesting([]*diskBranchNode{branchNode1})
	r.NoError(err)
	disk.readNodeFromDisk()

}

func newRawDiskForTesting(nodes []*diskBranchNode) (*rawDisk, error) {
	dir, err := os.MkdirTemp("", "rawdisk-test")
	if err != nil {
		return nil, err
	}

	file, err := os.OpenFile(filepath.Join(dir, testDbFilename), os.O_RDWR|os.O_CREATE, perms.ReadWrite)
	if err != nil {
		return nil, err
	}
	r := &rawDisk{file: file}

	// write shutdown byte
	err = r.setShutdownType(testShutdownByte)
	if err != nil {
		return nil, err
	}

	// write root key address
	_, err = file.WriteAt(testRootKeyAddress, 1)
	if err != nil {
		return nil, err
	}

	// write test disk branch nodes
	offset := int64(2)
	for _, node := range nodes {
		nodeBytes := encodeDiskBranchNode(node)
		_, err = file.WriteAt(nodeBytes, node.value)
		if err != nil {
			return nil, err
		}
		offset = offset + int64(len(nodeBytes)) + 1
	}

	return r, nil
}
