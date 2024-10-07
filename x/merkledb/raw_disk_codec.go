package merkledb

import (
	"math"
	"slices"
)

// Assumes [n] is non-nil.
func encodeDiskBranchNode(n *diskBranchNode) []byte {
	length := encodeDiskBranchNodeSize(n)
	w := codecWriter{
		b: make([]byte, 0, length),
	}

	w.MaybeBytes(n.value)

	numChildren := len(n.children)
	w.Uvarint(uint64(numChildren))

	// Avoid allocating keys entirely if the node doesn't have any children.
	if numChildren == 0 {
		return w.b
	}

	// By allocating BranchFactorLargest rather than [numChildren], this slice
	// is allocated on the stack rather than the heap. BranchFactorLargest is
	// at least [numChildren] which avoids memory allocations.
	keys := make([]byte, numChildren, BranchFactorLargest)
	i := 0
	for k := range n.children {
		keys[i] = k
		i++
	}

	// Ensure that the order of entries is correct.
	slices.Sort(keys)
	for _, index := range keys {
		entry := n.children[index]
		w.Uvarint(uint64(index))
		w.Key(entry.child.compressedKey)
		w.ID(entry.child.id)
		w.Bool(entry.child.hasValue)
		w.Uvarint(uint64(entry.address.offset))
		w.Uvarint(uint64(entry.address.size))
	}

	return w.b
}

// Assumes [n] is non-nil.
func decodeDiskBranchNode(b []byte, n *diskBranchNode) error {
	r := codecReader{
		b:    b,
		copy: true,
	}

	var err error
	n.value, err = r.MaybeBytes()
	if err != nil {
		return err
	}

	numChildren, err := r.Uvarint()
	if err != nil {
		return err
	}
	if numChildren > uint64(BranchFactorLargest) {
		return errTooManyChildren
	}

	n.children = make(map[byte]*diskChild, numChildren)
	var previousChild uint64
	for i := uint64(0); i < numChildren; i++ {
		index, err := r.Uvarint()
		if err != nil {
			return err
		}
		if (i != 0 && index <= previousChild) || index > math.MaxUint8 {
			return errChildIndexTooLarge
		}
		previousChild = index

		compressedKey, err := r.Key()
		if err != nil {
			return err
		}
		childID, err := r.ID()
		if err != nil {
			return err
		}
		hasValue, err := r.Bool()
		if err != nil {
			return err
		}
		offset, err := r.Uvarint()
		if err != nil {
			return err
		}
		size, err := r.Uvarint()
		if err != nil {
			return err
		}
		n.children[byte(index)] = &diskChild{
			child: child{
				compressedKey: compressedKey,
				id:            childID,
				hasValue:      hasValue,
			},
			address: diskAddress{
				offset: int64(offset),
				size:   int64(size),
			},
		}
	}
	if len(r.b) != 0 {
		return errExtraSpace
	}
	return nil
}

// Assumes [n] is non-nil.
func encodeDiskBranchNodeSize(n *diskBranchNode) int {
	// * number of children
	// * disk address
	// * bool indicating whether [n] has a value
	// * the value (optional)
	// * children
	size := uintSize(uint64(len(n.children))) + boolLen
	size += diskAddressSize * len(n.children)
	if n.value.HasValue() {
		valueLen := len(n.value.Value())
		size += uintSize(uint64(valueLen)) + valueLen
	}
	// for each non-nil entry, we add the additional size of the child entry
	for index, entry := range n.children {
		size += childSize(index, &entry.child)
	}
	return size
}
