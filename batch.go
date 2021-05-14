package golbat

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"sync"
)

var emptyValue = []byte{}

type WriteBatch struct {
	entris []*entry
	ptrs   []valPtr

	wg            sync.WaitGroup
	err           error
	ref           int
	maxBatchSize  int
	maxBatchCount int
	maxVersion    uint64
	sz            uint64
}

func NewWriteBatch(option *Options) *WriteBatch {
	return &WriteBatch{
		entris:        make([]*entry, 0),
		ptrs:          make([]valPtr, 0),
		wg:            sync.WaitGroup{},
		err:           nil,
		ref:           0,
		maxBatchSize:  option.MaxBatchSize,
		maxBatchCount: option.MaxBatchCount,
		sz:            0,
	}
}

func (b *WriteBatch) Put(key, value []byte) {
	e := &entry{
		key:   key,
		value: value,
		rtype: Value,
	}

	b.entris = append(b.entris, e)
	b.sz += sizeOfEntry(e)
}

func (b *WriteBatch) Delete(key []byte) {
	e := &entry{
		key:   key,
		value: emptyValue,
		rtype: Delete,
	}

	b.entris = append(b.entris, e)
	b.sz += sizeOfEntry(e)
}

func (b *WriteBatch) IsFull() bool {
	return len(b.entris) >= b.maxBatchSize || b.sz >= uint64(b.maxBatchSize)
}

func (b *WriteBatch) Clear() {
	b.entris = make([]*entry, 0)
	b.ptrs = make([]valPtr, 0)
	b.ref = 0
	b.sz = 0
}

func (b *WriteBatch) Append(other *WriteBatch) {
	b.entris = append(b.entris, other.entris...)
	b.ptrs = append(b.ptrs, other.ptrs...)
	b.sz += other.sz
}

func (b *WriteBatch) ApproximateSize() uint64 {
	return b.sz
}

func (b *WriteBatch) setMaxVersion(s uint64) {
	b.maxVersion = s
}

func sizeOfEntry(e *entry) uint64 {
	if e == nil {
		return 0
	}

	return uint64(maxHeaderSize+len(e.key)+len(e.value)) + crc32.Size
}

func keyWithVersion(key []byte, version uint64) []byte {
	res := make([]byte, len(key)+8)
	copy(res, key)
	binary.BigEndian.PutUint64(res[len(key):], version)

	return res
}

func parseVersion(key []byte) uint64 {
	if len(key) < 8 {
		return 0
	}

	return binary.BigEndian.Uint64(key[len(key)-8:])
}

func parseKey(key []byte) []byte {
	if key == nil {
		return nil
	}
	return key[:len(key)-8]
}

func sameKey(key1, key2 []byte) bool {
	if len(key1) != len(key2) {
		return false
	}

	return bytes.Equal(parseKey(key1), parseKey(key2))
}

// first compare keys by increasing order, then compare sequence number by decreasing order
func compareKeys(a, b []byte) int {
	if cmp := bytes.Compare(parseKey(a), parseKey(b)); cmp != 0 {
		return cmp
	}

	return bytes.Compare(b[len(a)-8:], a[len(b)-8:])
}
