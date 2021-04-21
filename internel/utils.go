package internel

import (
	"encoding/binary"
	"hash"
	"hash/crc32"
	"io"
	"reflect"
	"unsafe"
)

var CastagnoliCrcTable = crc32.MakeTable(crc32.Castagnoli)

type HashReader struct {
	r io.Reader
	h hash.Hash32

	bytesRead int
}

func NewHashReader(r io.Reader) *HashReader {
	hash := crc32.New(CastagnoliCrcTable)

	return &HashReader{
		r: r,
		h: hash,
	}
}

// Read reads len(p) bytes from the reader. Returns the number of bytes read, error on failure.
func (t *HashReader) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	if err != nil {
		return n, err
	}

	t.bytesRead += n
	return t.h.Write(p[:n])
}

// ReadByte reads exactly one byte from the reader. Returns error on failure.
func (t *HashReader) ReadByte() (byte, error) {
	b := make([]byte, 1)
	_, err := t.Read(b)

	return b[0], err
}

// Sum32 returns the sum32 of the underlying hash.
func (t *HashReader) Sum32() uint32 {
	return t.h.Sum32()
}

func (t *HashReader) BytesRead() int {
	return t.bytesRead
}

func Min(a, b int) int {
	if a <= b {
		return a
	}

	return b
}

func Max(a, b int) int {
	if a >= b {
		return a
	}

	return b
}

// U32ToBytes converts the given Uint32 to bytes
func U32ToBytes(v uint32) []byte {
	var uBuf [4]byte
	binary.BigEndian.PutUint32(uBuf[:], v)
	return uBuf[:]
}

// BytesToU32 converts the given byte slice to uint32
func BytesToU32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

// U32SliceToBytes converts the given Uint32 slice to byte slice
func U32SliceToBytes(u32s []uint32) []byte {
	if len(u32s) == 0 {
		return nil
	}
	var b []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	hdr.Len = len(u32s) * 4
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&u32s[0]))
	return b
}

// BytesToU32Slice converts the given byte slice to uint32 slice
func BytesToU32Slice(b []byte) []uint32 {
	if len(b) == 0 {
		return nil
	}
	var u32s []uint32
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&u32s))
	hdr.Len = len(b) / 4
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&b[0]))
	return u32s
}

// U64ToBytes converts the given Uint64 to bytes
func U64ToBytes(v uint64) []byte {
	var uBuf [8]byte
	binary.BigEndian.PutUint64(uBuf[:], v)
	return uBuf[:]
}

// BytesToU64 converts the given byte slice to uint64
func BytesToU64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// U64SliceToBytes converts the given Uint64 slice to byte slice
func U64SliceToBytes(u64s []uint64) []byte {
	if len(u64s) == 0 {
		return nil
	}
	var b []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	hdr.Len = len(u64s) * 8
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&u64s[0]))
	return b
}

// BytesToU64Slice converts the given byte slice to uint64 slice
func BytesToU64Slice(b []byte) []uint64 {
	if len(b) == 0 {
		return nil
	}
	var u64s []uint64
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&u64s))
	hdr.Len = len(b) / 8
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&b[0]))
	return u64s
}

func VerifyCheckSum(data []byte, checksum []byte) bool {
	actual := crc32.Checksum(data, CastagnoliCrcTable)
	expected := BytesToU32(checksum)
	return actual == expected
}

//go:linkname FastRand runtime.fastrand
func FastRand() uint32
