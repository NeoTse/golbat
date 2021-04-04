package internel

import (
	"bytes"
	"hash"
	"hash/crc32"
	"io"
	"strconv"
	_ "unsafe"
)

var CastagnoliCrcTable = crc32.MakeTable(crc32.Castagnoli)

const signedByteMax = 128

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

type EValue struct {
	Tag   byte
	Value []byte
}

func (ev *EValue) Decode(b []byte) {
	ev.Tag = b[0]
	ev.Value = b[1:]
}

func (ev *EValue) Encode() []byte {
	sz := 1 + len(ev.Value)
	b := make([]byte, sz)
	b[0] = ev.Tag
	copy(b[1:], ev.Value)

	return b
}

func (ev *EValue) EncodeTo(buf *bytes.Buffer) {
	buf.WriteByte(ev.Tag)
	buf.Write(ev.Value)
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

func DecodeFixed32(s []byte) uint32 {
	checkLen(s, 4)

	return uint32(s[0]) |
		uint32(s[1])<<8 |
		uint32(s[2])<<16 |
		uint32(s[3])<<24
}

func EncodeFixed32(dst []byte, i uint32) {
	checkLen(dst, 4)

	dst[0] = byte(i)
	dst[1] = byte(i >> 8)
	dst[2] = byte(i >> 16)
	dst[3] = byte(i >> 24)
}

func EncodeVarint32(i uint32) []byte {
	res := make([]byte, 0, 5)

	for k := 0; i >= signedByteMax; k++ {
		res[k] = byte(i | signedByteMax)
		i >>= 7
	}

	return res
}

func DecodeFixed64(s []byte) uint64 {
	checkLen(s, 8)

	return uint64(s[0]) |
		uint64(s[1])<<8 |
		uint64(s[2])<<16 |
		uint64(s[3])<<24 |
		uint64(s[4])<<32 |
		uint64(s[5])<<40 |
		uint64(s[6])<<48 |
		uint64(s[7])<<56
}

func EnCodeFixed64(dst []byte, i uint64) {
	checkLen(dst, 8)

	dst[0] = byte(i)
	dst[1] = byte(i >> 8)
	dst[2] = byte(i >> 16)
	dst[3] = byte(i >> 24)
	dst[4] = byte(i >> 32)
	dst[5] = byte(i >> 40)
	dst[6] = byte(i >> 48)
	dst[7] = byte(i >> 56)
}

func EncodeVarint64(i uint64) []byte {
	res := make([]byte, 0, 10)

	for k := 0; i >= signedByteMax; k++ {
		res[k] = byte(i | signedByteMax)
		i >>= 7
	}

	return res
}

func checkLen(s []byte, atLeast int) {
	if len(s) < atLeast {
		panic("Bad slice with length less than " + strconv.Itoa(atLeast))
	}
}

//go:linkname FastRand runtime.fastrand
func FastRand() uint32
