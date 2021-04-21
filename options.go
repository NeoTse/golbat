package golbat

import (
	"runtime"
	"strconv"
)

// DB contents are stored in a set of blocks, each of which holds a
// sequence of key,value pairs.  Each block may be compressed before
// being stored in a file.  The following enum describes which
// compression method (if any) is used to compress a block.
type CompressionType uint32

const (
	// WARNING: DON'T change the existing entries!
	NoCompression CompressionType = iota
	SnappyCompression
	ZSTDCompression
)

type ChecksumVerificationMode uint32

const (
	// WARNING: DON'T change the existing entries!
	NoChecksum ChecksumVerificationMode = iota
	OnTableRead
	OnBlockRead
	OnTableAndBlockRead
)

var (
	DefaultReadOptions  = &ReadOptions{VerifyCheckSum: false, FillCache: true, Snapshot: nil}
	DefaultWriteOptions = &WriteOptions{Sync: false}
)

type Option func(*Options)

// Options are params for creating DB object.
// Each option X can be setted with WithX method.
type Options struct {
	Dir             string
	CompressionType CompressionType
	Comparator      func([]byte, []byte) int
	NumGoroutines   int
	// Logger

	MemTableSize  int
	MaxBatchSize  int
	MaxBatchCount int

	ValueLogDir        string
	ValueLogFileSize   int
	ValueLogMaxEntries int
	ValueThreshold     int

	BlockSize int
	TableSize int
	checkMode ChecksumVerificationMode

	BloomFalsePositive   float64
	ZSTDCompressionLevel int
}

type ReadOptions struct {
	VerifyCheckSum bool
	FillCache      bool
	Snapshot       *Snapshot
}

type WriteOptions struct {
	Sync bool
}

func NumGoroutines(n int) Option {
	return func(o *Options) {
		if n <= 0 || n > 2*runtime.NumCPU() {
			panic("Invaild number of goroutines used in streams: " + strconv.Itoa(n))
		}

		o.NumGoroutines = n
	}
}
