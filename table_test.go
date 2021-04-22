package golbat

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func key(prefix string, i int) string {
	return prefix + fmt.Sprintf("%04d", i)
}

func getTestTableOptions() Options {
	return Options{
		CompressionType:      ZSTDCompression,
		ZSTDCompressionLevel: 15,
		BlockSize:            4 * 1024,
		BloomFalsePositive:   0.01,
	}
}

func buildTestTable(t *testing.T, prefix string, n int, opts Options) *Table {
	if opts.BlockSize == 0 {
		opts.BlockSize = 4 * 1024
	}
	AssertTrue(n <= 10000)
	keyValues := make([][]string, n)
	for i := 0; i < n; i++ {
		k := key(prefix, i)
		v := fmt.Sprintf("%d", i)
		keyValues[i] = []string{k, v}
	}
	return buildTable(t, keyValues, opts)
}

func buildTable(t *testing.T, keyValues [][]string, opts Options) *Table {
	b := NewTableBuilder(opts)
	dir, err := ioutil.TempDir("", "golbat-test")
	require.NoError(t, err)
	defer removeDir(dir)

	filename := NewTableFileName(rand.Uint64(), dir)

	sort.Slice(keyValues, func(i, j int) bool {
		return keyValues[i][0] < keyValues[j][0]
	})
	for _, kv := range keyValues {
		AssertTrue(len(kv) == 2)
		b.Add(keyWithVersion([]byte(kv[0]), 0),
			EValue{Value: []byte(kv[1]), Meta: Value}, 0)
	}
	tbl, err := CreateTable(filename, b)
	require.NoError(t, err, "writing to file failed")
	return tbl
}

func TestTableBasic(t *testing.T) {
	opts := getTestTableOptions()
	tbl := buildTestTable(t, "key", 10, opts)
	defer tbl.DecrRef()

	iter := tbl.NewIterator(false)
	defer iter.Close()
	require.True(t, iter.Valid())

	count := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		k := keyWithVersion([]byte(key("key", count)), 0)
		v := fmt.Sprintf("%d", count)

		require.Equal(t, k, iter.Key())
		require.Equal(t, v, string(iter.Value().Value))
		count++
	}

	require.Equal(t, 10, count)
}

func TestTableBigValues(t *testing.T) {
	value := func(i int) []byte {
		return []byte(fmt.Sprintf("%01048576d", i)) // Return 1MB value which is > math.MaxUint16.
	}

	rand.Seed(time.Now().UnixNano())

	n := 100 // Insert 100 keys.
	opts := Options{CompressionType: ZSTDCompression, BlockSize: 4 * 1024, BloomFalsePositive: 0.01,
		TableSize: n * 1 << 20}
	builder := NewTableBuilder(opts)

	for i := 0; i < n; i++ {
		key := keyWithVersion([]byte(key("", i)), uint64(i+1))
		vs := EValue{Value: value(i), Meta: Value}
		builder.Add(key, vs, 0)
	}

	dir, err := ioutil.TempDir("", "golbat-test")
	require.NoError(t, err)
	defer removeDir(dir)

	filename := NewTableFileName(rand.Uint64(), dir)

	tbl, err := CreateTable(filename, builder)
	require.NoError(t, err, "unable to open table")
	defer tbl.DecrRef()

	iter := tbl.NewIterator(false)
	defer iter.Close()
	require.True(t, iter.Valid())

	count := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		require.Equal(t, []byte(key("", count)), parseKey(iter.Key()), "keys are not equal")
		require.Equal(t, value(count), iter.Value().Value, "values are not equal")
		count++
	}
	require.False(t, iter.Valid(), "table iterator should be invalid now")
	require.Equal(t, n, count)
	require.Equal(t, n, int(tbl.MaxVersion()))
}

func TestTableChecksum(t *testing.T) {
	rand.Seed(time.Now().Unix())
	// we are going to write random byte at random location in table file.
	rb := make([]byte, 100)
	rand.Read(rb)
	opts := getTestTableOptions()
	opts.checkMode = OnTableAndBlockRead
	tbl := buildTestTable(t, "k", 10000, opts)
	// Write random bytes at random location.
	start := rand.Intn(len(tbl.Data) - len(rb))
	n := copy(tbl.Data[start:], rb)
	require.Equal(t, n, len(rb))

	require.Panics(t, func() {
		_, err := OpenTable(tbl.MmapFile, opts)
		if ErrChecksumMismatch == err {
			panic("checksum mismatch")
		}
	})
}
