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

func TestTableIterator(t *testing.T) {
	for _, n := range []int{99, 100, 101} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			opts := getTestTableOptions()
			table := buildTestTable(t, "key", n, opts)
			defer table.DecrRef()
			iter := table.NewIterator()
			defer iter.Close()

			count := 0

			for iter.SeekToFirst(); iter.Valid(); iter.Next() {
				k := keyWithVersion([]byte(key("key", count)), 0)
				v := fmt.Sprintf("%d", count)

				require.EqualValues(t, k, iter.Key())
				require.EqualValues(t, v, string(iter.Value().Value))
				count++
			}

			require.Equal(t, count, n)
		})
	}
}

func TestSeekToFirst(t *testing.T) {
	for _, n := range []int{99, 100, 101, 199, 200, 250, 9999, 10000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			opts := getTestTableOptions()
			table := buildTestTable(t, "key", n, opts)
			defer table.DecrRef()
			iter := table.NewIterator()
			defer iter.Close()
			iter.SeekToFirst()
			require.True(t, iter.Valid())
			k := keyWithVersion([]byte(key("key", 0)), 0)
			v := "0"

			require.EqualValues(t, k, iter.Key())
			require.EqualValues(t, v, string(iter.Value().Value))
		})
	}
}

func TestSeekToLast(t *testing.T) {
	for _, n := range []int{99, 100, 101, 199, 200, 250, 9999, 10000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			opts := getTestTableOptions()
			table := buildTestTable(t, "key", n, opts)
			defer table.DecrRef()
			iter := table.NewIterator()
			defer iter.Close()

			iter.SeekToLast()
			require.True(t, iter.Valid())
			k := keyWithVersion([]byte(key("key", n-1)), 0)
			v := fmt.Sprintf("%d", n-1)
			require.EqualValues(t, k, iter.Key())
			require.EqualValues(t, v, string(iter.Value().Value))

			iter.Prev()
			require.True(t, iter.Valid())
			k = keyWithVersion([]byte(key("key", n-2)), 0)
			v = fmt.Sprintf("%d", n-2)
			require.EqualValues(t, k, iter.Key())
			require.EqualValues(t, v, string(iter.Value().Value))
		})
	}
}

func TestSeek(t *testing.T) {
	opts := getTestTableOptions()
	table := buildTestTable(t, "k", 10000, opts)
	defer table.DecrRef()

	iter := table.NewIterator()
	defer iter.Close()

	var cases = []struct {
		in    string
		valid bool
		out   string
	}{
		{"abc", true, "k0000"},
		{"k0100", true, "k0100"},
		{"k0100b", true, "k0101"}, // Test case where we jump to next block.
		{"k1234", true, "k1234"},
		{"k1234b", true, "k1235"},
		{"k9999", true, "k9999"},
		{"z", false, ""},
	}

	for _, cas := range cases {
		iter.Seek(keyWithVersion([]byte(cas.in), 0))
		if !cas.valid {
			require.False(t, iter.Valid())
			continue
		}

		require.True(t, iter.Valid())
		k := parseKey(iter.Key())
		require.EqualValues(t, cas.out, string(k))
	}
}

func TestIterateFromStart(t *testing.T) {
	// Vary the number of elements added.
	for _, n := range []int{99, 100, 101, 199, 200, 250, 9999, 10000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			opts := getTestTableOptions()
			table := buildTestTable(t, "key", n, opts)
			defer table.DecrRef()
			iter := table.NewIterator()
			defer iter.Close()

			count := 0
			for iter.SeekToFirst(); iter.Valid(); iter.Next() {
				k := keyWithVersion([]byte(key("key", count)), 0)
				v := fmt.Sprintf("%d", count)

				require.EqualValues(t, k, iter.Key())
				require.EqualValues(t, v, string(iter.Value().Value))
				count++
			}

			require.EqualValues(t, n, count)
		})
	}
}

func TestIterateFromEnd(t *testing.T) {
	// Vary the number of elements added.
	for _, n := range []int{99, 100, 101, 199, 200, 250, 9999, 10000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			opts := getTestTableOptions()
			table := buildTestTable(t, "key", n, opts)
			defer table.DecrRef()
			iter := table.NewIterator()
			defer iter.Close()

			count := n
			for iter.SeekToLast(); iter.Valid(); iter.Prev() {
				k := keyWithVersion([]byte(key("key", count-1)), 0)
				v := fmt.Sprintf("%d", count-1)

				require.EqualValues(t, k, iter.Key())
				require.EqualValues(t, v, string(iter.Value().Value))
				count--
			}

			require.EqualValues(t, 0, count)
		})
	}
}

func TestUniIterator(t *testing.T) {
	opts := getTestTableOptions()
	table := buildTestTable(t, "key", 10000, opts)
	defer table.DecrRef()
	{
		iter := table.NewIterator()
		defer iter.Close()
		count := 0
		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			k := keyWithVersion([]byte(key("key", count)), 0)
			v := fmt.Sprintf("%d", count)

			require.EqualValues(t, k, iter.Key())
			require.EqualValues(t, v, string(iter.Value().Value))
			count++
		}
		require.EqualValues(t, 10000, count)
	}
	{
		iter := table.NewIterator()
		defer iter.Close()
		count := 10000
		for iter.SeekToLast(); iter.Valid(); iter.Prev() {
			k := keyWithVersion([]byte(key("key", count-1)), 0)
			v := fmt.Sprintf("%d", count-1)

			require.EqualValues(t, k, iter.Key())
			require.EqualValues(t, v, string(iter.Value().Value))
			count--
		}
		require.EqualValues(t, 0, count)
	}
}

func TestTablesIteratorOneTable(t *testing.T) {
	opts := getTestTableOptions()
	tbl := buildTable(t, [][]string{
		{"k1", "a1"},
		{"k2", "a2"},
	}, opts)

	defer tbl.DecrRef()

	iter := NewTablesIterator([]*Table{tbl})
	defer iter.Close()

	iter.SeekToFirst()
	require.True(t, iter.Valid())

	require.EqualValues(t, "k1", string(parseKey(iter.currIter.Key())))
	require.EqualValues(t, "a1", string(iter.Value().Value))
}

func TestConcatIterator(t *testing.T) {
	opts := getTestTableOptions()
	tbl := buildTestTable(t, "keya", 10000, opts)
	tbl2 := buildTestTable(t, "keyb", 10000, opts)
	tbl3 := buildTestTable(t, "keyc", 10000, opts)
	defer tbl.DecrRef()
	defer tbl2.DecrRef()
	defer tbl3.DecrRef()

	{
		iter := NewTablesIterator([]*Table{tbl, tbl2, tbl3})
		defer iter.Close()

		count := 0
		prefix := "key`"
		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			if count%10000 == 0 {
				bs := []byte(prefix)
				bs[3] += 1
				prefix = string(bs)
			}
			k := keyWithVersion([]byte(key(prefix, count%10000)), 0)
			v := fmt.Sprintf("%d", count%10000)

			require.EqualValues(t, k, iter.Key())
			require.EqualValues(t, v, string(iter.Value().Value))
			count++
		}
		require.EqualValues(t, 30000, count)

		iter.Seek(keyWithVersion([]byte("a"), 0))
		require.EqualValues(t, "keya0000", string(parseKey(iter.Key())))
		vs := iter.Value()
		require.EqualValues(t, "0", string(vs.Value))

		iter.Seek(keyWithVersion([]byte("keyb"), 0))
		require.EqualValues(t, "keyb0000", string(parseKey(iter.Key())))
		vs = iter.Value()
		require.EqualValues(t, "0", string(vs.Value))

		iter.Seek(keyWithVersion([]byte("keyb9999b"), 0))
		require.EqualValues(t, "keyc0000", string(parseKey(iter.Key())))
		vs = iter.Value()
		require.EqualValues(t, "0", string(vs.Value))

		iter.Seek(keyWithVersion([]byte("keyd"), 0))
		require.False(t, iter.Valid())
	}
	{
		iter := NewTablesIterator([]*Table{tbl, tbl2, tbl3})
		defer iter.Close()

		count := 30000
		prefix := "keyd"
		for iter.SeekToLast(); iter.Valid(); iter.Prev() {
			if count%10000 == 0 {
				bs := []byte(prefix)
				bs[3] -= 1
				prefix = string(bs)
			}
			k := keyWithVersion([]byte(key(prefix, (count-1)%10000)), 0)
			v := fmt.Sprintf("%d", (count-1)%10000)

			require.EqualValues(t, k, iter.Key())
			require.EqualValues(t, v, string(iter.Value().Value))
			count--
		}
		require.EqualValues(t, 0, count)
	}
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

	iter := tbl.NewIterator()
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
