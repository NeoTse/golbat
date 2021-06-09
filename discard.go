package golbat

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/neotse/golbat/internel"
)

const discardFileName = "DISCARD"
const (
	initDiscardSize = 1 << 20 // 1 MB(total 64K discard entries)
	entrySize       = 16      // leftmost 8 bit for fid, rightmost 8 bit for length of discarded records
)

type discard struct {
	next int

	sync.Mutex // because the difference reader and writer
	*internel.MmapFile
}

func NewDiscard(option Options) (*discard, error) {
	fname := filepath.Join(option.ValueLogDir, discardFileName)

	m, err := internel.OpenMmapFile(fname, os.O_CREATE|os.O_RDWR, initDiscardSize)

	if err != nil {
		return nil, Wrapf(err, "while open discard file: %s\n", fname)
	}

	d := &discard{
		MmapFile: m,
	}

	if m.NewFile {
		d.zeroNextEntry()
	}

	for i := 0; i < d.cap(); i++ {
		if d.get(i*entrySize) == 0 {
			d.next = i
			break
		}
	}

	sort.Sort(d)

	return d, nil
}

func (d *discard) Update(fid uint32, count uint64) uint64 {
	return d._update(uint64(fid), int64(count))
}

func (d *discard) Get(fid uint32) uint64 {
	return d._update(uint64(fid), 0)
}

func (d *discard) Reset(fid uint32) uint64 {
	return d._update(uint64(fid), -1)
}

func (d *discard) _update(fid uint64, count int64) uint64 {
	d.Lock()
	defer d.Unlock()

	index := sort.Search(d.next, func(i int) bool { return d.get(i*entrySize) >= fid })
	if index < d.next && d.get(index*entrySize) == fid {
		coffset := index*entrySize + 8
		if count < 0 {
			d.set(coffset, 0)
			return 0
		}

		ncount := d.get(coffset) + uint64(count)
		if count == 0 {
			return ncount
		}

		d.set(coffset, ncount)
		return ncount
	}

	if count < 0 {
		count = 0
	}

	d.set(d.next*entrySize, fid)
	d.set(d.next*entrySize+8, uint64(count))

	d.next++
	if d.next > d.cap() {
		Check(d.Truncate(int64(2 * len(d.Data))))
	}
	d.zeroNextEntry()

	sort.Sort(d)

	return uint64(count)
}

func (d *discard) Max() (uint32, uint64) {
	d.Lock()
	defer d.Unlock()

	var maxFid, maxCount uint64
	d.Iterate(func(fid, count uint64) {
		if count > maxCount {
			maxFid = fid
			maxCount = count
		}
	})
	return uint32(maxFid), maxCount
}

func (d *discard) Iterate(f func(fid, count uint64)) {
	for i := 0; i < d.next; i++ {
		f(d.get(i*entrySize), d.get(i*entrySize+8))
	}
}

func (d *discard) Len() int {
	return d.next
}

func (d *discard) Less(i, j int) bool {
	return d.get(i*entrySize) < d.get(j*entrySize)
}

func (d *discard) Swap(i, j int) {
	l := d.Data[i*entrySize : i*entrySize+16]
	r := d.Data[j*entrySize : j*entrySize+16]
	tmp := make([]byte, 16)
	copy(tmp, l)
	copy(l, r)
	copy(r, tmp)
}

func (d *discard) cap() int {
	return len(d.Data) / entrySize
}

func (d *discard) set(offset int, val uint64) {
	binary.BigEndian.PutUint64(d.Data[offset:offset+8], val)
}

func (d *discard) get(offset int) uint64 {
	return binary.BigEndian.Uint64(d.Data[offset : offset+8])
}

func (d *discard) zeroNextEntry() {
	d.set(d.next*entrySize, 0)
	d.set(d.next*entrySize+8, 0)
}
