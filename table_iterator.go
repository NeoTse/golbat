package golbat

import (
	"io"
	"sort"
)

// TableIterator uses to iterate all entries int the table than iterator belong to
type TableIterator struct {
	table *Table
	biter *blockIterator

	blockId int
	err     error
}

func (t *Table) NewIterator() *TableIterator {
	t.IncrRef()

	return &TableIterator{table: t}
}

// Seek return the first entry that is >= input key from start
func (iter *TableIterator) Seek(key []byte) {
	iter.seekFrom(key, restart)
}

// Seek return the first entry that is >= input key from pos (may be start or last pos)
func (iter *TableIterator) seekFrom(key []byte, mode seekMode) {
	iter.err = nil
	if mode == restart {
		iter.reset()
	}

	found := sort.Search(iter.table.BlockCount(), func(i int) bool {
		currKey := iter.table.index.baseKeys[i]

		return compareKeys(currKey, key) > 0
	})

	if found == 0 {
		// the smallest key in table is greater than input key, just like SeekToFirst
		iter.seekInBlock(0, key)
		return
	}

	// block[found].smallest is > key.
	// Since found>0, we know block[found-1].smallest is <= key.
	// There are two cases.
	// 1) Everything in block[found-1] is strictly < key. In this case, we should go to the first
	//    element of block[found].
	// 2) Some element in block[found-1] is >= key. We should go to that element.
	iter.seekInBlock(found-1, key)
	if iter.err == io.EOF {
		// Case 1. Need to visit block[found].
		if found == iter.table.BlockCount() {
			// If found == iter.table.BlockCount() then input key is greater than ANY element of table.
			// There's nothing we can do. Valid() should return false as we seek to end of table.
			return
		}
		// Since block[found].smallest is > key. This is essentially a block[found].SeekToFirst.
		iter.seekInBlock(found, key)
	}
	// Case 2: No need to do anything. We already did the seek in block[found-1].
}

// SeekToFirst get the first entry(with smallest key) in the table
func (iter *TableIterator) SeekToFirst() {
	iter.err = nil
	iter.readBlockById(0)
	if iter.err != nil {
		return
	}
	iter.biter.SeekToFirst()
}

// SeekToFirst get the last entry(with biggest key) in the table
func (iter *TableIterator) SeekToLast() {
	iter.err = nil
	iter.readBlockById(iter.table.BlockCount() - 1)
	if iter.err != nil {
		return
	}
	iter.biter.SeekToLast()
}

// Next get next entry in the current block, if there is no any entry,
// it will move to the next block and get the first entry, until the last block
func (iter *TableIterator) Next() {
	iter.err = nil

	// no more block
	if iter.blockId >= iter.table.BlockCount() {
		iter.err = io.EOF
		return
	}

	// invalid or first read
	if iter.biter == nil {
		iter.readBlockById(iter.blockId)
		if iter.err != nil {
			return
		}

		iter.biter.SeekToFirst()
		iter.err = iter.biter.err
		return
	}

	// if reach the end of current block, get the next block
	iter.biter.Next()
	if !iter.biter.Valid() {
		iter.blockId++
		iter.biter = nil
		iter.Next()
	}
}

// Prev get prev entry in the current block, if there is already first entry,
// it will move to prev block and get the last entry, until the first block
func (iter *TableIterator) Prev() {
	iter.err = nil

	// no more block
	if iter.blockId < 0 {
		iter.err = io.EOF
		return
	}

	// invalid or first read, read from the last entry of first block
	if iter.biter == nil {
		iter.readBlockById(iter.blockId)
		if iter.err != nil {
			return
		}

		iter.biter.SeekToLast()
		iter.err = iter.biter.Error()
		return
	}

	// if reach the begin of current block, get the next block
	iter.biter.Prev()
	if !iter.biter.Valid() {
		iter.blockId--
		iter.biter = nil
		iter.Prev()
	}
}

// Returns the key with version.
func (iter *TableIterator) Key() []byte {
	return iter.biter.Key()
}

// Returns the value with meta
func (iter *TableIterator) Value() EValue {
	return iter.biter.Value()
}

// Returns the copy value with meta
func (iter *TableIterator) ValueCopy() (ret EValue) {
	dst := make([]byte, len(iter.biter.value))
	copy(dst, iter.biter.value)
	ret.Decode(dst)
	return
}

// Close closes the iterator (and it must be called).
func (iter *TableIterator) Close() error {
	if iter.biter != nil {
		iter.biter.Close()
	}

	return iter.table.DecrRef()
}

func (iter *TableIterator) Valid() bool {
	return iter.err == nil
}

func (iter *TableIterator) seekInBlock(bid int, key []byte) {
	iter.readBlockById(bid)
	if iter.err != nil {
		return
	}
	iter.biter.Seek(key)
	iter.err = iter.biter.Error()
}

func (iter *TableIterator) readBlockById(id int) {
	if iter.blockId != id || iter.biter == nil {
		iter.blockId = id
		block, err := iter.table.getBlockAt(id)
		if err != nil {
			iter.err = err
			return
		}

		iter.err = nil
		if iter.biter != nil {
			iter.biter.Close()
		}

		iter.biter = NewBlockIterator(block, int(iter.table.ID()), id)
	}
}

func (iter *TableIterator) reset() {
	iter.blockId = 0
	iter.err = nil
}

// TablesIterator uses to iterate over all the entries in tables than the iterator has
type TablesIterator struct {
	currIter *TableIterator
	iters    []*TableIterator
	tables   []*Table

	tableId int
}

func NewTablesIterator(tables []*Table) *TablesIterator {
	iters := make([]*TableIterator, len(tables))

	for i := 0; i < len(tables); i++ {
		tables[i].IncrRef()
	}

	return &TablesIterator{
		iters:   iters,
		tables:  tables,
		tableId: -2,
	}
}

// Seek find the first entry's key >= input key.
func (iter *TablesIterator) Seek(key []byte) {
	n := len(iter.tables)

	found := sort.Search(n, func(i int) bool {
		return compareKeys(iter.tables[i].biggest, key) >= 0
	})

	if found == n {
		// not found, change status to invalid
		iter.getTableIteratorById(-1)
		return
	}

	iter.getTableIteratorById(found)
	iter.currIter.Seek(key)
}

// SeekToFirst get the first entry(with smallest key) in the tables
func (iter *TablesIterator) SeekToFirst() {
	iter.getTableIteratorById(0)
	iter.currIter.SeekToFirst()
}

// SeekToLast get the first entry(with biggest key) in the table
func (iter *TablesIterator) SeekToLast() {
	iter.getTableIteratorById(len(iter.tables) - 1)
	iter.currIter.SeekToLast()
}

// Next get next entry in the current table, if there is no any entry,
// it will move to the next table and get the first entry, until the last table
func (iter *TablesIterator) Next() {
	// first read
	if iter.tableId == -2 {
		iter.SeekToFirst()
		return
	}

	iter.currIter.Next()
	// reach the end of current table or empty table, get next table
	if !iter.currIter.Valid() {
		iter.getTableIteratorById(iter.tableId + 1)
		// no more tables
		if !iter.Valid() {
			return
		}
		iter.Next()
	}
}

// Prev get prev entry in the current block, if there is no any entry,
// it will move to the prev table and get the first entry, until the first table
func (iter *TablesIterator) Prev() {
	// first read
	if iter.tableId == -2 {
		iter.getTableIteratorById(0)
		iter.currIter.Prev()
		return
	}

	iter.currIter.Prev()
	// reach the begin of current table or empty table, get prev table
	if !iter.currIter.Valid() {
		iter.getTableIteratorById(iter.tableId - 1)
		// no more tables
		if !iter.Valid() {
			return
		}

		iter.currIter.SeekToLast()
		if iter.currIter.Valid() {
			return
		}

		iter.Prev()
	}

}

func (iter *TablesIterator) Key() []byte {
	return iter.currIter.Key()
}

func (iter *TablesIterator) Value() EValue {
	return iter.currIter.Value()
}

func (iter *TablesIterator) Valid() bool {
	return iter.currIter != nil && iter.currIter.Valid()
}

func (iter *TablesIterator) Close() error {
	for _, table := range iter.tables {
		if err := table.DecrRef(); err != nil {
			return err
		}
	}

	for _, it := range iter.iters {
		if it == nil {
			continue
		}

		if err := it.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (iter *TablesIterator) getTableIteratorById(tableId int) {
	if tableId < 0 || tableId >= len(iter.tables) {
		iter.currIter = nil
		return
	}

	iter.tableId = tableId
	if iter.iters[tableId] == nil {
		iter.iters[tableId] = iter.tables[tableId].NewIterator()
	}

	iter.currIter = iter.iters[tableId]
}
