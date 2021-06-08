package golbat

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golbat/internel"
	"github.com/pkg/errors"
)

// DB interface is useful when test
type DB interface {
	Put(options *WriteOptions, key, value []byte) error
	Delete(options *WriteOptions, key []byte) error
	Write(options *WriteOptions, batch *WriteBatch) error
	Get(options *ReadOptions, key []byte) (value []byte, err error)
	NewIterator(options *ReadOptions) (iterator Iterator, err error)
	GetSnapshot() *Snapshot
	ReleaseSnapshot(snapshot *Snapshot)
	GetExtend(options *ReadOptions, key []byte) (value *EValue, err error)
	GetOption() Options
	Close() error
}

// closers for those goroutines that need run backgroud when DB opened.
type closers struct {
	compact  *internel.Closer
	memtable *internel.Closer
	writes   *internel.Closer
	valueGC  *internel.Closer
}

type DBImpl struct {
	sync.RWMutex // Guards list of inmemory tables, not individual reads and writes.
	closers      closers

	dirLockGuard  *internel.DirLockGuard // Ensure that one and only one database is opened
	valueDirGuard *internel.DirLockGuard

	mem        *memTable
	imm        []*memTable
	nextMemFid int

	option    *Options
	snapshots *snapshotList
	manifest  *manifestFile
	ls        *levels
	vlog      *valueLog
	writeCh   chan *writeBatchInternel
	flushCh   chan *memTable

	blockWrites int32
	isClosed    uint32    // atomic
	closeOnce   sync.Once // for close db just once
}

const (
	lockFile        = "LOCK"
	writeChCapacity = 1000
)

func Open(options Options) (DB, error) {
	if err := checkOptions(&options); err != nil {
		return nil, err
	}

	if err := createDirs([]string{options.Dir, options.ValueLogDir}); err != nil {
		return nil, err
	}

	dirLockGuard, err := internel.AcquireDirLock(options.Dir, lockFile)
	if err != nil {
		return nil, Wrapf(err, "acquire dir(%q) lock failed, when open db.", options.Dir)
	}

	// if any error found during db open, release the lock.
	defer func() {
		if dirLockGuard != nil {
			_ = dirLockGuard.Release()
		}
	}()

	absDir, err := filepath.Abs(options.Dir)
	if err != nil {
		return nil, err
	}
	absValueLogDir, err := filepath.Abs(options.ValueLogDir)
	if err != nil {
		return nil, err
	}

	var valueDirLockGuard *internel.DirLockGuard
	// value log file isn't store with sst tables.
	// so lock value log dir too.
	if absDir != absValueLogDir {
		valueDirLockGuard, err = internel.AcquireDirLock(options.ValueLogDir, lockFile)
		if err != nil {
			return nil, err
		}

		// if any error found during db open, release the lock.
		defer func() {
			if valueDirLockGuard != nil {
				_ = valueDirLockGuard.Release()
			}
		}()
	}

	options.Logger.Infof("Opening manifest file: %q", filepath.Join(options.Dir, ManifestFilename))
	manifestFile, manifest, err := OpenManifestFile(options.Dir)
	if err != nil {
		return nil, err
	}
	// if any error found during db open, close the manifest file.
	defer func() {
		if manifestFile != nil {
			_ = manifestFile.Close()
		}
	}()

	db := &DBImpl{
		imm:           make([]*memTable, 0, options.NumMemtables),
		flushCh:       make(chan *memTable, options.NumMemtables),
		writeCh:       make(chan *writeBatchInternel, writeChCapacity),
		option:        &options,
		manifest:      manifestFile,
		dirLockGuard:  dirLockGuard,
		valueDirGuard: valueDirLockGuard,
		snapshots:     newSnapshotList(),
	}

	// Cleanup all the goroutines started by badger in case of an error.
	defer func() {
		if err != nil {
			options.Logger.Errorf("while opening db, got an err: %v. Cleaning up...", err)
			db.cleanup()
			db = nil
		}
	}()

	// load old memtables if exists, just read only
	db.option.Logger.Infof("Opening old memtables")
	if err := db.openMemTables(); err != nil {
		return nil, Wrapf(err, "while opening memtables")
	}

	// create new empty memtable for write
	db.option.Logger.Infof("Creating new memtables")
	if db.mem, err = db.newMemTable(); err != nil {
		return nil, Wrapf(err, "cannot create memtable")
	}

	// init value log
	db.option.Logger.Infof("Opening value log")
	db.vlog, err = OpenValueLog(*db.option)
	if err != nil {
		return db, Wrapf(err, "During value log open")
	}

	// init levels
	db.option.Logger.Infof("Initing tables")
	if db.ls, err = NewLevels(db.option, &manifest, db.vlog, db.manifest, db.snapshots); err != nil {
		return db, err
	}

	// init maxVersion,
	maxVersion := db.initMaxVersion()
	db.ls.updateMaxVersion(maxVersion)
	db.option.Logger.Infof("Set maxVersion to %d", maxVersion)

	// start compact goroutines
	db.closers.compact = internel.NewCloser(1)
	db.option.Logger.Infof("Starting compact(%d) goroutines", db.option.NumCompactors)
	db.ls.StartCompact(db.closers.compact)

	// start a goroutine for flush old memtables
	db.closers.memtable = internel.NewCloser(1)
	db.option.Logger.Infof("Starting memtable flush")
	go func() {
		_ = db.flushMemTable(db.closers.memtable)
	}()
	for _, mt := range db.imm {
		db.flushCh <- mt
	}

	// start write goroutines
	db.closers.writes = internel.NewCloser(1)
	db.option.Logger.Infof("Starting write goroutine")
	go db.doWrites(db.closers.writes)

	// start value gc goroutine
	db.closers.valueGC = internel.NewCloser(1)
	db.option.Logger.Infof("Starting value log gc goroutine")
	go db.vlog.waitGC(db.closers.valueGC)

	valueDirLockGuard = nil
	dirLockGuard = nil
	manifestFile = nil
	return db, nil
}

func (db *DBImpl) initMaxVersion() uint64 {
	var maxVersion uint64
	set := func(v uint64) {
		if v > maxVersion {
			maxVersion = v
		}
	}

	db.Lock()

	// get version from current memtable first
	set(db.mem.maxVersion)

	// then get version from old immutable memtables
	for _, mt := range db.imm {
		set(mt.maxVersion)
	}

	db.Unlock()

	// final, get version from levels
	for _, t := range db.ls.GetTableMeta() {
		set(t.MaxVersion)
	}

	return maxVersion
}

func (db *DBImpl) getMaxVersion() uint64 {
	return db.ls.MaxVersion()
}

// Close closes a DB. Calling Close(db) multiple times would still only close the DB once.
func Close(db DB) {
	if err := db.Close(); err != nil {
		Wrap(err, "close db failed.")
	}
}

// RunValueLogGC triggers a value log garbage collection.
//
// It picks value log files to perform GC based on statistics that are collected
// during compactions.  If no such statistics are available, then log files are
// picked in random order. The process stops as soon as the first log file is
// encountered which does not result in garbage collection.
//
// When a log file is picked, it is first sampled. If the sample shows that we
// can discard at least discardRatio space of that file, it would be rewritten.
//
// If a call to RunValueLogGC results in no rewrites, then an ErrNoRewrite is
// thrown indicating that the call resulted in no file rewrites.
//
// We recommend setting discardRatio to 0.5, thus indicating that a file be
// rewritten if half the space can be discarded.  This results in a lifetime
// value log write amplification of 2 (1 from original write + 0.5 rewrite +
// 0.25 + 0.125 + ... = 2). Setting it to higher value would result in fewer
// space reclaims, while setting it to a lower value would result in more space
// reclaims at the cost of increased activity on the LSM tree. discardRatio
// must be in the range (0.0, 1.0), both endpoints excluded, otherwise an
// error is returned.
//
// Only one GC is allowed at a time. If another value log GC is running, or DB
// has been closed, this would return an ErrRejected.
//
// Note: Every time GC is run, it would produce a spike of activity on the LSM
// tree.
func RunValueLogGC(db DB, discardRatio float64) error {
	if discardRatio >= 1.0 || discardRatio <= 0.0 {
		return errors.Errorf("discardRatio set to a invalid value(%.2f), it should in (0.0, 1.0).",
			discardRatio)
	}

	dbImpl, _ := db.(*DBImpl)

	return dbImpl.vlog.runGC(discardRatio, db)
}

// Put write the key and value into the db with options
func (db *DBImpl) Put(options *WriteOptions, key, value []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}

	wb := NewWriteBatch(db)
	wb.Put(key, value)

	return db.Write(options, wb)
}

// Delete delete value of the key from db with options
func (db *DBImpl) Delete(options *WriteOptions, key []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}

	wb := NewWriteBatch(db)
	wb.Delete(key)

	return db.Write(options, wb)
}

// Write writes the record in batch into db with write options.
// ATTENTION: write synchronous, if there is no room for write in memtable, it will be blocked.
func (db *DBImpl) Write(options *WriteOptions, batch *WriteBatch) error {
	if err := batch.Validate(); err != nil {
		return err
	}

	batch.sync = options.Sync
	wb, err := db.doWrite(batch)
	if err != nil {
		return err
	}

	return wb.Wait()
}

func (db *DBImpl) doWrite(batch *WriteBatch) (*writeBatchInternel, error) {
	if atomic.LoadInt32(&db.blockWrites) == 1 {
		return nil, ErrDBClosed
	}

	wb := batchPool.Get().(*writeBatchInternel)
	count := uint64(len(batch.entries))
	version := db.ls.updateMaxVersion(count) + 1
	wb.Reset()
	wb.setVersion(version)
	wb.Fill(batch)
	wb.wg.Add(1)
	wb.IncrRef()

	db.writeCh <- wb

	return wb, nil
}

// Get read newest value of the key if not any snapshot set in options,
// otherwise it will read the version (or below it if there is no such version) in snapshot.
func (db *DBImpl) Get(options *ReadOptions, key []byte) (value []byte, err error) {
	vs, err := db.GetExtend(options, key)
	if err != nil {
		return nil, err
	}

	return vs.Value, nil
}

// NewIterator returns a iterator for the db.
// Iterators have the nuance of being a snapshot of the writes for the transaction at the time
// iterator was created. If writes are performed after an iterator is created, then that iterator
// will not be able to see those writes. Only writes performed before an iterator was created can be
// viewed.
// CAUTION: when done with iteration, a iterator should be closed.
func (db *DBImpl) NewIterator(options *ReadOptions) (iterator Iterator, err error) {
	if db.IsClosed() {
		return nil, ErrDBClosed
	}

	tables, decr := db.getMemTables()
	defer decr()

	var iters []Iterator
	for _, table := range tables {
		iters = append(iters, &memTableIterator{table.skl.Iterator()})
	}

	iters = db.ls.appendIterators(iters, options)
	dbIter := NewDBIterator(db, options,
		NewTablesMergeIterator(db.option, iters, false), db.getMaxVersion())

	return dbIter, nil
}

// GetSnapshot return a snapshot with current max version
func (db *DBImpl) GetSnapshot() *Snapshot {
	return db.snapshots.New(db.getMaxVersion())
}

// ReleaseSnapshot delete the snapshot from db
// CAUTION: when snapshot not be used again, ReleaseSnapshot should be called.
func (db *DBImpl) ReleaseSnapshot(snapshot *Snapshot) {
	db.snapshots.Delete(snapshot)
}

// GetExtend Get read newest value (with meta) of the key if not any snapshot set in options,
// otherwise it will read the version (or below it if there is no such version) in snapshot.
func (db *DBImpl) GetExtend(options *ReadOptions, key []byte) (value *EValue, err error) {
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}

	res, err := db.get(options, key)
	if err != nil {
		return nil, err
	}

	if res.Value == nil && res.Meta == 0 {
		return nil, ErrKeyNotFound
	}

	if res.Meta&Delete == Delete {
		return nil, ErrKeyNotFound
	}

	if res.Meta&ValPtr == ValPtr {
		var vp valPtr
		vp.Decode(res.Value)

		// TODO: need to improve read
		entry, err := db.vlog.Read(options, vp)
		if err != nil {
			db.option.Logger.Errorf("Unable to read from vlog: Key: %v, Version : %v, meta: %v"+
				" Error: %v", key, res.version, res.Meta, err)
			return nil, err
		}

		res.Value = safeCopy(res.Value, entry.value)
	}

	return &res, nil
}

func (db *DBImpl) get(options *ReadOptions, key []byte) (value EValue, err error) {
	if db.IsClosed() {
		return EValue{}, ErrDBClosed
	}

	tables, decr := db.getMemTables()
	defer decr()

	var version = db.getMaxVersion()
	if options.Snapshot != nil {
		version = options.Snapshot.version
	}

	key = KeyWithVersion(key, version)

	for _, table := range tables {
		v := table.skl.Get(key)
		if v == nil {
			continue
		}

		// make a deep copy
		var buf []byte
		buf = safeCopy(buf, v)

		var ev EValue
		ev.Decode(buf)

		return ev, nil
	}

	return db.ls.GetValue(key, 0)
}

func (db *DBImpl) getMemTables() ([]*memTable, func()) {
	db.RLock()
	defer db.RUnlock()

	res := []*memTable{db.mem}
	db.mem.IncrRef()

	// get immutable memtables reverse (get newest first)
	for i := len(db.imm) - 1; i >= 0; i-- {
		res = append(res, db.imm[i])
		db.imm[i].IncrRef()
	}

	return res, func() {
		for _, table := range res {
			table.DecrRef()
		}
	}
}

// GetOption return the options used in the db
func (db *DBImpl) GetOption() Options {
	return *db.option
}

// GetTables return the meta of tables in the db
func (db *DBImpl) GetTables() []TableMeta {
	return db.ls.GetTableMeta()
}

// Close closes a DB. It's crucial to call it to ensure all the pending updates make their way to
// disk. Calling DB.Close() multiple times would still only close the DB once.
func (db *DBImpl) Close() error {
	var err error
	db.closeOnce.Do(func() {
		err = db.doClose()
	})

	return err
}

func (db *DBImpl) IsClosed() bool {
	return atomic.LoadUint32(&db.isClosed) == 1
}

func (db *DBImpl) doClose() error {
	var err error
	db.option.Logger.Debugf("Closing database.")
	db.option.Logger.Infof("Lifetime L0 stalled for: %s\n",
		time.Duration(atomic.LoadInt64(&db.ls.l0stallMs)))

	atomic.StoreInt32(&db.blockWrites, 1)

	// stop value gc first.
	db.option.Logger.Infof("Stoping value log gc goroutines")
	db.closers.valueGC.SignalAndWait()

	// stop writes next.
	db.option.Logger.Infof("Stoping write goroutines")
	db.closers.writes.SignalAndWait()

	// close write chan, doesn't accept any writes.
	close(db.writeCh)

	db.option.Logger.Infof("Closing value log")
	if verr := db.vlog.Close(); verr != nil {
		err = Wrap(err, "DB.close")
	}

	// make sure all mem in flush chan will flush to disk. should push current mem into imm
	if db.mem != nil {
		if db.mem.skl.Empty() {
			db.mem.DecrRef()
		} else {
			db.option.Logger.Debugf("Flushing memtable.")

			pushMem := func() bool {
				db.Lock()
				defer db.Unlock()

				select {
				case db.flushCh <- db.mem:
					db.imm = append(db.imm, db.mem)
					db.mem = nil
					db.option.Logger.Debugf("pushed the memtable into flush chan")

					return true
				default:
					db.option.Logger.Debugf("pushed the memtable into flush chan failed, retry...")
					// do nothing
				}
				return false
			}

			for {
				if pushMem() {
					break
				}

				// push failed, so wait for a while, then try again.
				time.Sleep(10 * time.Millisecond)
			}
		}
	}

	// stop mem flush and wait it finished
	db.option.Logger.Infof("Stoping memtable flush goroutine")
	close(db.flushCh)
	db.closers.memtable.SignalAndWait()

	// stop compact and wait it finished
	db.option.Logger.Infof("Stoping compact goroutines")
	db.closers.compact.SignalAndWait()

	db.option.Logger.Infof("Closing tables")
	db.option.Logger.Infof(levelsToString(db.ls.GetLevelMeta()))
	if lErr := db.ls.Close(); err == nil {
		err = Wrap(lErr, "DB.Close")
	}

	// mark the status of db is closed
	atomic.StoreUint32(&db.isClosed, 1)

	// release dir locks
	db.option.Logger.Infof("Releasing directories lock guard")
	if db.dirLockGuard != nil {
		if guardErr := db.dirLockGuard.Release(); err == nil {
			err = Wrap(guardErr, "DB.Close")
		}
	}
	if db.valueDirGuard != nil {
		if guardErr := db.valueDirGuard.Release(); err == nil {
			err = Wrap(guardErr, "DB.Close")
		}
	}

	// close manifest file
	db.option.Logger.Infof("Closing manifest file")
	if manifestErr := db.manifest.Close(); err == nil {
		err = Wrap(manifestErr, "DB.Close")
	}

	// Fsync directories to ensure that lock file, and any other removed files whose directory
	// we haven't specifically fsynced, are guaranteed to have their directory entry removal
	// persisted to disk.
	db.option.Logger.Infof("Syncing database's directories")
	if syncErr := syncDir(db.option.Dir); err == nil {
		err = Wrap(syncErr, "DB.Close")
	}
	if syncErr := syncDir(db.option.ValueLogDir); err == nil {
		err = Wrap(syncErr, "DB.Close")
	}

	return err
}

func (db *DBImpl) cleanup() {
	// stop memtable flush
	db.stopMemTableFlush()

	// stop compact
	db.stopCompact()

	// stop value log gc
	if db.closers.valueGC != nil {
		db.closers.valueGC.Signal()
	}

	// stop write
	if db.closers.writes != nil {
		db.closers.writes.Signal()
	}
}

func (db *DBImpl) stopMemTableFlush() {
	if db.closers.memtable != nil {
		// close flush chan
		close(db.flushCh)
		// wait for running memtable flush finish, keep data consistency
		db.closers.memtable.SignalAndWait()
	}
}

func (db *DBImpl) stopCompact() {
	if db.closers.compact != nil {
		// wait for running compact finish, keep data consistency
		db.closers.compact.SignalAndWait()
	}
}

func (db *DBImpl) doWrites(c *internel.Closer) {
	defer c.Done()

	pendingCh := make(chan struct{}, 1)
	write := func(batches []*writeBatchInternel) {
		if err := db.writeBatches(batches); err != nil {
			db.option.Logger.Errorf("write batch err: %+v", err)
		}

		<-pendingCh
	}

	batches := make([]*writeBatchInternel, 0, 10)
	for {
		var batch *writeBatchInternel
		select {
		case batch = <-db.writeCh:
		case <-c.HasBeenClosed():
			goto closedCase
		}

		for {
			batches = append(batches, batch)

			// write should be blocked if there are so many writes
			if len(batches) >= 3*writeChCapacity {
				pendingCh <- struct{}{}
				goto writeCase
			}

			select {
			case batch = <-db.writeCh:
			case pendingCh <- struct{}{}:
				goto writeCase
			case <-c.HasBeenClosed():
				goto closedCase
			}
		}

	closedCase:
		for {
			select {
			case batch = <-db.writeCh:
				batches = append(batches, batch)
			default:
				pendingCh <- struct{}{}
				write(batches)
				return
			}
		}

	writeCase:
		go write(batches)
		batches = make([]*writeBatchInternel, 0, 10)
	}
}

func (db *DBImpl) writeBatches(batches []*writeBatchInternel) error {
	if len(batches) == 0 {
		return nil
	}

	done := func(err error) {
		for _, batch := range batches {
			batch.err = err
			batch.wg.Done()
		}
	}

	db.option.Logger.Debugf("writing batches. Write to value log")
	for _, batch := range batches {
		if err := db.vlog.Write(batch); err != nil {
			done(err)
			return err
		}
	}

	db.option.Logger.Debugf("Writing to memtable")
	count := 0
	for _, batch := range batches {
		if len(batch.entries) == 0 {
			continue
		}

		count += len(batch.entries)
		var i uint64
		var err error
		for err = db.ensureRoomForWrite(); err == ErrNoRoom; err = db.ensureRoomForWrite() {
			i++

			if i%100 == 0 {
				db.option.Logger.Debugf("Making room for writes.")
			}

			// We need to poll a bit because both ensureRoomForWrite and the flusher need access to s.imm.
			// When flushChan is full and you are blocked there, and the flusher is trying to update s.imm,
			// you will get a deadlock.
			time.Sleep(10 * time.Millisecond)
		}

		if err != nil {
			done(err)
			return err
		}

		if err = db.writeToLSM(batch); err != nil {
			done(err)
			return err
		}
	}

	done(nil)
	db.option.Logger.Debugf("%d entries written.", count)

	return nil
}

func (db *DBImpl) ensureRoomForWrite() error {
	db.Lock()
	defer db.Unlock()

	// current memtable is not full
	if !db.mem.IsFull() {
		return nil
	}

	var err error
	// current memtable is full, move it to imm and flush if have to, then create a new one
	select {
	case db.flushCh <- db.mem:
		db.option.Logger.Debugf("Flushing memtable, memtable.size=%d, flush chan: %d",
			db.mem.skl.Size(), len(db.flushCh))

		db.imm = append(db.imm, db.mem)
		db.mem, err = db.newMemTable()
		if err != nil {
			return Wrapf(err, "cannot create new mem table")
		}

		return nil
	default:
		return ErrNoRoom
	}
}

func (db *DBImpl) writeToLSM(batch *writeBatchInternel) error {
	for i, entry := range batch.entries {
		var vs EValue
		if entry.checkWithThreshold(uint32(db.option.ValueThreshold)) {
			vs = EValue{
				Value: entry.value,
				Meta:  entry.rtype &^ ValPtr,
			}
		} else {
			vs = EValue{
				Value: batch.ptrs[i].Encode(),
				Meta:  entry.rtype | ValPtr,
			}
		}

		if err := db.mem.Put(entry.key, vs); err != nil {
			return Wrapf(err, "while writing to memTable")
		}
	}

	if batch.sync {
		return db.mem.SyncWAL()
	}

	return nil
}

func (db *DBImpl) openMemTables() error {
	files, err := ioutil.ReadDir(db.option.Dir)
	if err != nil {
		return Wrapf(err, "Unable to open mem dir: %q", db.option.Dir)
	}

	var fids []int
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), MemFileExt) {
			continue
		}
		fsz := len(file.Name())
		fid, err := strconv.ParseInt(file.Name()[:fsz-len(MemFileExt)], 10, 64)
		if err != nil {
			return Wrapf(err, "Unable to parse log id. file: %s", file.Name())
		}

		fids = append(fids, int(fid))
	}

	// Sort by fid in ascending order (aka. by table files' created time).
	sort.Slice(fids, func(i, j int) bool {
		return fids[i] < fids[j]
	})
	for _, fid := range fids {
		flags := os.O_RDWR
		mt, err := OpenMemTable(fid, flags, *db.option)
		if err != nil {
			return Wrapf(err, "while opening fid: %d", fid)
		}
		// If this memtable is empty we don't need to add it. This is a
		// memtable that was completely truncated.
		if mt.skl.Empty() {
			mt.DecrRef()
			continue
		}
		// These should no longer be written to. So, make them part of the imm.
		db.imm = append(db.imm, mt)
	}
	if len(fids) != 0 {
		db.nextMemFid = fids[len(fids)-1]
	}

	db.nextMemFid++
	return nil
}

func (db *DBImpl) flushMemTable(c *internel.Closer) error {
	defer c.Done()

	for mt := range db.flushCh {
		if mt == nil {
			continue
		}

		// If an error occurs during flushing, continue flushing until it is successful.
		for {
			err := db.doFlush(mt)
			if err == nil {
				// remove the flushed memtable.
				db.Lock()
				db.imm = db.imm[1:]
				mt.DecrRef()
				db.Unlock()

				break
			}

			// Encountered error. Retry indefinitely.
			db.option.Logger.Errorf("Failure while flushing memtable to disk: %v. Retrying...\n",
				err)
			time.Sleep(time.Second)
		}
	}

	return nil
}

func (db *DBImpl) doFlush(mt *memTable) error {
	if mt.skl.Empty() {
		return nil
	}

	builder := buildL0Table(mt, *db.option)

	if builder.Empty() {
		builder.Finish()
		return nil
	}

	fileId := db.ls.nextTableId()
	table, err := CreateTable(NewTableFileName(fileId, db.option.Dir), builder)
	if err != nil {
		return Wrapf(err, "while creating table")
	}

	err = db.ls.AddLevel0Table(table)
	_ = table.DecrRef()

	return err
}

func (db *DBImpl) newMemTable() (*memTable, error) {
	mem, err := NewMemTable(db.nextMemFid, *db.option)
	if err != nil {
		return nil, err
	}

	db.nextMemFid++
	return mem, nil
}

// buildL0Table builds a new table from the memtable.
func buildL0Table(mt *memTable, opts Options) *TableBuilder {
	iter := mt.skl.Iterator()
	defer iter.Close()
	b := NewTableBuilder(opts)
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		var vs EValue
		vs.Decode(iter.Value())

		var vp valPtr
		if vs.Meta&ValPtr > 0 {
			vp.Decode(vs.Value)
		}

		b.Add(iter.Key(), vs, vp.len)
	}

	return b
}

func checkOptions(opt *Options) error {
	// It's okay to have zero compactors which will disable all compactions but
	// we cannot have just one compactor otherwise we will end up with all data
	// on level 2.
	if opt.NumCompactors == 1 {
		return errors.New("Cannot have 1 compactor. Need at least 2")
	}

	if opt.Dir == "" || opt.ValueLogDir == "" {
		return errors.New("Unset directory for DB or value log.")
	}

	opt.maxBatchSize = (15 * opt.MemTableSize) / 100
	opt.maxBatchCount = opt.maxBatchSize / internel.MaxNodeSize

	// This is the maximum value, vlogThreshold can have if dynamic thresholding is enabled.
	maxVt := int(math.Min(maxValueThreshold, float64(opt.maxBatchSize)))

	// We are limiting opt.ValueThreshold to maxValueThreshold for now.
	if opt.ValueThreshold > maxVt {
		return errors.Errorf("Invalid ValueThreshold, must be less or equal to %d",
			maxValueThreshold)
	}

	// If ValueThreshold is greater than opt.maxBatchSize, we won't be able to push any data using
	// the transaction APIs. Transaction batches entries into batches of size opt.maxBatchSize.
	if opt.ValueThreshold > opt.maxBatchSize {
		return errors.Errorf("Valuethreshold %d greater than max batch size of %d. Either "+
			"reduce opt.ValueThreshold or increase opt.MaxTableSize.",
			opt.ValueThreshold, opt.maxBatchSize)
	}
	// ValueLogFileSize should be stricly LESS than 2<<30 otherwise we will
	// overflow the uint32 when we mmap it in OpenMemtable.
	if !(opt.ValueLogFileSize < 2<<30 && opt.ValueLogFileSize >= 1<<20) {
		return ErrValueLogSize
	}

	return nil
}

func createDirs(dirs []string) error {
	for _, dir := range dirs {
		dirExists, err := exists(dir)
		if err != nil {
			return Wrapf(err, "Invalid dir: %q", dir)
		}

		if !dirExists {
			err = os.MkdirAll(dir, 0700)
			if err != nil {
				return Wrapf(err, "Error Creating dir: %q", dir)
			}
		}
	}

	return nil
}

func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func levelsToString(ls []LevelMeta) string {
	const MB = float64(1 << 20)
	var b strings.Builder
	b.WriteRune('\n')

	base := func(b bool) string {
		if b {
			return "B"
		}
		return " "
	}

	makeReadable := func(n int64) float64 {
		return float64(n) / MB
	}

	for _, l := range ls {
		b.WriteString(fmt.Sprintf(
			"Level %d [%s]: NumTables: %02d. Size: %.2f MiB of %.2f MiB. Score: %.2f->%.2f"+
				" Target FileSize: %.2f MiB\n",
			l.Id, base(l.IsBaseLevel), l.NumTables,
			makeReadable(l.Size), makeReadable(l.TargetSize), l.Score, l.Adjusted,
			makeReadable(l.MaxFileSize)))
	}

	b.WriteString("Level Done\n")
	return b.String()
}

type EValue struct {
	Meta  byte
	Value []byte

	version uint64 // not decode or encode
}

func (ev *EValue) Decode(b []byte) {
	// CAUTION: not copy, just ref
	ev.Meta = b[0]
	ev.Value = b[1:]
}

func (ev *EValue) Encode() []byte {
	sz := 1 + len(ev.Value)
	b := make([]byte, sz)
	b[0] = ev.Meta
	copy(b[1:sz], ev.Value)

	return b
}

func (ev *EValue) EncodeTo(buf *bytes.Buffer) {
	buf.WriteByte(ev.Meta)
	buf.Write(ev.Value)
}

func (ev *EValue) EncodedSize() uint32 {
	return uint32(1 + len(ev.Value))
}
