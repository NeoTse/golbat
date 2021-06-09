# Globat

## Introduce

> Globat is an embeddable, persistent and fast key-value database like [leveldb](https://github.com/google/leveldb), written in pure Go and optimized for SSD with [WiscKey](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf).

## Getting Started

### Installing

To start using Golbat, Please install Go 1.12 or above, and also needs go modules. Run the following command to retrieve the library.

```sh
 go get github.com/neotse/golbat
```

Note: Golbat does not directly use CGO but it relies on [https://github.com/DataDog/zstd](https://github.com/DataDog/zstd) for compression and it requires gcc/cgo. If you wish to use golbat without gcc/cgo, you can run:

```sh
CGO_ENABLED=0 go get github.com/neotse/golbat
```

which will download golbat without the support for ZSTD compression algorithm.

#### Installing Command Line Tool

Download and extract the latest release from [https://github.com/NeoTse/golbat/releases](https://github.com/NeoTse/golbat/releases) and then run the following commands:

```sh
cd golbat-<version>/cmd
go install
```

This will install the command line utility into your `$GOBIN` path.

### Opening A Database

The following example shows how to open a database:

```go
import "github.com/neotse/golbat"

dir := "/tmp/golbat"
option := golbat.DefaultOption(dir)
db, err := golbat.Open(option)
if err != nil {
    return err
}
```

If there is no any database in the dir, golbat will create a new one. If there is already has a database, but not opened by other process or goroutine, golbat will open it. Otherwise, an error will be returned.

### Closing A Database

The following example shows how to close a database:

```go
err := golbat.Close(db)
if err != nil {
    ...
}
```

or

```go
err := db.Close()
if err != nil {
    ...
}
```

If `golbat.Close()` or `db.Close()` called more than once, multiple times would still only close the DB once.

### Reads And Writes

The database provides Put, Delete, and Get methods to modify/query the database. For example, the following code moves the value stored under key1 to key2.

```go
value, err := db.Get(&golbat.DefaultReadOptions, key1)
if err != nil {
    return err
}

err = db.Put(&golbat.DefaultWriteOptions, key2, value)
if err != nil {
    return err
}

err = db.Delete((&golbat.DefaultWriteOptions, key1)
if err != nil {
    return err
}
```

### Atomic Updates

Note that if the process dies after the Put of key2 but before the delete of key1, the same value may be left stored under multiple keys. Such problems can be avoided by using the WriteBatch class to atomically apply a set of updates:

```go
value, err := db.Get(&golbat.DefaultReadOptions, key1)
if err != nil {
    return err
}

batch := NewWriteBatch(db)
batch.Delete(key1)
batch.Put(key2, value)
err = db.Write((&golbat.DefaultWriteOptions, batch)
if err != nil {
    return err
}
```

Apart from its atomicity benefits, WriteBatch may also be used to speed up bulk updates by placing lots of individual mutations into the same batch.

### Synchronous Writes

By default, each write to leveldb is asynchronous: it returns after pushing the write from the process into the operating system. The transfer from operating system memory to the underlying persistent storage happens asynchronously. The sync flag can be turned on for a particular write to make the write operation not return until the data being written has been pushed all the way to persistent storage. (On Posix systems, this is implemented by calling either fsync(...) or fdatasync(...) or msync(..., MS_SYNC) before the write operation returns.)

```go
var wopt golbat.WriteOptions
wopt.Sync = true
err := db.Put(&wopt, key1, value1)
```

`WriteBatch` provides an alternative to asynchronous writes. Multiple updates may be placed in the same WriteBatch and applied together using a synchronous write (i.e., `WriteOptions.Sync` is set to true). The extra cost of the synchronous write will be amortized across all of the writes in the batch.

### Concurrency

A database may only be opened by one process at a time. The golbat implementation acquires a lock from the operating system to prevent misuse. Within a single process, the same `golbat.DB` object may be safely shared by multiple concurrent goroutines. I.e., different goroutines may write into or fetch iterators or call Get on the same database without any external synchronization. However other objects (like `Iterator` and `WriteBatch`) may require external synchronization. If two goroutines share such an object, they must protect access to it using their own locking protocol.

### Iteration

The following example show how to print all valid key,value pairs in a database.

```go
iter, err := db.NewItertor(&golbat.DefaultReadOptions)
if err != nil {
    return err
}
defer iter.Close()

for iter.SeekToFirst(); iter.Vaild(); iter.Next() {
    fmt.Printf("key: %s, Value: %s", string(iter.Key()), string(iter.Value().Value))
}
```

Note: If an iterator no longer in use, the `Close` method must be called.

Sometimes we want to get all the key and value pairs, regardless of which version or whether they are deleted.

```go
var ropt golbat.ReadOptions
ropt.AllVersion = true
iter, err := db.NewItertor(&ropt)
if err != nil {
    return err
}
defer iter.Close()

for iter.SeekToFirst(); iter.Vaild(); iter.Next() {
    fmt.Printf("key: %s, Value: %v", string(iter.Key()), string(iter.Value()))
}
```

If you don't want the deleted key, value pairs, you should filter it by youself. The `iter.Value()` return a `EValue` that contains the status(Value/Delete) of value.

### Snapshots

Snapshots provide consistent read-only views over the entire state of the key-value store. ReadOptions::snapshot may be non-NULL to indicate that a read should operate on a particular version of the DB state. If ReadOptions::snapshot is NULL, the read will operate on an implicit snapshot of the current state.

Snapshots are created by the `GetSnapshot()` method:

```go
var ropt golbat.ReadOptions
ropt.snaphot = db.GetSnapshot()
defer db.ReleaseSnaphot(ropt.snaphot)

... apply some updates to db ...
iter, err := db.NewItertor(&ropt)
if err != nil {
    return err
}
defer iter.Close()
... read using iter to view the state when the snapshot was created ...
```

Note: that when a snapshot is no longer needed, it should be released using the `ReleaseSnapshot` method.

### Value Log GC

The Value Log store the key, value pairs when the size of value is greater than `Options.ValueThreshold`. So some key, value pairs not only stored in LSM, but also stored in value log. When we deleted a key from database(aka. LSM), if the key also stored in value log, then we should also deleted it at some point in the future. We can do this job by call `RunValueLogGC` method.

```go
discardRatio := 0.5
err := RunValueLogGC(db, discardRatio)
if err != nil {
    return err
}
```

`discardRatio` is the percentage of key and value pairs that have been deleted in the LSM. We recommend setting discardRatio to 0.5, thus indicating that a file be rewritten if half the space can be discarded.  This results in a lifetime value log write amplification of 2 (1 from original write + 0.5 rewrite + 0.25 + 0.125 + ... = 2). Setting it to higher value would result in fewer space reclaims, while setting it to a lower value would result in more space reclaims at the cost of increased activity on the LSM tree. discardRatio must be in the range (0.0, 1.0), both endpoints excluded, otherwise an error is returned.

### Performance

Performance can be tuned by changing the default values of the types defined in `golat.Options`

### ValueThreshold

The ValueThreshold is the most important setting. If sets a higher ValueThreshold so values would be collocated with the LSM tree, with value log largely acting as a write-ahead log only. These options would reduce the disk usage of value log, and make golbat act more like a typical LSM tree. When there are many small values (<=1KB) and read frequently, sets a higher ValueThreshold (bigger than the most values) can make a good performance. Otherwise, write frequently or more big values (>= 64KB), sets a lower ValueThreshold more better. The default ValueThreshold in `golat.DefaultOptions` is 1MB.

### Block size

Golbat groups adjacent keys together into the same block and such a block is the unit of transfer to and from persistent storage. The default block size is approximately 4096 uncompressed bytes. Applications that mostly do bulk scans over the contents of the database may wish to increase this size. Applications that do a lot of point reads of small values may wish to switch to a smaller block size if performance measurements indicate an improvement. There isn't much benefit in using blocks smaller than one kilobyte, or larger than a few megabytes. Also note that compression will be more effective with larger block sizes.

### Compression

Each block is individually compressed before being written to persistent storage. Compression is on by default (snappy). the `ZSTD` compression recommended but it need `CGO`.

### Checksums

Golbat associates checksums with all data it stores in the file system. There are two separate controls provided over how aggressively these checksums are verified:

`ReadOptions.VerifyCheckSum` may be set to true to force checksum verification of all data that is read from the file system on behalf of a particular read (include value log). By default, no such verification is done.

`Options.VerifyTableCheckSum` may be set to true before opening a database to make the database implementation raise an error as soon as it detects an internal corruption. Depending on which portion of the database has been corrupted, the error may be raised when the database is opened, or later by another database operation. By default, VerifyTableCheckSum is off so that the database can be used even if parts of its persistent storage have been corrupted.
