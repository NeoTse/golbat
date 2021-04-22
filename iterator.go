package golbat

type Iterator interface {
	Seek(key []byte)
	SeekToFirst()
	SeekToLast()
	Next()
	Prev()
	Key() []byte
	Value() EValue
	Valid() bool
	Close() error
}
