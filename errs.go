package golbat

import (
	"errors"
	"fmt"
	"log"
)

var (
	ErrTruncating    = errors.New("do truncate")
	ErrStopIteration = errors.New("stop iteration")
	ErrBadWAL        = errors.New(
		"WAL log is broken, need to be truncated that might cause data loss")
)

func Check(err error) {
	if err != nil {
		log.Fatalf("%+v", err)
	}
}

func Check2(_ interface{}, err error) {
	Check(err)
}

func AssertTrue(b bool) {
	if !b {
		log.Fatalf("%+v", fmt.Errorf("assert failed"))
	}
}

func AssertTruef(b bool, format string, args ...interface{}) {
	if !b {
		log.Fatalf("%+v", fmt.Errorf(format, args...))
	}
}

func Wrap(err error, msg string) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s err: %+v", msg, err)
}

func Wrapf(err error, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf(format+" error: %+v", append(args, err)...)
}