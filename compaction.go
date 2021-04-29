package golbat

import (
	"bytes"
	"fmt"
	"math"
	"sync"
)

type keyRange struct {
	left  []byte
	right []byte

	comparator Comparator
}

func getKeyRange(tables []*Table) keyRange {
	if len(tables) == 0 {
		return keyRange{}
	}

	comparator := tables[0].opt.comparator
	smallest := tables[0].Smallest()
	biggest := tables[1].Biggest()

	for _, table := range tables {
		if comparator(smallest, table.Smallest()) > 0 {
			smallest = table.Smallest()
		}

		if comparator(biggest, table.Biggest()) < 0 {
			biggest = table.Biggest()
		}
	}

	// pick all the versions of the smallest and the biggest key. Note that version zero would
	// be the rightmost key, considering versions are default sorted in descending order.
	return keyRange{
		left:       keyWithVersion(parseKey(smallest), math.MaxUint64),
		right:      keyWithVersion(parseKey(biggest), 0),
		comparator: comparator,
	}
}

func (r keyRange) Empty() bool {
	return len(r.left) == 0 && len(r.right) == 0
}

func (r keyRange) Equal(other keyRange) bool {
	return bytes.Equal(r.left, other.left) &&
		bytes.Equal(r.right, other.right)
}

func (r keyRange) IsOverlapsWith(other keyRange) bool {
	if r.Empty() {
		return true
	}

	if other.Empty() {
		return false
	}

	if r.comparator(r.left, other.right) < 0 {
		return false
	}

	if r.comparator(r.right, other.left) < 0 {
		return false
	}

	return true
}

func (r *keyRange) MergeFrom(other keyRange) {
	if other.Empty() {
		return
	}

	if r.Empty() {
		*r = other
	}

	if len(r.left) == 0 || r.comparator(r.left, other.left) > 0 {
		r.left = other.left
	}

	if len(r.right) == 0 || r.comparator(r.right, other.right) < 0 {
		r.right = other.right
	}
}

func (r keyRange) String() string {
	return fmt.Sprintf("[left=%x, right=%x]", r.left, r.right)
}

type levelCompactStatus struct {
	ranges  []keyRange
	delSize int64
}

func (lcs *levelCompactStatus) IsOverlapsWith(other keyRange) bool {
	for _, r := range lcs.ranges {
		if r.IsOverlapsWith(other) {
			return true
		}
	}

	return false
}

func (lcs *levelCompactStatus) Remove(del keyRange) bool {
	after := []keyRange{}
	found := false
	for _, r := range lcs.ranges {
		if !r.Equal(del) {
			after = append(after, r)
		} else {
			found = true
		}
	}

	lcs.ranges = after
	return found
}

func (lcs *levelCompactStatus) String() string {
	var b bytes.Buffer
	for _, r := range lcs.ranges {
		b.WriteString(r.String())
	}
	return b.String()
}

type compactStatus struct {
	sync.RWMutex
	levels []*levelCompactStatus
	tables map[uint64]struct{}
}

func (cs *compactStatus) IsOverlapsWith(level int, kr keyRange) bool {
	cs.RLock()
	defer cs.RUnlock()

	thisLevel := cs.levels[level]
	return thisLevel.IsOverlapsWith(kr)
}

func (cs *compactStatus) DeleteSize(l int) int64 {
	cs.RLock()
	defer cs.RUnlock()

	return cs.levels[l].delSize
}
