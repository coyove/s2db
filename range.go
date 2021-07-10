package main

import (
	"github.com/cockroachdb/pebble"
)

type RangeLimit struct {
	Value     string
	Inclusive bool
}

func (z *DB) rangeLex(name string, start, end RangeLimit) ([]Pair, error) {
	s := z.db.NewSnapshot()
	defer s.Close()

	startNameKey := makeZSetNameKey(name, start.Value)
	if !start.Inclusive {
		startNameKey = append(startNameKey, 1)
	}

	endNameKey := makeZSetNameKey(name, end.Value)
	if end.Inclusive {
		endNameKey = append(endNameKey, 1)
	}

	i := s.NewIter(&pebble.IterOptions{
		LowerBound: startNameKey,
		UpperBound: endNameKey,
	})

	var pairs []Pair

	if !i.First() {
		return pairs, nil
	}
	for {
		_, key := parseZSetNameKey(i.Key())
		score := bytesToFloat(i.Value())
		pairs = append(pairs, Pair{key, score})
		if !i.Next() {
			break
		}
	}
	return pairs, nil
}

func (z *DB) rangeLexIndex(name string, start, end int) ([]Pair, error) {
	s := z.db.NewSnapshot()
	defer s.Close()

	i := s.NewIter(&pebble.IterOptions{
		LowerBound: makeZSetNameKey(name, ""),
		UpperBound: makeZSetNameKey(name, "\xff"),
	})

	var pairs []Pair
	if !i.First() {
		return pairs, nil
	}

	for idx := 0; ; idx++ {
		if idx >= start && idx < end {
			_, key := parseZSetNameKey(i.Key())
			score := bytesToFloat(i.Value())
			pairs = append(pairs, Pair{key, score})
		}
		if !i.Next() {
			break
		}
	}
	return pairs, nil
}

func (z *DB) rangeScore(name string, start, end RangeLimit) ([]Pair, error) {
	s := z.db.NewSnapshot()
	defer s.Close()

	buf := floatToBytes(atof(start.Value))
	if !start.Inclusive {
		buf[len(buf)-1]++
	}
	startScoreKey := makeZSetScoreKey2(name, "", buf)

	buf = floatToBytes(atof(end.Value))
	if end.Inclusive {
		buf[len(buf)-1]++
	}
	endScoreKey := makeZSetScoreKey2(name, "", buf)

	i := s.NewIter(&pebble.IterOptions{
		LowerBound: startScoreKey,
		UpperBound: endScoreKey,
	})

	var pairs []Pair

	if !i.First() {
		return pairs, nil
	}

	for {
		_, key, score := parseZSetScoreKey(i.Key())
		pairs = append(pairs, Pair{key, score})
		if !i.Next() {
			break
		}
	}
	return pairs, nil
}

func (z *DB) rangeScoreIndex(name string, start, end int) ([]Pair, error) {
	s := z.db.NewSnapshot()
	defer s.Close()

	i := s.NewIter(&pebble.IterOptions{
		LowerBound: makeZSetScoreKey(name, "", 0),
		UpperBound: makeZSetScoreKey2(name, "", []byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255}),
	})

	var pairs []Pair

	if !i.First() {
		return pairs, nil
	}

	for idx := 0; ; idx++ {
		if idx >= start && idx < end {
			_, key, score := parseZSetScoreKey(i.Key())
			pairs = append(pairs, Pair{key, score})
		}
		if !i.Next() {
			break
		}
	}
	return pairs, nil
}
