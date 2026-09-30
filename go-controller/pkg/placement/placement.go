// Package placement implements rack-aware replica placement (KIP-36).
// Distributes partition replicas across distinct racks; falls back to round-robin.
package placement

import (
	"fmt"
	"sort"
)

// Broker is the placement-relevant view of a broker.
type Broker struct {
	ID   uint32
	Rack string // empty = unknown
}

// RackAware reports whether every broker declares a rack.
func RackAware(brokers []Broker) bool {
	if len(brokers) == 0 {
		return false
	}
	for _, b := range brokers {
		if b.Rack == "" {
			return false
		}
	}
	return true
}

// alternated returns brokers ordered so that consecutive entries come from different racks
// (rack-alternated list as in Kafka's AdminUtils). Racks and brokers are sorted for determinism.
func alternated(brokers []Broker) []Broker {
	byRack := map[string][]Broker{}
	for _, b := range brokers {
		byRack[b.Rack] = append(byRack[b.Rack], b)
	}
	racks := make([]string, 0, len(byRack))
	for r := range byRack {
		racks = append(racks, r)
		rs := byRack[r]
		sort.Slice(rs, func(i, j int) bool { return rs[i].ID < rs[j].ID })
	}
	sort.Strings(racks)
	out := make([]Broker, 0, len(brokers))
	for i := 0; len(out) < len(brokers); i++ {
		for _, r := range racks {
			if i < len(byRack[r]) {
				out = append(out, byRack[r][i])
			}
		}
	}
	return out
}

func sortedByID(brokers []Broker) []Broker {
	out := append([]Broker(nil), brokers...)
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Assign computes replica lists for `count` partitions numbered startPartition..startPartition+count-1.
// The first replica of each list is the preferred leader. rf is capped at len(brokers).
func Assign(brokers []Broker, startPartition, count, rf int) ([][]uint32, error) {
	if count < 0 || rf < 0 {
		return nil, fmt.Errorf("invalid count/replication factor")
	}
	if len(brokers) == 0 {
		// No active brokers: partitions exist but are unassigned (legacy behaviour).
		return make([][]uint32, count), nil
	}
	if rf > len(brokers) {
		rf = len(brokers)
	}
	out := make([][]uint32, 0, count)
	if !RackAware(brokers) {
		ordered := sortedByID(brokers)
		n := len(ordered)
		for i := 0; i < count; i++ {
			p := startPartition + i
			reps := make([]uint32, 0, rf)
			for r := 0; r < rf; r++ {
				reps = append(reps, ordered[(p+r)%n].ID)
			}
			out = append(out, reps)
		}
		return out, nil
	}
	alt := alternated(brokers)
	n := len(alt)
	numRacks := map[string]bool{}
	for _, b := range alt {
		numRacks[b.Rack] = true
	}
	for i := 0; i < count; i++ {
		p := startPartition + i
		first := p % n
		reps := []uint32{alt[first].ID}
		seenRack := map[string]bool{alt[first].Rack: true}
		inReps := map[uint32]bool{alt[first].ID: true}
		// The follower scan offset shifts each full rotation so followers vary across partitions.
		shift := 1 + (p/n)%maxInt(n-1, 1)
		for step := 0; len(reps) < rf && step < 2*n*n; step++ {
			b := alt[(first+shift+step)%n]
			if inReps[b.ID] {
				continue
			}
			if len(seenRack) < len(numRacks) && seenRack[b.Rack] {
				continue // unused racks remain: skip brokers on an already-used rack
			}
			reps = append(reps, b.ID)
			inReps[b.ID] = true
			seenRack[b.Rack] = true
		}
		out = append(out, reps)
	}
	return out, nil
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// ValidateManual checks an explicit replica list: known brokers, no duplicates.
func ValidateManual(replicas []uint32, known map[uint32]bool) error {
	if len(replicas) == 0 {
		return fmt.Errorf("empty replica list")
	}
	seen := map[uint32]bool{}
	for _, id := range replicas {
		if !known[id] {
			return fmt.Errorf("unknown broker %d", id)
		}
		if seen[id] {
			return fmt.Errorf("duplicate broker %d in replica list", id)
		}
		seen[id] = true
	}
	return nil
}

// RackDiversity returns the number of distinct racks covered by the replica set.
func RackDiversity(replicas []uint32, racks map[uint32]string) int {
	s := map[string]bool{}
	for _, id := range replicas {
		s[racks[id]] = true
	}
	return len(s)
}
