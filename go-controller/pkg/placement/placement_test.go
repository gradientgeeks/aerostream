package placement

import "testing"

func racksOf(bs []Broker) map[uint32]string {
	m := map[uint32]string{}
	for _, b := range bs {
		m[b.ID] = b.Rack
	}
	return m
}

func TestNoRackRoundRobinMatchesLegacy(t *testing.T) {
	bs := []Broker{{ID: 1}, {ID: 2}, {ID: 3}}
	got, _ := Assign(bs, 0, 4, 2)
	want := [][]uint32{{1, 2}, {2, 3}, {3, 1}, {1, 2}}
	for i := range want {
		if got[i][0] != want[i][0] || got[i][1] != want[i][1] {
			t.Fatalf("p%d got %v want %v", i, got[i], want[i])
		}
	}
}

func TestRackAwareSpreadsReplicas(t *testing.T) {
	bs := []Broker{{1, "a"}, {2, "a"}, {3, "b"}, {4, "b"}, {5, "c"}, {6, "c"}}
	got, err := Assign(bs, 0, 12, 3)
	if err != nil {
		t.Fatal(err)
	}
	racks := racksOf(bs)
	leaders := map[string]int{}
	for i, r := range got {
		if len(r) != 3 {
			t.Fatalf("p%d rf=%d", i, len(r))
		}
		if d := RackDiversity(r, racks); d != 3 {
			t.Fatalf("p%d replicas %v cover %d racks, want 3", i, r, d)
		}
		leaders[racks[r[0]]]++
	}
	for _, rk := range []string{"a", "b", "c"} {
		if leaders[rk] != 4 {
			t.Fatalf("leaders not balanced across racks: %v", leaders)
		}
	}
}

func TestRackAwareRFExceedsRacks(t *testing.T) {
	bs := []Broker{{1, "a"}, {2, "a"}, {3, "b"}, {4, "b"}}
	got, _ := Assign(bs, 0, 4, 3)
	racks := racksOf(bs)
	for i, r := range got {
		if len(r) != 3 || RackDiversity(r, racks) != 2 {
			t.Fatalf("p%d %v", i, r)
		}
		seen := map[uint32]bool{}
		for _, id := range r {
			if seen[id] {
				t.Fatalf("dup in %v", r)
			}
			seen[id] = true
		}
	}
}

func TestPartialRackFallsBack(t *testing.T) {
	if RackAware([]Broker{{1, "a"}, {2, ""}}) {
		t.Fatal("should not be rack aware")
	}
}

func TestRFCappedAndEmpty(t *testing.T) {
	got, _ := Assign([]Broker{{1, "a"}}, 0, 2, 3)
	if len(got[0]) != 1 {
		t.Fatalf("%v", got)
	}
	got, _ = Assign(nil, 0, 2, 3)
	if len(got) != 2 || got[0] != nil {
		t.Fatalf("%v", got)
	}
}

func TestValidateManual(t *testing.T) {
	known := map[uint32]bool{1: true, 2: true}
	if ValidateManual([]uint32{1, 2}, known) != nil {
		t.Fatal()
	}
	if ValidateManual([]uint32{1, 1}, known) == nil || ValidateManual([]uint32{9}, known) == nil {
		t.Fatal()
	}
}
