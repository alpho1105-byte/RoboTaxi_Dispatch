package location

import (
	"testing"
	"time"

	"robotaxi-dispatch/internal/domain"
	"robotaxi-dispatch/internal/geo"
)

var origin = domain.Point{Lat: 25.03, Lng: 121.56}

func TestNearbyReturnsClosestAvailableVehicles(t *testing.T) {
	x := NewIndex(0.005)
	now := time.Now()
	x.Update("near", geo.Offset(origin, 100, 0), 0.9, false, now)
	x.Update("mid", geo.Offset(origin, 0, 900), 0.9, false, now)
	x.Update("far", geo.Offset(origin, 3_000, 0), 0.9, false, now)
	x.Update("busy", geo.Offset(origin, 50, 0), 0.9, true, now)
	x.Update("flat", geo.Offset(origin, 60, 0), 0.1, false, now)
	x.Update("out", geo.Offset(origin, 9_000, 0), 0.9, false, now)

	got := x.Nearby(origin, 5, 5_000, 0.2, map[string]bool{"mid": true})
	if len(got) != 2 || got[0].AVID != "near" || got[1].AVID != "far" {
		t.Fatalf("nearby = %+v, want near then far (busy, low battery, excluded and out of range skipped)", got)
	}
	if d := got[0].DistanceM; d < 95 || d > 105 {
		t.Fatalf("distance = %.1f, want about 100m", d)
	}
}

func TestMovingVehicleLeavesItsOldCell(t *testing.T) {
	x := NewIndex(0.005)
	x.Update("a", origin, 0.9, false, time.Now())
	x.Update("a", geo.Offset(origin, 5_000, 0), 0.9, false, time.Now())

	if got := x.Nearby(origin, 1, 1_000, 0, nil); len(got) != 0 {
		t.Fatalf("vehicle still found at its old position: %+v", got)
	}
	if got := x.Nearby(geo.Offset(origin, 5_000, 0), 1, 1_000, 0, nil); len(got) != 1 {
		t.Fatal("vehicle not found at its new position")
	}
}

func TestReservationHidesVehicleUntilReleasedOrExpired(t *testing.T) {
	now := time.Now()
	x := NewIndex(0.005)
	x.now = func() time.Time { return now }
	x.Update("a", origin, 0.9, false, now)

	if !x.Reserve("a", "ride-1", 10*time.Second) {
		t.Fatal("first reservation should succeed")
	}
	if x.Reserve("a", "ride-2", 10*time.Second) {
		t.Fatal("a reserved vehicle must not be reserved for another ride")
	}
	if got := x.Nearby(origin, 1, 1_000, 0, nil); len(got) != 0 {
		t.Fatal("a reserved vehicle should not be offered to other rides")
	}

	x.Release("a", "ride-2") // 不是保留者，不能解除
	if x.Reserve("a", "ride-2", time.Second) {
		t.Fatal("release by a non-owner must be ignored")
	}

	now = now.Add(11 * time.Second)
	if !x.Reserve("a", "ride-2", time.Second) {
		t.Fatal("an expired reservation should not block new ones")
	}
	x.SetBusy("a", true)
	if x.Reserve("a", "ride-3", time.Second) {
		t.Fatal("a busy vehicle cannot be reserved")
	}
}
