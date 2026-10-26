package ride

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"robotaxi-dispatch/internal/domain"
	"robotaxi-dispatch/internal/fare"
)

type publisher struct{ published []domain.RideRequest }

func (p *publisher) Publish(r domain.RideRequest) uint64 {
	p.published = append(p.published, r)
	return uint64(len(p.published))
}

func newService(t *testing.T) (*Service, *Store, *publisher) {
	t.Helper()
	store, err := Open(filepath.Join(t.TempDir(), "rides.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	pub := &publisher{}
	return NewService(store, fare.DefaultEstimator(), pub), store, pub
}

var (
	pickup      = domain.Point{Lat: 25.03, Lng: 121.56}
	destination = domain.Point{Lat: 25.06, Lng: 121.53}
)

func request(t *testing.T, s *Service, rider string) domain.Ride {
	t.Helper()
	ctx := context.Background()
	f, err := s.EstimateFare(ctx, rider, pickup, destination)
	if err != nil {
		t.Fatal(err)
	}
	r, _, err := s.RequestRide(ctx, rider, f.ID)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestRequestRideIsIdempotentPerFare(t *testing.T) {
	ctx := context.Background()
	s, _, pub := newService(t)
	f, _ := s.EstimateFare(ctx, "alice", pickup, destination)
	if f.AmountCents < fare.DefaultEstimator().MinimumCents || f.DistanceM == 0 {
		t.Fatalf("fare = %+v", f)
	}

	first, created1, _ := s.RequestRide(ctx, "alice", f.ID)
	second, created2, _ := s.RequestRide(ctx, "alice", f.ID)
	if !created1 || created2 || first.ID != second.ID || len(pub.published) != 1 {
		t.Fatalf("created=%v,%v ids=%s,%s published=%d", created1, created2, first.ID, second.ID, len(pub.published))
	}
	if _, _, err := s.RequestRide(ctx, "mallory", f.ID); !errors.Is(err, ErrFareNotFound) {
		t.Fatalf("another rider using the fare: %v", err)
	}
}

func TestExpiredFareCannotBeUsed(t *testing.T) {
	ctx := context.Background()
	s, _, _ := newService(t)
	f, _ := s.EstimateFare(ctx, "alice", pickup, destination)
	s.now = func() time.Time { return time.Now().Add(time.Hour) }
	if _, _, err := s.RequestRide(ctx, "alice", f.ID); !errors.Is(err, ErrFareExpired) {
		t.Fatalf("err = %v", err)
	}
}

func TestUniqueIndexAllowsOneActiveRidePerVehicle(t *testing.T) {
	ctx := context.Background()
	s, store, _ := newService(t)
	r1, r2 := request(t, s, "alice"), request(t, s, "bob")

	if err := s.Assign(ctx, r1.ID, "av-1"); err != nil {
		t.Fatal(err)
	}
	if err := s.Assign(ctx, r2.ID, "av-1"); !errors.Is(err, ErrAVBusy) {
		t.Fatalf("second active ride for av-1: %v, want ErrAVBusy", err)
	}
	if err := s.Assign(ctx, r1.ID, "av-2"); !errors.Is(err, ErrNotAssignable) {
		t.Fatalf("reassigning an assigned ride: %v", err)
	}

	// 行程結束後，這台車可以接下一趟。
	s.TripEvent(ctx, "av-1", r1.ID, domain.TripPickedUp)
	s.TripEvent(ctx, "av-1", r1.ID, domain.TripCompleted)
	if err := s.Assign(ctx, r2.ID, "av-1"); err != nil {
		t.Fatalf("assign after completion: %v", err)
	}
	done, _, _ := store.Ride(ctx, r1.ID)
	if done.Status != domain.RideCompleted || done.CompletedAt.IsZero() || done.ResolvedAt.IsZero() {
		t.Fatalf("ride 1 = %+v", done)
	}
}

func TestTripEventsOnlyFromTheAssignedVehicle(t *testing.T) {
	ctx := context.Background()
	s, store, _ := newService(t)
	r := request(t, s, "alice")
	s.Assign(ctx, r.ID, "av-1")
	s.TripEvent(ctx, "av-2", r.ID, domain.TripPickedUp)
	if got, _, _ := store.Ride(ctx, r.ID); got.Status != domain.RideAssigned {
		t.Fatalf("status = %s, another vehicle must not advance the trip", got.Status)
	}
}

func TestCancelOnlyWhileWaiting(t *testing.T) {
	ctx := context.Background()
	s, _, _ := newService(t)
	r := request(t, s, "alice")
	if err := s.Cancel(ctx, "alice", r.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.Cancel(ctx, "alice", r.ID); !errors.Is(err, ErrNotCancelable) {
		t.Fatalf("second cancel: %v", err)
	}
	if err := s.Assign(ctx, r.ID, "av-1"); !errors.Is(err, ErrNotAssignable) {
		t.Fatalf("assigning a canceled ride: %v", err)
	}
}
