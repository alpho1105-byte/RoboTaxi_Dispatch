package matching

import (
	"context"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"robotaxi-dispatch/internal/domain"
	"robotaxi-dispatch/internal/fare"
	"robotaxi-dispatch/internal/geo"
	"robotaxi-dispatch/internal/location"
	"robotaxi-dispatch/internal/ride"
)

// recorder 是假的 AV Gateway，記錄送給每台車的指令。
type recorder struct {
	mu   sync.Mutex
	sent map[string][]domain.Command
}

func (r *recorder) Send(av string, cmd domain.Command) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sent[av] = append(r.sent[av], cmd)
	return nil
}

func (r *recorder) kinds(av string) []domain.CommandKind {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []domain.CommandKind
	for _, c := range r.sent[av] {
		out = append(out, c.Kind)
	}
	return out
}

type fixture struct {
	t       *testing.T
	m       *Matcher
	store   *Store
	loc     *location.Index
	rides   *ride.Service
	rideDB  *ride.Store
	gw      *recorder
	pickup  domain.Point
	rideIDs []string
}

func newFixture(t *testing.T, cfg Config, vehicles ...string) *fixture {
	t.Helper()
	db, err := ride.Open(filepath.Join(t.TempDir(), "rides.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	f := &fixture{t: t, store: NewStore(), loc: location.NewIndex(0.005), rideDB: db, gw: &recorder{sent: map[string][]domain.Command{}},
		pickup: domain.Point{Lat: 25.03, Lng: 121.56}}
	f.rides = ride.NewService(db, fare.DefaultEstimator(), noopPublisher{})
	// 車輛依序排在越來越遠的地方：v1 最近。
	for i, id := range vehicles {
		f.loc.Update(id, geo.Offset(f.pickup, float64(100*(i+1)), 0), 0.9, false, time.Now())
	}
	f.m = NewMatcher(cfg, f.store, f.loc, f.rides, f.gw, nil, slog.New(slog.DiscardHandler))
	return f
}

type noopPublisher struct{}

func (noopPublisher) Publish(domain.RideRequest) uint64 { return 0 }

func (f *fixture) newRide() domain.RideRequest {
	f.t.Helper()
	ctx := context.Background()
	fr, _ := f.rides.EstimateFare(ctx, "rider", f.pickup, geo.Offset(f.pickup, 3_000, 0))
	r, _, err := f.rides.RequestRide(ctx, "rider", fr.ID)
	if err != nil {
		f.t.Fatal(err)
	}
	return domain.RideRequest{RideID: r.ID, Pickup: r.Pickup, Destination: r.Destination, RequestedAt: r.RequestedAt}
}

func (f *fixture) decide(rideID, av string, d domain.Decision) {
	f.m.OnDecision(context.Background(), domain.DispatchDecision{RideID: rideID, AVID: av, Decision: d})
}

func (f *fixture) ride(id string) domain.Ride {
	r, _, _ := f.rideDB.Ride(context.Background(), id)
	return r
}

func testConfig() Config {
	return Config{Candidates: 3, SearchRadiusM: 2_000, MinBattery: 0.2, OfferTimeout: 50 * time.Millisecond,
		Deadline: time.Minute, RetryEvery: 50 * time.Millisecond, ReservationTTL: time.Minute}
}

func TestRejectMovesToNextCandidateAndAcceptConfirms(t *testing.T) {
	f := newFixture(t, testConfig(), "v1", "v2", "v3")
	req := f.newRide()
	f.m.Start(context.Background(), req)

	if got := f.gw.kinds("v1"); len(got) != 1 || got[0] != domain.CommandDispatch {
		t.Fatalf("v1 commands = %v, the closest vehicle should be asked first", got)
	}
	f.decide(req.RideID, "v1", domain.Reject)
	f.decide(req.RideID, "v2", domain.Accept)

	st, _ := f.store.Get(req.RideID)
	if st.Status != Done || st.AssignedAV != "v2" {
		t.Fatalf("state = %+v", st)
	}
	if r := f.ride(req.RideID); r.Status != domain.RideAssigned || r.AVID != "v2" {
		t.Fatalf("ride = %+v", r)
	}
	if got := f.gw.kinds("v2"); len(got) != 2 || got[1] != domain.CommandConfirm {
		t.Fatalf("v2 commands = %v, want dispatch then confirm", got)
	}
}

// 第一台逾時後改派第二台，結果兩台的 ACCEPT 先後到達：只有先到的那台會被指派，另一台被撤回。
func TestTimeoutThenBothAcceptOnlyOneWins(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, testConfig(), "v1", "v2", "v3")
	req := f.newRide()
	f.m.Start(ctx, req)

	time.Sleep(60 * time.Millisecond)
	f.m.OnTimeouts(ctx)
	if got := f.gw.kinds("v2"); len(got) != 1 {
		t.Fatalf("v2 should be asked after v1 timed out, got %v", got)
	}

	var wg sync.WaitGroup
	for _, av := range []string{"v1", "v2"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f.decide(req.RideID, av, domain.Accept)
		}()
	}
	wg.Wait()

	st, _ := f.store.Get(req.RideID)
	winner, loser := st.AssignedAV, "v1"
	if winner == "v1" {
		loser = "v2"
	}
	if f.ride(req.RideID).AVID != winner {
		t.Fatal("ride DB and matching state disagree on the winner")
	}
	loserCmds := f.gw.kinds(loser)
	if loserCmds[len(loserCmds)-1] != domain.CommandCancel {
		t.Fatalf("%s commands = %v, the losing vehicle must be told to cancel", loser, loserCmds)
	}
}

// 車輛同時接受兩趟行程時，資料庫的 unique index 只讓其中一趟指派成功，另一趟改派下一台。
func TestVehicleAcceptingTwoRidesIsCaughtByTheDatabase(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig()
	cfg.ReservationTTL = 0 // 關掉軟性保留，讓兩趟行程同時詢問同一台車
	f := newFixture(t, cfg, "v1", "v2")
	r1, r2 := f.newRide(), f.newRide()
	f.m.Start(ctx, r1)
	f.m.Start(ctx, r2)

	f.decide(r1.RideID, "v1", domain.Accept)
	f.decide(r2.RideID, "v1", domain.Accept)

	if f.m.Stats().Conflicts != 1 {
		t.Fatalf("conflicts = %d, want 1", f.m.Stats().Conflicts)
	}
	if got := f.gw.kinds("v2"); len(got) != 1 || got[0] != domain.CommandDispatch {
		t.Fatalf("v2 commands = %v, ride 2 should move on to v2", got)
	}
	f.decide(r2.RideID, "v2", domain.Accept)
	if f.ride(r1.RideID).AVID != "v1" || f.ride(r2.RideID).AVID != "v2" {
		t.Fatalf("rides = %+v / %+v", f.ride(r1.RideID), f.ride(r2.RideID))
	}
}

func TestRedeliveredRequestDoesNotDispatchAgain(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, testConfig(), "v1")
	req := f.newRide()
	f.m.Start(ctx, req)
	f.m.Start(ctx, req)
	if got := f.gw.kinds("v1"); len(got) != 1 || f.m.Stats().Resumed != 1 {
		t.Fatalf("v1 commands = %v, resumed = %d", got, f.m.Stats().Resumed)
	}
}

func TestDeadlineFailsTheRide(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig()
	cfg.Deadline = 100 * time.Millisecond
	f := newFixture(t, cfg) // 附近沒有任何車
	req := f.newRide()
	f.m.Start(ctx, req)

	time.Sleep(150 * time.Millisecond)
	f.m.OnTimeouts(ctx)
	if r := f.ride(req.RideID); r.Status != domain.RideNoAVAvailable || r.ResolvedAt.IsZero() {
		t.Fatalf("ride = %+v", r)
	}

	// 期限過後才到的 ACCEPT 不能讓行程起死回生。
	f.loc.Update("late", f.pickup, 0.9, false, time.Now())
	f.decide(req.RideID, "late", domain.Accept)
	if r := f.ride(req.RideID); r.Status != domain.RideNoAVAvailable {
		t.Fatalf("ride = %+v", r)
	}
}
