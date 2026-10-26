package fleet

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"robotaxi-dispatch/internal/domain"
	"robotaxi-dispatch/internal/gateway"
	"robotaxi-dispatch/internal/geo"
)

// Behavior 控制模擬車輛的行為，包含各種不可靠的情況。
type Behavior struct {
	// RejectRate：以其他理由（維修、清潔）拒絕派車的機率。
	RejectRate float64
	// NoReplyRate：收到派車詢問後完全不回覆的機率（例如網路中斷）。
	NoReplyRate float64
	// LateReplyRate：沒有回覆的車中，有多少會在逾時之後才回覆 ACCEPT。
	LateReplyRate float64
	LateDelay     time.Duration
	// MisreportRate：有多少比例的車因為軟體 bug，載客時仍回報自己是空車。
	// 位置索引會因此把它們當成可派的車，必須靠 Ride DB 的 unique index 擋下重複指派。
	MisreportRate float64
	// BusyAcceptRate：已經有行程卻還是接受新派車的機率，模擬車輛軟體的 bug。
	BusyAcceptRate float64
	LowBattery     float64
	ThinkTime      time.Duration
	SpeedMps       float64
	Tick           time.Duration
	// ConfirmTimeout：接受派車後多久沒收到確認就放棄，恢復成空車。
	ConfirmTimeout time.Duration
}

func DefaultBehavior() Behavior {
	return Behavior{
		RejectRate: 0.03, NoReplyRate: 0.03, LateReplyRate: 0.5, LateDelay: 3 * time.Second,
		MisreportRate: 0.05, BusyAcceptRate: 0.5,
		LowBattery: 0.15, ThinkTime: 20 * time.Millisecond, SpeedMps: 300, Tick: 200 * time.Millisecond,
		ConfirmTimeout: 5 * time.Second,
	}
}

type leg int

const (
	idle leg = iota
	toPickup
	toDestination
)

type vehicle struct {
	id        string
	pos       domain.Point
	battery   float64
	session   *gateway.Session
	rng       *rand.Rand
	misreport bool

	serving     string
	leg         leg
	target      domain.Point
	destination domain.Point
	pending     map[string]time.Time
}

// Fleet 模擬一整個車隊：每台車一個 goroutine，透過 Gateway 的連線回報位置、回覆派車、回報行程進度。
// 它同時記錄每趟行程實際是哪台車去載的，讓模擬器能從車輛端驗證沒有重複指派。
type Fleet struct {
	gw  *gateway.Gateway
	beh Behavior

	vehicles []*vehicle

	mu        sync.Mutex
	servedBy  map[string][]string
	overlaps  atomic.Int64
	completed atomic.Int64
}

func New(gw *gateway.Gateway, beh Behavior) *Fleet {
	return &Fleet{gw: gw, beh: beh, servedBy: make(map[string][]string)}
}

// Spawn 在 center 半徑 radiusM 公尺內均勻放置 n 台車。
func (f *Fleet) Spawn(n int, center domain.Point, radiusM float64, seed uint64) {
	rng := rand.New(rand.NewPCG(seed, 11))
	for i := range n {
		r := radiusM * math.Sqrt(rng.Float64())
		theta := 2 * math.Pi * rng.Float64()
		v := &vehicle{
			id:      fmt.Sprintf("av-%05d", len(f.vehicles)+i),
			pos:     geo.Offset(center, r*math.Cos(theta), r*math.Sin(theta)),
			battery: 0.05 + 0.95*rng.Float64(),
			rng:     rand.New(rand.NewPCG(seed, uint64(i)+100)),
			pending: make(map[string]time.Time),
		}
		v.misreport = v.rng.Float64() < f.beh.MisreportRate
		v.session = f.gw.Connect(v.id)
		v.session.ReportLocation(v.pos, v.battery, false)
		f.vehicles = append(f.vehicles, v)
	}
}

func (f *Fleet) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, v := range f.vehicles {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f.drive(ctx, v)
		}()
	}
	wg.Wait()
}

func (f *Fleet) drive(ctx context.Context, v *vehicle) {
	// 錯開每台車的回報時間，避免所有更新同時湧入。
	time.Sleep(time.Duration(v.rng.Int64N(int64(f.beh.Tick))))
	ticker := time.NewTicker(f.beh.Tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case cmd := <-v.session.Commands():
			f.handle(ctx, v, cmd)
		case <-ticker.C:
			f.step(ctx, v)
			v.session.ReportLocation(v.pos, v.battery, v.busy() && !v.misreport)
		}
	}
}

func (v *vehicle) busy() bool {
	return v.serving != "" || len(v.pending) > 0
}

func (f *Fleet) handle(ctx context.Context, v *vehicle, cmd domain.Command) {
	switch cmd.Kind {
	case domain.CommandDispatch:
		f.decide(ctx, v, cmd.RideID)
	case domain.CommandConfirm:
		delete(v.pending, cmd.RideID)
		if v.serving != "" && v.serving != cmd.RideID {
			// 同時被確認接兩趟：服務端的保證失效了，模擬器會把它當成錯誤回報。
			f.overlaps.Add(1)
			return
		}
		v.serving, v.leg, v.target, v.destination = cmd.RideID, toPickup, cmd.Pickup, cmd.Destination
		f.mu.Lock()
		f.servedBy[cmd.RideID] = append(f.servedBy[cmd.RideID], v.id)
		f.mu.Unlock()
	case domain.CommandCancel:
		delete(v.pending, cmd.RideID)
	}
}

func (f *Fleet) decide(ctx context.Context, v *vehicle, rideID string) {
	b := f.beh
	accept := func() {
		v.pending[rideID] = time.Now()
		time.Sleep(b.ThinkTime)
		v.session.Decide(ctx, rideID, domain.Accept, "")
	}
	switch r := v.rng.Float64(); {
	case v.busy():
		if v.rng.Float64() < b.BusyAcceptRate {
			accept()
		} else {
			v.session.Decide(ctx, rideID, domain.Reject, "BUSY")
		}
	case v.battery < b.LowBattery:
		v.session.Decide(ctx, rideID, domain.Reject, "LOW_BATTERY")
	case r < b.RejectRate:
		v.session.Decide(ctx, rideID, domain.Reject, "MAINTENANCE")
	case r < b.RejectRate+b.NoReplyRate:
		if v.rng.Float64() < b.LateReplyRate {
			go func() {
				time.Sleep(b.LateDelay)
				v.session.Decide(ctx, rideID, domain.Accept, "late")
			}()
		}
	default:
		accept()
	}
}

func (f *Fleet) step(ctx context.Context, v *vehicle) {
	now := time.Now()
	for ride, at := range v.pending {
		if now.Sub(at) > f.beh.ConfirmTimeout {
			delete(v.pending, ride)
		}
	}
	if v.serving == "" {
		v.battery = math.Min(1, v.battery+0.002)
		return
	}
	pos, arrived := geo.MoveToward(v.pos, v.target, f.beh.SpeedMps*f.beh.Tick.Seconds())
	v.battery = math.Max(0, v.battery-geo.DistanceM(v.pos, pos)/1000*0.002)
	v.pos = pos
	if !arrived {
		return
	}
	if v.leg == toPickup {
		v.session.Trip(ctx, v.serving, domain.TripPickedUp)
		v.leg, v.target = toDestination, v.destination
		return
	}
	v.session.Trip(ctx, v.serving, domain.TripCompleted)
	v.serving, v.leg = "", idle
	f.completed.Add(1)
}

// ServedBy 回傳每趟行程實際被哪些車確認接走。
func (f *Fleet) ServedBy() map[string][]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string][]string, len(f.servedBy))
	for k, v := range f.servedBy {
		out[k] = append([]string(nil), v...)
	}
	return out
}

// Overlaps 回傳車輛在載客途中又被確認接另一趟的次數，正常情況必須是 0。
func (f *Fleet) Overlaps() int64 {
	return f.overlaps.Load()
}

func (f *Fleet) Completed() int64 {
	return f.completed.Load()
}
