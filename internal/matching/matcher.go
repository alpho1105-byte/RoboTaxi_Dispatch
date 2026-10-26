package matching

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"robotaxi-dispatch/internal/domain"
	"robotaxi-dispatch/internal/location"
	"robotaxi-dispatch/internal/metrics"
	"robotaxi-dispatch/internal/ride"
)

type Locations interface {
	Nearby(center domain.Point, k int, maxM, minBattery float64, exclude map[string]bool) []location.Candidate
	Reserve(avID, rideID string, ttl time.Duration) bool
	Release(avID, rideID string)
	SetBusy(avID string, busy bool)
}

// Rides 是 Ride Service 對 Matching Service 開放的 RPC。
type Rides interface {
	Assign(ctx context.Context, rideID, avID string) error
	Fail(ctx context.Context, rideID, reason string) error
}

// Dispatcher 把指令送到車輛的長連線上。
type Dispatcher interface {
	Send(avID string, cmd domain.Command) error
}

type Config struct {
	Candidates     int
	SearchRadiusM  float64
	MinBattery     float64
	OfferTimeout   time.Duration
	Deadline       time.Duration
	RetryEvery     time.Duration
	ReservationTTL time.Duration
}

// DefaultConfig 的配對期限是 55 秒：需求是「一分鐘內成功或失敗」，
// 留幾秒給逾時掃描的間隔與寫入資料庫。
func DefaultConfig() Config {
	return Config{
		Candidates: 5, SearchRadiusM: 5_000, MinBattery: 0.2,
		OfferTimeout: 5 * time.Second, Deadline: 55 * time.Second, RetryEvery: time.Second, ReservationTTL: 15 * time.Second,
	}
}

type Stats struct {
	Started, Resumed, Matched, Failed  int64
	Offers, Accepts, Rejects, Timeouts int64
	Conflicts, LateAccepts             int64
}

// Matcher 是 Matching Service 的核心：找出附近的候選車，一台一台送出派車詢問，
// 處理接受、拒絕與逾時。它本身不保存狀態，所有狀態都在 Store 裡，可以有任意多個 instance。
type Matcher struct {
	cfg     Config
	store   *Store
	loc     Locations
	rides   Rides
	gw      Dispatcher
	latency *metrics.Latency
	log     *slog.Logger

	started, resumed, matched, failed  atomic.Int64
	offers, accepts, rejects, timeouts atomic.Int64
	conflicts, lateAccepts             atomic.Int64
}

func NewMatcher(cfg Config, store *Store, loc Locations, rides Rides, gw Dispatcher, latency *metrics.Latency, log *slog.Logger) *Matcher {
	return &Matcher{cfg: cfg, store: store, loc: loc, rides: rides, gw: gw, latency: latency, log: log}
}

// Start 開始為一趟行程配對。queue 重送同一個請求時，狀態已經存在，直接交給原本的流程繼續。
func (m *Matcher) Start(ctx context.Context, req domain.RideRequest) {
	m.store.Update(req.RideID, func(st *State) *State {
		if st != nil {
			m.resumed.Add(1)
			return nil
		}
		m.started.Add(1)
		st = &State{
			RideID: req.RideID, Pickup: req.Pickup, Destination: req.Destination, RequestedAt: req.RequestedAt,
			Offered: make(map[string]bool), Tried: make(map[string]bool), Status: Searching,
			Deadline: req.RequestedAt.Add(m.cfg.Deadline),
		}
		m.advance(ctx, st, time.Now())
		return st
	})
}

// advance 把派車詢問送給下一台可用的候選車。呼叫端必須持有該行程的鎖。
func (m *Matcher) advance(ctx context.Context, st *State, now time.Time) {
	for refills := 0; ; {
		if !now.Before(st.Deadline) {
			m.fail(ctx, st, "no av accepted within the matching deadline")
			return
		}
		if st.Cursor >= len(st.Candidates) {
			cands := m.loc.Nearby(st.Pickup, m.cfg.Candidates, m.cfg.SearchRadiusM, m.cfg.MinBattery, st.Tried)
			if len(cands) == 0 || refills >= 3 {
				// 附近暫時沒有空車：稍後再找。尖峰時，正在完成行程的車會陸續空出來。
				st.OfferDeadline = now.Add(m.cfg.RetryEvery)
				return
			}
			refills++
			st.Candidates = st.Candidates[:0]
			for _, c := range cands {
				st.Candidates = append(st.Candidates, c.AVID)
			}
			st.Cursor = 0
			st.Rounds++
		}

		av := st.Candidates[st.Cursor]
		// 先軟性保留這台車，降低它同時被詢問接其他行程的機會。
		if !m.loc.Reserve(av, st.RideID, m.cfg.ReservationTTL) {
			st.Cursor++
			continue
		}
		cmd := domain.Command{Kind: domain.CommandDispatch, RideID: st.RideID, Pickup: st.Pickup, Destination: st.Destination}
		if err := m.gw.Send(av, cmd); err != nil {
			m.loc.Release(av, st.RideID)
			st.Cursor++
			continue
		}
		m.offers.Add(1)
		st.Tried[av] = true
		st.Offered[av] = true
		st.OfferDeadline = now.Add(m.cfg.OfferTimeout)
		return
	}
}

// OnDecision 處理車輛的回覆。可能由任何一個 worker 收到，甚至不是送出詢問的那一個。
func (m *Matcher) OnDecision(ctx context.Context, d domain.DispatchDecision) {
	now := time.Now()
	m.store.Update(d.RideID, func(st *State) *State {
		// 這台車的詢問已經撤回（行程已經有結果，例如另一台車先接受了），晚到的 ACCEPT 一律撤回。
		if st == nil || !st.Offered[d.AVID] {
			if d.Decision == domain.Accept {
				m.lateAccepts.Add(1)
				m.gw.Send(d.AVID, domain.Command{Kind: domain.CommandCancel, RideID: d.RideID})
			}
			return nil
		}
		delete(st.Offered, d.AVID)

		current := st.Cursor < len(st.Candidates) && st.Candidates[st.Cursor] == d.AVID
		next := func() {
			if current {
				st.Cursor++
				m.advance(ctx, st, now)
			}
		}
		if d.Decision == domain.Reject {
			m.rejects.Add(1)
			m.loc.Release(d.AVID, st.RideID)
			next()
			return st
		}

		if !now.Before(st.Deadline) {
			// 已經超過配對期限，乘客端應該已經收到失敗；不能再把車派過去。
			m.gw.Send(d.AVID, domain.Command{Kind: domain.CommandCancel, RideID: st.RideID})
			m.loc.Release(d.AVID, st.RideID)
			m.fail(ctx, st, "no av accepted within the matching deadline")
			return st
		}
		m.accepts.Add(1)
		switch err := m.rides.Assign(ctx, st.RideID, d.AVID); {
		case err == nil:
			st.Status, st.AssignedAV = Done, d.AVID
			m.loc.SetBusy(d.AVID, true)
			m.gw.Send(d.AVID, domain.Command{Kind: domain.CommandConfirm, RideID: st.RideID, Pickup: st.Pickup, Destination: st.Destination})
			m.withdraw(st)
			m.matched.Add(1)
			m.latency.Observe(now.Sub(st.RequestedAt))
		case errors.Is(err, ride.ErrAVBusy):
			// 這台車其實已經有進行中的行程（例如它同時接受了兩趟），資料庫的 unique index 擋下了這次指派。
			m.conflicts.Add(1)
			m.gw.Send(d.AVID, domain.Command{Kind: domain.CommandCancel, RideID: st.RideID})
			next()
		case errors.Is(err, ride.ErrNotAssignable):
			// 行程已經不在等待配對（例如乘客取消了）。
			st.Status = Failed
			m.gw.Send(d.AVID, domain.Command{Kind: domain.CommandCancel, RideID: st.RideID})
			m.loc.Release(d.AVID, st.RideID)
			m.withdraw(st)
		default:
			m.log.Warn("assign failed, trying next candidate", "ride", st.RideID, "av", d.AVID, "err", err)
			m.gw.Send(d.AVID, domain.Command{Kind: domain.CommandCancel, RideID: st.RideID})
			m.loc.Release(d.AVID, st.RideID)
			next()
		}
		return st
	})
}

// OnTimeouts 處理回覆期限已過的行程。每個 worker 都會定期呼叫；
// 在行程的鎖內重新確認期限，多個 worker 同時處理同一筆也不會重複推進。
func (m *Matcher) OnTimeouts(ctx context.Context) {
	now := time.Now()
	for _, id := range m.store.Expired(now) {
		m.store.Update(id, func(st *State) *State {
			if st == nil || st.Status != Searching || st.OfferDeadline.After(now) {
				return nil
			}
			if st.Cursor < len(st.Candidates) && st.Offered[st.Candidates[st.Cursor]] {
				// 目前的候選沒有在期限內回覆，換下一台。詢問不撤回：它晚到的 ACCEPT 仍然可以成立。
				m.timeouts.Add(1)
				st.Cursor++
			}
			m.advance(ctx, st, now)
			return st
		})
	}
}

func (m *Matcher) fail(ctx context.Context, st *State, reason string) {
	st.Status = Failed
	m.withdraw(st)
	if err := m.rides.Fail(ctx, st.RideID, reason); err != nil {
		m.log.Warn("mark ride failed", "ride", st.RideID, "err", err)
	}
	m.failed.Add(1)
}

// withdraw 撤回所有還在等回覆的派車詢問。
func (m *Matcher) withdraw(st *State) {
	for av := range st.Offered {
		m.gw.Send(av, domain.Command{Kind: domain.CommandCancel, RideID: st.RideID})
		m.loc.Release(av, st.RideID)
	}
	clear(st.Offered)
}

func (m *Matcher) Stats() Stats {
	return Stats{
		Started: m.started.Load(), Resumed: m.resumed.Load(), Matched: m.matched.Load(), Failed: m.failed.Load(),
		Offers: m.offers.Load(), Accepts: m.accepts.Load(), Rejects: m.rejects.Load(), Timeouts: m.timeouts.Load(),
		Conflicts: m.conflicts.Load(), LateAccepts: m.lateAccepts.Load(),
	}
}
