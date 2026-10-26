package matching

import (
	"maps"
	"slices"
	"sync"
	"time"

	"robotaxi-dispatch/internal/domain"
)

type Status string

const (
	Searching Status = "SEARCHING"
	Done      Status = "DONE"
	Failed    Status = "FAILED"
)

// State 是一趟行程的配對狀態，對應 Redis 上的 match:ride:<ride_id>。
// 狀態放在共享的 Store 而不是 worker 的記憶體裡：送出派車詢問的 worker
// 和收到車輛回覆的 worker 可以是不同的 instance。
type State struct {
	RideID      string
	Pickup      domain.Point
	Destination domain.Point
	RequestedAt time.Time
	Candidates  []string
	Cursor      int
	// Offered 是已送出派車詢問、還沒回覆的車。逾時的車仍留在這裡：它晚到的 ACCEPT 依然可能成立。
	Offered map[string]bool
	// Tried 是這趟行程已經詢問過的車，重新找候選時排除。
	Tried      map[string]bool
	Status     Status
	AssignedAV string
	// OfferDeadline 是目前候選的回覆期限；附近沒有空車時，則是下一次重新尋找的時間。
	OfferDeadline time.Time
	// Deadline 是整個配對的期限，超過就宣告失敗。
	Deadline time.Time
	Rounds   int
}

func (s *State) clone() *State {
	c := *s
	c.Candidates = slices.Clone(s.Candidates)
	c.Offered = maps.Clone(s.Offered)
	c.Tried = maps.Clone(s.Tried)
	return &c
}

// Store 模擬 Redis 上的 matching state 與 per-ride lock。
// 同一趟行程的所有修改都在它的鎖內進行，同一時間只有一個 worker 能推進它的狀態。
type Store struct {
	mu        sync.Mutex
	states    map[string]*State
	locks     map[string]*sync.Mutex
	deadlines map[string]time.Time
}

func NewStore() *Store {
	return &Store{states: make(map[string]*State), locks: make(map[string]*sync.Mutex), deadlines: make(map[string]time.Time)}
}

// Update 取得 rideID 的鎖，把目前的狀態（不存在時為 nil）交給 fn；fn 回傳非 nil 的狀態會被寫回。
func (s *Store) Update(rideID string, fn func(st *State) *State) {
	s.mu.Lock()
	l, ok := s.locks[rideID]
	if !ok {
		l = &sync.Mutex{}
		s.locks[rideID] = l
	}
	s.mu.Unlock()

	l.Lock()
	defer l.Unlock()
	// 拿到鎖之後才讀狀態，看到的一定是上一個持鎖者寫回的版本。
	s.mu.Lock()
	st := s.states[rideID]
	s.mu.Unlock()
	if st != nil {
		st = st.clone()
	}
	next := fn(st)
	if next == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.states[rideID] = next
	if next.Status == Searching {
		s.deadlines[rideID] = next.OfferDeadline
	} else {
		delete(s.deadlines, rideID)
	}
}

// Expired 回傳仍在配對中、且回覆期限已過的行程。Redis 上可以用以期限為分數的 sorted set 做到。
func (s *Store) Expired(now time.Time) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for id, d := range s.deadlines {
		if d.Before(now) {
			out = append(out, id)
		}
	}
	return out
}

func (s *Store) Get(rideID string) (State, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.states[rideID]
	if !ok {
		return State{}, false
	}
	return *st.clone(), true
}

func (s *Store) Counts() map[Status]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[Status]int)
	for _, st := range s.states {
		out[st.Status]++
	}
	return out
}
