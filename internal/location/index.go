package location

import (
	"hash/fnv"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"robotaxi-dispatch/internal/domain"
	"robotaxi-dispatch/internal/geo"
)

// 鎖分成 64 段：位置更新只會鎖住自己那台車和所在網格的那一段，
// 不同區域、不同車輛的更新可以同時進行。
const shards = 64

type cellKey struct{ x, y int32 }

type cellShard struct {
	mu    sync.RWMutex
	cells map[cellKey]map[string]struct{}
}

type avShard struct {
	mu  sync.Mutex
	avs map[string]*record
}

type record struct {
	pos           domain.Point
	cell          cellKey
	battery       float64
	busy          bool
	reservedFor   string
	reservedUntil time.Time
	updatedAt     time.Time
}

// Candidate 是一台候選車輛與它到上車點的距離。
type Candidate struct {
	AVID      string
	DistanceM float64
}

// Info 是一台車目前的狀態。
type Info struct {
	AVID      string
	Pos       domain.Point
	Battery   float64
	Busy      bool
	UpdatedAt time.Time
}

// Index 是車輛位置的記憶體索引。位置資料是暫時性的，只需要最新位置，
// 所以不寫資料庫，維護兩個方向的對應：網格 → 車輛（找附近的車）、車輛 → 網格（換格時移除舊位置）。
// 正式環境可以用 Redis GEO，或依 H3 / S2 自建的地理索引服務。
type Index struct {
	cellDeg float64
	cells   [shards]cellShard
	avs     [shards]avShard
	now     func() time.Time
	updates atomic.Int64
}

// NewIndex 建立網格大小為 cellDeg 度的索引（0.005 度約 550 公尺）。
func NewIndex(cellDeg float64) *Index {
	x := &Index{cellDeg: cellDeg, now: time.Now}
	for i := range shards {
		x.cells[i].cells = make(map[cellKey]map[string]struct{})
		x.avs[i].avs = make(map[string]*record)
	}
	return x
}

func (x *Index) cellOf(p domain.Point) cellKey {
	return cellKey{int32(math.Floor(p.Lat / x.cellDeg)), int32(math.Floor(p.Lng / x.cellDeg))}
}

func (x *Index) cellShard(c cellKey) *cellShard {
	return &x.cells[uint32(c.x*73856093^c.y*19349663)%shards]
}

func (x *Index) avShard(id string) *avShard {
	h := fnv.New32a()
	h.Write([]byte(id))
	return &x.avs[h.Sum32()%shards]
}

// Update 記錄車輛回報的最新位置與狀態。busy 由車輛自己回報，是否載客以車輛為準。
func (x *Index) Update(id string, pos domain.Point, battery float64, busy bool, at time.Time) {
	x.updates.Add(1)
	s := x.avShard(id)
	s.mu.Lock()
	defer s.mu.Unlock()

	c := x.cellOf(pos)
	r, ok := s.avs[id]
	if !ok {
		r = &record{cell: c}
		s.avs[id] = r
		x.addToCell(c, id)
	} else if r.cell != c {
		x.removeFromCell(r.cell, id)
		x.addToCell(c, id)
		r.cell = c
	}
	r.pos, r.battery, r.busy, r.updatedAt = pos, battery, busy, at
}

func (x *Index) Remove(id string) {
	s := x.avShard(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.avs[id]; ok {
		x.removeFromCell(r.cell, id)
		delete(s.avs, id)
	}
}

func (x *Index) addToCell(c cellKey, id string) {
	cs := x.cellShard(c)
	cs.mu.Lock()
	defer cs.mu.Unlock()
	set := cs.cells[c]
	if set == nil {
		set = make(map[string]struct{})
		cs.cells[c] = set
	}
	set[id] = struct{}{}
}

func (x *Index) removeFromCell(c cellKey, id string) {
	cs := x.cellShard(c)
	cs.mu.Lock()
	defer cs.mu.Unlock()
	delete(cs.cells[c], id)
	if len(cs.cells[c]) == 0 {
		delete(cs.cells, c)
	}
}

// Nearby 找出 center 附近 maxM 公尺內、可接單的 k 台車，由近到遠排序。
// 從中心網格一圈一圈往外找；第 r 圈之外的車至少相距 r 個網格，
// 已經找到 k 台比這更近的車就可以停止。
func (x *Index) Nearby(center domain.Point, k int, maxM, minBattery float64, exclude map[string]bool) []Candidate {
	now := x.now()
	c := x.cellOf(center)
	cellM := x.cellDeg * geo.MetersPerDegreeLat * math.Cos(center.Lat*math.Pi/180)
	maxRing := int(maxM/cellM) + 1

	var found []Candidate
	for r := 0; r <= maxRing; r++ {
		for dx := -r; dx <= r; dx++ {
			for dy := -r; dy <= r; dy++ {
				if max(abs(dx), abs(dy)) != r {
					continue
				}
				for _, id := range x.idsIn(cellKey{c.x + int32(dx), c.y + int32(dy)}) {
					if exclude[id] {
						continue
					}
					if d, ok := x.available(id, center, minBattery, now); ok && d <= maxM {
						found = append(found, Candidate{AVID: id, DistanceM: d})
					}
				}
			}
		}
		if len(found) >= k {
			slices.SortFunc(found, func(a, b Candidate) int { return cmpFloat(a.DistanceM, b.DistanceM) })
			if found[k-1].DistanceM <= float64(r)*cellM {
				break
			}
		}
	}
	slices.SortFunc(found, func(a, b Candidate) int { return cmpFloat(a.DistanceM, b.DistanceM) })
	return found[:min(k, len(found))]
}

func (x *Index) idsIn(c cellKey) []string {
	cs := x.cellShard(c)
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	ids := make([]string, 0, len(cs.cells[c]))
	for id := range cs.cells[c] {
		ids = append(ids, id)
	}
	return ids
}

func (x *Index) available(id string, center domain.Point, minBattery float64, now time.Time) (float64, bool) {
	s := x.avShard(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.avs[id]
	if !ok || r.busy || r.battery < minBattery || (r.reservedFor != "" && now.Before(r.reservedUntil)) {
		return 0, false
	}
	return geo.DistanceM(center, r.pos), true
}

// Reserve 暫時把車保留給某趟行程，避免同一時間被詢問接其他行程。
// 這只是降低衝突的軟性保留，有 TTL 會自動失效；真正保證一台車只接一趟的是 Ride DB 的 unique index。
func (x *Index) Reserve(id, rideID string, ttl time.Duration) bool {
	now := x.now()
	s := x.avShard(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.avs[id]
	if !ok || r.busy || (r.reservedFor != "" && r.reservedFor != rideID && now.Before(r.reservedUntil)) {
		return false
	}
	r.reservedFor, r.reservedUntil = rideID, now.Add(ttl)
	return true
}

// Release 解除保留；只有保留者本人能解除。
func (x *Index) Release(id, rideID string) {
	s := x.avShard(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.avs[id]; ok && r.reservedFor == rideID {
		r.reservedFor = ""
	}
}

// SetBusy 在配對成立時立刻標記車輛忙碌，不必等車輛下一次回報。
func (x *Index) SetBusy(id string, busy bool) {
	s := x.avShard(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.avs[id]; ok {
		r.busy = busy
		r.reservedFor = ""
	}
}

func (x *Index) Get(id string) (Info, bool) {
	s := x.avShard(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.avs[id]
	if !ok {
		return Info{}, false
	}
	return Info{AVID: id, Pos: r.pos, Battery: r.battery, Busy: r.busy, UpdatedAt: r.updatedAt}, true
}

func (x *Index) Updates() int64 {
	return x.updates.Load()
}

func abs(v int) int {
	if v < 0 {
		return -v
	}
	return v
}

func cmpFloat(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}
