package gateway

import (
	"context"
	"errors"
	"sync"
	"time"

	"robotaxi-dispatch/internal/domain"
	"robotaxi-dispatch/internal/location"
)

var (
	ErrNotConnected = errors.New("av is not connected")
	ErrBackpressure = errors.New("av command buffer is full")
)

// TripHandler 接收車輛回報的行程進度。
type TripHandler interface {
	TripEvent(ctx context.Context, avID, rideID string, kind domain.TripEventKind) error
}

// Gateway 是 AV Gateway：每台車都和它維持一條已驗證的長連線（正式環境是 gRPC bidirectional stream）。
// 位置更新直接寫進位置索引；派車回覆轉給 Matching Service；行程進度轉給 Ride Service。
type Gateway struct {
	loc       *location.Index
	trips     TripHandler
	decisions chan domain.DispatchDecision

	mu       sync.RWMutex
	sessions map[string]*Session
}

func New(loc *location.Index, trips TripHandler, buffer int) *Gateway {
	return &Gateway{loc: loc, trips: trips, decisions: make(chan domain.DispatchDecision, buffer), sessions: make(map[string]*Session)}
}

// Session 是一台車的連線。
type Session struct {
	AVID     string
	commands chan domain.Command
	g        *Gateway
}

// Connect 建立連線；同一台車重新連線時取代舊的連線。
func (g *Gateway) Connect(avID string) *Session {
	s := &Session{AVID: avID, commands: make(chan domain.Command, 64), g: g}
	g.mu.Lock()
	g.sessions[avID] = s
	g.mu.Unlock()
	return s
}

func (g *Gateway) Disconnect(s *Session) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.sessions[s.AVID] == s {
		delete(g.sessions, s.AVID)
		g.loc.Remove(s.AVID)
	}
}

// Send 把指令送到車輛的連線上。不會阻塞：車輛來不及消化時直接回報失敗，由呼叫端換下一台。
func (g *Gateway) Send(avID string, cmd domain.Command) error {
	g.mu.RLock()
	s, ok := g.sessions[avID]
	g.mu.RUnlock()
	if !ok {
		return ErrNotConnected
	}
	select {
	case s.commands <- cmd:
		return nil
	default:
		return ErrBackpressure
	}
}

func (g *Gateway) Decisions() <-chan domain.DispatchDecision {
	return g.decisions
}

func (s *Session) Commands() <-chan domain.Command {
	return s.commands
}

func (s *Session) ReportLocation(pos domain.Point, battery float64, busy bool) {
	s.g.loc.Update(s.AVID, pos, battery, busy, time.Now())
}

func (s *Session) Decide(ctx context.Context, rideID string, d domain.Decision, reason string) {
	select {
	case s.g.decisions <- domain.DispatchDecision{RideID: rideID, AVID: s.AVID, Decision: d, Reason: reason}:
	case <-ctx.Done():
	}
}

func (s *Session) Trip(ctx context.Context, rideID string, kind domain.TripEventKind) error {
	return s.g.trips.TripEvent(ctx, s.AVID, rideID, kind)
}
