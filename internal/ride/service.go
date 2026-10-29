package ride

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	"robotaxi-dispatch/internal/domain"
	"robotaxi-dispatch/internal/fare"
)

var (
	ErrInvalidLocation = errors.New("pickup and destination must be valid coordinates")
	ErrFareNotFound    = errors.New("fare not found")
	ErrFareExpired     = errors.New("fare expired, request a new estimate")
	ErrRideNotFound    = errors.New("ride not found")
	ErrNotCancelable   = errors.New("only rides still waiting for an av can be canceled")
)

// Publisher 把行程請求交給 Matching Service。
type Publisher interface {
	Publish(domain.RideRequest) uint64
}

// Service 是 Ride Service：處理估價與叫車，並保存行程狀態。叫車只把請求放進 queue 就回應，
// 配對再慢、Matching Service 再忙，都不會拖垮這個入口。
type Service struct {
	store     *Store
	estimator fare.Estimator
	queue     Publisher
	now       func() time.Time
}

func NewService(store *Store, estimator fare.Estimator, queue Publisher) *Service {
	return &Service{store: store, estimator: estimator, queue: queue, now: time.Now}
}

func (s *Service) EstimateFare(ctx context.Context, riderID string, pickup, destination domain.Point) (domain.Fare, error) {
	if !pickup.Valid() || !destination.Valid() {
		return domain.Fare{}, ErrInvalidLocation
	}
	now := s.now()
	distance, amount := s.estimator.Estimate(pickup, destination)
	f := domain.Fare{
		ID: newID("fare_"), RiderID: riderID, Pickup: pickup, Destination: destination,
		DistanceM: distance, AmountCents: amount, ExpiresAt: now.Add(s.estimator.TTL), CreatedAt: now,
	}
	return f, s.store.SaveFare(ctx, f)
}

// RequestRide 依乘客確認過的估價單叫車，回傳行程與是否為新建立。
// 同一張估價單重送時回傳同一趟行程，也不會重複派車。
func (s *Service) RequestRide(ctx context.Context, riderID, fareID string) (domain.Ride, bool, error) {
	f, found, err := s.store.Fare(ctx, fareID)
	if err != nil {
		return domain.Ride{}, false, err
	}
	if !found || f.RiderID != riderID {
		return domain.Ride{}, false, ErrFareNotFound
	}
	now := s.now()
	if now.After(f.ExpiresAt) {
		return domain.Ride{}, false, ErrFareExpired
	}
	r, created, err := s.store.CreateRide(ctx, domain.Ride{
		ID: newID("ride_"), FareID: f.ID, RiderID: riderID, Pickup: f.Pickup, Destination: f.Destination,
		AmountCents: f.AmountCents, Status: domain.RideRequested, RequestedAt: now,
	})
	if err != nil {
		return domain.Ride{}, false, err
	}
	// 重送時行程若還在等待配對，就再送一次請求：上一次可能在寫入資料庫之後、放進 queue 之前就遺失了，
	// 只靠第一次送出的話，這趟行程會永遠停在 REQUESTED。Matching 會認出重複的請求，不會重複派車。
	if created || r.Status == domain.RideRequested {
		s.publish(r)
	}
	return r, created, nil
}

func (s *Service) publish(r domain.Ride) {
	s.queue.Publish(domain.RideRequest{RideID: r.ID, Pickup: r.Pickup, Destination: r.Destination, RequestedAt: r.RequestedAt})
}

// Recover 在啟動時把所有還在等待配對的行程重新放進 queue。queue 與配對狀態都在記憶體裡（正式環境是 SQS 與 Redis），
// 程序重啟後它們是空的，資料庫裡 REQUESTED 的行程沒有人會去處理。超過配對期限的行程會被 Matching 直接判定失敗。
func (s *Service) Recover(ctx context.Context) (int, error) {
	rides, err := s.store.Requested(ctx)
	if err != nil {
		return 0, err
	}
	for _, r := range rides {
		s.publish(r)
	}
	return len(rides), nil
}

func (s *Service) Ride(ctx context.Context, riderID, rideID string) (domain.Ride, error) {
	r, found, err := s.store.Ride(ctx, rideID)
	if err != nil {
		return domain.Ride{}, err
	}
	if !found || r.RiderID != riderID {
		return domain.Ride{}, ErrRideNotFound
	}
	return r, nil
}

func (s *Service) Cancel(ctx context.Context, riderID, rideID string) error {
	if _, err := s.Ride(ctx, riderID, rideID); err != nil {
		return err
	}
	ok, err := s.store.Transition(ctx, rideID, "", domain.RideRequested, domain.RideCanceled, "resolved_at", "canceled by rider", s.now())
	if err != nil {
		return err
	}
	if !ok {
		return ErrNotCancelable
	}
	// 通知 Matching 停止配對，不必等到某台車接受時才從 Assign 的失敗發現這趟行程已經取消。
	s.queue.Publish(domain.RideRequest{RideID: rideID, Canceled: true})
	return nil
}

// Assign 由 Matching Service 以 RPC 呼叫：車輛接受後把行程指派給它。
func (s *Service) Assign(ctx context.Context, rideID, avID string) error {
	return s.store.Assign(ctx, rideID, avID, s.now())
}

// Fail 由 Matching Service 呼叫：在期限內找不到願意接單的車。
func (s *Service) Fail(ctx context.Context, rideID, reason string) error {
	_, err := s.store.Transition(ctx, rideID, "", domain.RideRequested, domain.RideNoAVAvailable, "resolved_at", reason, s.now())
	return err
}

// TripEvent 處理車輛回報的行程進度。只接受被指派的那台車所回報的事件。
func (s *Service) TripEvent(ctx context.Context, avID, rideID string, kind domain.TripEventKind) error {
	now := s.now()
	var err error
	switch kind {
	case domain.TripPickedUp:
		_, err = s.store.Transition(ctx, rideID, avID, domain.RideAssigned, domain.RideInProgress, "picked_up_at", "", now)
	case domain.TripCompleted:
		_, err = s.store.Transition(ctx, rideID, avID, domain.RideInProgress, domain.RideCompleted, "completed_at", "", now)
	}
	return err
}

func newID(prefix string) string {
	b := make([]byte, 8)
	rand.Read(b)
	return prefix + hex.EncodeToString(b)
}
