package ride

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"robotaxi-dispatch/internal/domain"
	"robotaxi-dispatch/internal/storage"
)

// uniq_active_ride_per_av 是「一台車同時只接一趟」的最後防線：
// 就算配對流程出錯、車輛同時接受了兩趟，資料庫也只會讓其中一個指派成功。
const schema = `
CREATE TABLE IF NOT EXISTS fares (
	id           TEXT PRIMARY KEY,
	rider_id     TEXT    NOT NULL,
	pickup_lat   REAL    NOT NULL,
	pickup_lng   REAL    NOT NULL,
	dest_lat     REAL    NOT NULL,
	dest_lng     REAL    NOT NULL,
	distance_m   INTEGER NOT NULL,
	amount_cents INTEGER NOT NULL,
	expires_at   INTEGER NOT NULL,
	created_at   INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS rides (
	id           TEXT PRIMARY KEY,
	fare_id      TEXT    NOT NULL UNIQUE,
	rider_id     TEXT    NOT NULL,
	pickup_lat   REAL    NOT NULL,
	pickup_lng   REAL    NOT NULL,
	dest_lat     REAL    NOT NULL,
	dest_lng     REAL    NOT NULL,
	amount_cents INTEGER NOT NULL,
	status       TEXT    NOT NULL,
	av_id        TEXT,
	reason       TEXT    NOT NULL DEFAULT '',
	requested_at INTEGER NOT NULL,
	resolved_at  INTEGER,
	assigned_at  INTEGER,
	picked_up_at INTEGER,
	completed_at INTEGER
);
CREATE UNIQUE INDEX IF NOT EXISTS uniq_active_ride_per_av ON rides(av_id)
	WHERE status IN ('DRIVER_ASSIGNED', 'IN_PROGRESS');
CREATE INDEX IF NOT EXISTS idx_rides_status ON rides(status);`

var (
	// ErrAVBusy 表示這台車已經有一趟進行中的行程。
	ErrAVBusy = errors.New("av already has an active ride")
	// ErrNotAssignable 表示行程已不在等待配對的狀態（已配對、取消或失敗）。
	ErrNotAssignable = errors.New("ride is not waiting for an av")
)

type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	db, err := storage.OpenSQLite(path)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("create ride schema: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) SaveFare(ctx context.Context, f domain.Fare) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO fares (id, rider_id, pickup_lat, pickup_lng, dest_lat, dest_lng, distance_m, amount_cents, expires_at, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		f.ID, f.RiderID, f.Pickup.Lat, f.Pickup.Lng, f.Destination.Lat, f.Destination.Lng,
		f.DistanceM, f.AmountCents, f.ExpiresAt.UnixMilli(), f.CreatedAt.UnixMilli())
	return err
}

func (s *Store) Fare(ctx context.Context, id string) (domain.Fare, bool, error) {
	var f domain.Fare
	var expires, created int64
	err := s.db.QueryRowContext(ctx, `
		SELECT id, rider_id, pickup_lat, pickup_lng, dest_lat, dest_lng, distance_m, amount_cents, expires_at, created_at
		FROM fares WHERE id = ?`, id).Scan(&f.ID, &f.RiderID, &f.Pickup.Lat, &f.Pickup.Lng, &f.Destination.Lat,
		&f.Destination.Lng, &f.DistanceM, &f.AmountCents, &expires, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Fare{}, false, nil
	}
	if err != nil {
		return domain.Fare{}, false, err
	}
	f.ExpiresAt, f.CreatedAt = time.UnixMilli(expires), time.UnixMilli(created)
	return f, true, nil
}

// CreateRide 建立行程並回傳是否為新建立。同一張估價單重送時回傳原本的行程，
// client 逾時重試不會叫到兩台車。
func (s *Store) CreateRide(ctx context.Context, r domain.Ride) (domain.Ride, bool, error) {
	res, err := s.db.ExecContext(ctx, `
		INSERT INTO rides (id, fare_id, rider_id, pickup_lat, pickup_lng, dest_lat, dest_lng, amount_cents, status, requested_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(fare_id) DO NOTHING`,
		r.ID, r.FareID, r.RiderID, r.Pickup.Lat, r.Pickup.Lng, r.Destination.Lat, r.Destination.Lng,
		r.AmountCents, string(r.Status), r.RequestedAt.UnixMilli())
	if err != nil {
		return domain.Ride{}, false, err
	}
	n, _ := res.RowsAffected()
	existing, _, err := s.query(ctx, `WHERE fare_id = ?`, r.FareID)
	return existing, n == 1, err
}

func (s *Store) Ride(ctx context.Context, id string) (domain.Ride, bool, error) {
	return s.query(ctx, `WHERE id = ?`, id)
}

// Assign 把等待配對的行程指派給車輛。車輛已有進行中的行程時，unique index 會讓它失敗。
func (s *Store) Assign(ctx context.Context, rideID, avID string, at time.Time) error {
	res, err := s.db.ExecContext(ctx, `
		UPDATE rides SET status = ?, av_id = ?, assigned_at = ?, resolved_at = ?
		WHERE id = ? AND status = ?`,
		string(domain.RideAssigned), avID, at.UnixMilli(), at.UnixMilli(), rideID, string(domain.RideRequested))
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return ErrAVBusy
		}
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotAssignable
	}
	return nil
}

// Transition 在行程目前是 from 狀態（且屬於 avID，若有指定）時改成 to，並記錄時間欄位。
func (s *Store) Transition(ctx context.Context, rideID, avID string, from, to domain.RideStatus, timeColumn, reason string, at time.Time) (bool, error) {
	q := `UPDATE rides SET status = ?, reason = ?`
	args := []any{string(to), reason}
	if timeColumn != "" {
		q += `, ` + timeColumn + ` = ?`
		args = append(args, at.UnixMilli())
	}
	q += ` WHERE id = ? AND status = ?`
	args = append(args, rideID, string(from))
	if avID != "" {
		q += ` AND av_id = ?`
		args = append(args, avID)
	}
	res, err := s.db.ExecContext(ctx, q, args...)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

func (s *Store) CountByStatus(ctx context.Context) (map[domain.RideStatus]int, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT status, COUNT(*) FROM rides GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[domain.RideStatus]int)
	for rows.Next() {
		var st domain.RideStatus
		var n int
		if err := rows.Scan(&st, &n); err != nil {
			return nil, err
		}
		out[st] = n
	}
	return out, rows.Err()
}

// All 回傳全部行程，只用於模擬與離線驗證。
func (s *Store) All(ctx context.Context) ([]domain.Ride, error) {
	return s.list(ctx, ``)
}

// Requested 回傳還在等待配對的行程。
func (s *Store) Requested(ctx context.Context) ([]domain.Ride, error) {
	return s.list(ctx, `WHERE status = ?`, string(domain.RideRequested))
}

func (s *Store) query(ctx context.Context, where string, args ...any) (domain.Ride, bool, error) {
	rides, err := s.list(ctx, where+` LIMIT 1`, args...)
	if err != nil || len(rides) == 0 {
		return domain.Ride{}, false, err
	}
	return rides[0], true, nil
}

func (s *Store) list(ctx context.Context, where string, args ...any) ([]domain.Ride, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, fare_id, rider_id, pickup_lat, pickup_lng, dest_lat, dest_lng, amount_cents, status,
			COALESCE(av_id, ''), reason, requested_at, resolved_at, assigned_at, picked_up_at, completed_at
		FROM rides `+where, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Ride
	for rows.Next() {
		var r domain.Ride
		var requested int64
		var resolved, assigned, picked, completed sql.NullInt64
		if err := rows.Scan(&r.ID, &r.FareID, &r.RiderID, &r.Pickup.Lat, &r.Pickup.Lng, &r.Destination.Lat,
			&r.Destination.Lng, &r.AmountCents, &r.Status, &r.AVID, &r.Reason, &requested, &resolved, &assigned, &picked, &completed); err != nil {
			return nil, err
		}
		r.RequestedAt, r.ResolvedAt = time.UnixMilli(requested), optTime(resolved)
		r.AssignedAt, r.PickedUpAt, r.CompletedAt = optTime(assigned), optTime(picked), optTime(completed)
		out = append(out, r)
	}
	return out, rows.Err()
}

func optTime(v sql.NullInt64) time.Time {
	if !v.Valid {
		return time.Time{}
	}
	return time.UnixMilli(v.Int64)
}
