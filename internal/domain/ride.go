package domain

import "time"

type Point struct {
	Lat float64 `json:"lat"`
	Lng float64 `json:"lng"`
}

func (p Point) Valid() bool {
	return p.Lat >= -90 && p.Lat <= 90 && p.Lng >= -180 && p.Lng <= 180
}

// Fare 是一次車資估價。乘客確認後才用它叫車，同一張估價單只能產生一趟行程。
type Fare struct {
	ID          string
	RiderID     string
	Pickup      Point
	Destination Point
	DistanceM   int
	AmountCents int64
	ExpiresAt   time.Time
	CreatedAt   time.Time
}

type RideStatus string

const (
	RideRequested     RideStatus = "REQUESTED"
	RideAssigned      RideStatus = "DRIVER_ASSIGNED"
	RideInProgress    RideStatus = "IN_PROGRESS"
	RideCompleted     RideStatus = "COMPLETED"
	RideNoAVAvailable RideStatus = "NO_AV_AVAILABLE"
	RideCanceled      RideStatus = "CANCELED"
)

// Resolved 表示配對已經有結果（成功或失敗），不會再被派車。
func (s RideStatus) Resolved() bool {
	return s != RideRequested
}

type Ride struct {
	ID          string
	FareID      string
	RiderID     string
	Pickup      Point
	Destination Point
	AmountCents int64
	Status      RideStatus
	AVID        string
	Reason      string
	RequestedAt time.Time
	// ResolvedAt 是配對有結果的時間：指派成功或宣告失敗。
	ResolvedAt  time.Time
	AssignedAt  time.Time
	PickedUpAt  time.Time
	CompletedAt time.Time
}

// RideRequest 是 Ride Service 放進 queue、交給 Matching Service 的事件。
type RideRequest struct {
	RideID      string
	Pickup      Point
	Destination Point
	RequestedAt time.Time
	// Canceled 為 true 時是乘客取消：Matching 停止配對並撤回已送出的派車詢問。
	Canceled bool
}
