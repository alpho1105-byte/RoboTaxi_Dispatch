package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"robotaxi-dispatch/internal/auth"
	"robotaxi-dispatch/internal/domain"
	"robotaxi-dispatch/internal/location"
	"robotaxi-dispatch/internal/ride"
)

type Deps struct {
	Signer    *auth.Signer
	Rides     *ride.Service
	Locations *location.Index
	// Dev 為 true 時註冊 /dev/token，方便本機簽發測試用的 token。
	Dev bool
}

func NewRouter(d Deps) http.Handler {
	mux := http.NewServeMux()
	h := &handler{rides: d.Rides, locations: d.Locations}
	authed := func(fn http.HandlerFunc) http.Handler { return requireAuth(d.Signer, fn) }

	mux.Handle("POST /fare", authed(h.estimate))
	mux.Handle("POST /rides", authed(h.request))
	mux.Handle("GET /rides/{rideId}", authed(h.get))
	mux.Handle("POST /rides/{rideId}/cancel", authed(h.cancel))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	if d.Dev {
		mux.HandleFunc("POST /dev/token", func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				RiderID string `json:"riderId"`
			}
			if err := decodeJSON(w, r, &req); err != nil || req.RiderID == "" {
				writeError(w, http.StatusBadRequest, "riderId is required")
				return
			}
			token, _ := d.Signer.Issue(req.RiderID, 24*time.Hour)
			writeJSON(w, http.StatusOK, map[string]string{"token": token})
		})
	}
	return mux
}

type handler struct {
	rides     *ride.Service
	locations *location.Index
}

// POST /fare
func (h *handler) estimate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		PickupLocation domain.Point `json:"pickupLocation"`
		Destination    domain.Point `json:"destination"`
	}
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "pickupLocation and destination are required")
		return
	}
	f, err := h.rides.EstimateFare(r.Context(), auth.UserFrom(r.Context()), req.PickupLocation, req.Destination)
	if err != nil {
		writeRideError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"fareId": f.ID, "amount": float64(f.AmountCents) / 100, "currency": "USD",
		"distanceKm": float64(f.DistanceM) / 1000, "expiresAt": f.ExpiresAt.UnixMilli(),
	})
}

// POST /rides：以估價單叫車。叫車請求進入 queue 後就回應，配對結果用 GET /rides/{rideId} 查詢。
func (h *handler) request(w http.ResponseWriter, r *http.Request) {
	var req struct {
		FareID string `json:"fareId"`
	}
	if err := decodeJSON(w, r, &req); err != nil || req.FareID == "" {
		writeError(w, http.StatusBadRequest, "fareId is required")
		return
	}
	rd, created, err := h.rides.RequestRide(r.Context(), auth.UserFrom(r.Context()), req.FareID)
	if err != nil {
		writeRideError(w, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, h.rideJSON(rd))
}

// GET /rides/{rideId}：包含被指派車輛的即時位置。
func (h *handler) get(w http.ResponseWriter, r *http.Request) {
	rd, err := h.rides.Ride(r.Context(), auth.UserFrom(r.Context()), r.PathValue("rideId"))
	if err != nil {
		writeRideError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, h.rideJSON(rd))
}

// POST /rides/{rideId}/cancel
func (h *handler) cancel(w http.ResponseWriter, r *http.Request) {
	if err := h.rides.Cancel(r.Context(), auth.UserFrom(r.Context()), r.PathValue("rideId")); err != nil {
		writeRideError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rideId": r.PathValue("rideId"), "status": domain.RideCanceled})
}

func (h *handler) rideJSON(rd domain.Ride) map[string]any {
	out := map[string]any{
		"rideId": rd.ID, "fareId": rd.FareID, "status": rd.Status, "amount": float64(rd.AmountCents) / 100,
		"pickupLocation": rd.Pickup, "destination": rd.Destination, "requestedAt": rd.RequestedAt.UnixMilli(),
	}
	if rd.Reason != "" {
		out["reason"] = rd.Reason
	}
	if rd.AVID != "" {
		av := map[string]any{"avId": rd.AVID}
		if info, ok := h.locations.Get(rd.AVID); ok {
			av["location"], av["battery"] = info.Pos, info.Battery
		}
		out["av"], out["assignedAt"] = av, rd.AssignedAt.UnixMilli()
	}
	return out
}

func writeRideError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, ride.ErrInvalidLocation):
		status = http.StatusBadRequest
	case errors.Is(err, ride.ErrFareNotFound), errors.Is(err, ride.ErrRideNotFound):
		status = http.StatusNotFound
	case errors.Is(err, ride.ErrFareExpired):
		status = http.StatusGone
	case errors.Is(err, ride.ErrNotCancelable):
		status = http.StatusConflict
	}
	msg := err.Error()
	if status == http.StatusInternalServerError {
		msg = "internal error"
	}
	writeError(w, status, msg)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func decodeJSON(w http.ResponseWriter, r *http.Request, v any) error {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

func requireAuth(signer *auth.Signer, next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok {
			writeError(w, http.StatusUnauthorized, "missing bearer token")
			return
		}
		user, err := signer.Verify(token)
		if err != nil {
			writeError(w, http.StatusUnauthorized, err.Error())
			return
		}
		next(w, r.WithContext(auth.WithUser(r.Context(), user)))
	})
}
