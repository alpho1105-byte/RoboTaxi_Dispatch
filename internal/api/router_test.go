package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"robotaxi-dispatch/internal/app"
	"robotaxi-dispatch/internal/auth"
	"robotaxi-dispatch/internal/domain"
	"robotaxi-dispatch/internal/fleet"
)

type server struct {
	t       *testing.T
	handler http.Handler
	signer  *auth.Signer
}

func newServer(t *testing.T, vehicles int) *server {
	t.Helper()
	opts := app.DefaultOptions(t.TempDir())
	opts.Logger = slog.New(slog.DiscardHandler)
	a, err := app.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	beh := fleet.Behavior{SpeedMps: 1, Tick: 50 * time.Millisecond, ThinkTime: time.Millisecond, ConfirmTimeout: time.Second}
	f := fleet.New(a.Gateway, beh)
	f.Spawn(vehicles, domain.Point{Lat: 25.03, Lng: 121.56}, 500, 1)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{}, 2)
	go func() { a.Run(ctx); done <- struct{}{} }()
	go func() { f.Run(ctx); done <- struct{}{} }()
	t.Cleanup(func() {
		cancel()
		<-done
		<-done
		a.Close()
	})
	signer := auth.NewSigner([]byte("secret"))
	return &server{t: t, signer: signer, handler: NewRouter(Deps{Signer: signer, Rides: a.RideService, Locations: a.Locations})}
}

func (s *server) do(method, path, body, rider string) (int, map[string]any) {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if rider != "" {
		tok, _ := s.signer.Issue(rider, time.Hour)
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	rec := httptest.NewRecorder()
	s.handler.ServeHTTP(rec, req)
	var out map[string]any
	json.NewDecoder(rec.Body).Decode(&out)
	return rec.Code, out
}

const trip = `{"pickupLocation":{"lat":25.031,"lng":121.561},"destination":{"lat":25.05,"lng":121.54}}`

func TestFareRideAndMatch(t *testing.T) {
	s := newServer(t, 5)
	code, fare := s.do(http.MethodPost, "/fare", trip, "alice")
	if code != http.StatusOK || fare["fareId"] == nil || fare["amount"].(float64) <= 0 {
		t.Fatalf("fare = %d %v", code, fare)
	}
	body := `{"fareId":"` + fare["fareId"].(string) + `"}`
	code, rd := s.do(http.MethodPost, "/rides", body, "alice")
	if code != http.StatusCreated || rd["status"] != string(domain.RideRequested) {
		t.Fatalf("ride = %d %v", code, rd)
	}
	if code, again := s.do(http.MethodPost, "/rides", body, "alice"); code != http.StatusOK || again["rideId"] != rd["rideId"] {
		t.Fatalf("retry = %d %v, want the same ride", code, again)
	}

	path := "/rides/" + rd["rideId"].(string)
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, got := s.do(http.MethodGet, path, "", "alice")
		if got["status"] == string(domain.RideAssigned) {
			av := got["av"].(map[string]any)
			if av["avId"] == "" || av["location"] == nil {
				t.Fatalf("assigned ride without vehicle info: %v", got)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("ride was not matched: %v", got)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if code, _ := s.do(http.MethodGet, path, "", "bob"); code != http.StatusNotFound {
		t.Fatalf("another rider can see the ride: %d", code)
	}
	if code, _ := s.do(http.MethodPost, path+"/cancel", "", "alice"); code != http.StatusConflict {
		t.Fatalf("cancel after match = %d, want 409", code)
	}
}

func TestValidationAndAuth(t *testing.T) {
	s := newServer(t, 0)
	cases := []struct {
		method, path, body, rider string
		want                      int
	}{
		{http.MethodPost, "/fare", trip, "", http.StatusUnauthorized},
		{http.MethodPost, "/fare", `{"pickupLocation":{"lat":95,"lng":0},"destination":{"lat":0,"lng":0}}`, "alice", http.StatusBadRequest},
		{http.MethodPost, "/rides", `{"fareId":"fare_unknown"}`, "alice", http.StatusNotFound},
		{http.MethodPost, "/rides", `{}`, "alice", http.StatusBadRequest},
		{http.MethodGet, "/rides/ride_unknown", "", "alice", http.StatusNotFound},
	}
	for _, c := range cases {
		if code, body := s.do(c.method, c.path, c.body, c.rider); code != c.want {
			t.Errorf("%s %s => %d %v, want %d", c.method, c.path, code, body, c.want)
		}
	}
}
