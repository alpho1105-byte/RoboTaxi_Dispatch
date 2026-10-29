package queue

import (
	"context"
	"sync"
	"time"
)

type Message[T any] struct {
	ID         uint64
	Body       T
	Deliveries int
}

type inflight[T any] struct {
	msg      Message[T]
	deadline time.Time
}

type Stats struct {
	Ready       int
	InFlight    int
	MaxDepth    int
	Published   int64
	Acked       int64
	Redelivered int64
}

// Leased 是帶有可見性逾時（visibility timeout）的佇列，語意類似 SQS：
// 取出的訊息只是「租借」給 consumer，必須在期限內 Ack；consumer 當機或卡住沒有 Ack，
// 租約到期後訊息會重新出現在佇列中，交給其他 consumer。尖峰時它也是緩衝，吸收超出處理速度的請求。
type Leased[T any] struct {
	visibility time.Duration

	mu       sync.Mutex
	ready    []Message[T]
	inflight map[uint64]inflight[T]
	nextID   uint64
	wake     chan struct{}
	stats    Stats
}

func NewLeased[T any](visibility time.Duration) *Leased[T] {
	return &Leased[T]{visibility: visibility, inflight: make(map[uint64]inflight[T]), wake: make(chan struct{})}
}

func (q *Leased[T]) Publish(body T) uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.nextID++
	q.ready = append(q.ready, Message[T]{ID: q.nextID, Body: body})
	q.stats.Published++
	q.stats.MaxDepth = max(q.stats.MaxDepth, len(q.ready))
	q.signal()
	return q.nextID
}

// Receive 一次取出最多 max 則訊息（prefetch）並開始租約；佇列為空時阻塞。
func (q *Leased[T]) Receive(ctx context.Context, max int) ([]Message[T], error) {
	for {
		q.mu.Lock()
		if len(q.ready) > 0 {
			n := min(max, len(q.ready))
			batch := make([]Message[T], n)
			copy(batch, q.ready[:n])
			q.ready = q.ready[n:]
			deadline := time.Now().Add(q.visibility)
			for i := range batch {
				batch[i].Deliveries++
				q.inflight[batch[i].ID] = inflight[T]{msg: batch[i], deadline: deadline}
			}
			q.mu.Unlock()
			return batch, nil
		}
		wake := q.wake
		q.mu.Unlock()
		select {
		case <-wake:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Ack 確認訊息已處理完畢；租約已過期、訊息已被重新投遞時回傳 false。
// 以投遞次數當作租約的 token：租約過期的 consumer 晚一步才 Ack 時，這則訊息可能已經交給別的 consumer，
// 只比對 id 的話，舊 consumer 會把新 consumer 還在處理的租約一起結束掉。
func (q *Leased[T]) Ack(m Message[T]) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	f, ok := q.inflight[m.ID]
	if !ok || f.msg.Deliveries != m.Deliveries {
		return false
	}
	delete(q.inflight, m.ID)
	q.stats.Acked++
	return true
}

// Run 定期把租約到期的訊息放回佇列。
func (q *Leased[T]) Run(ctx context.Context, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			q.requeueExpired(time.Now())
		}
	}
}

func (q *Leased[T]) requeueExpired(now time.Time) {
	q.mu.Lock()
	defer q.mu.Unlock()
	requeued := false
	for id, f := range q.inflight {
		if now.After(f.deadline) {
			delete(q.inflight, id)
			q.ready = append(q.ready, f.msg)
			q.stats.Redelivered++
			requeued = true
		}
	}
	if requeued {
		q.signal()
	}
}

func (q *Leased[T]) signal() {
	close(q.wake)
	q.wake = make(chan struct{})
}

func (q *Leased[T]) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()
	s := q.stats
	s.Ready, s.InFlight = len(q.ready), len(q.inflight)
	return s
}
