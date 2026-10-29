package queue

import (
	"context"
	"testing"
	"time"
)

func TestUnackedMessagesAreRedelivered(t *testing.T) {
	ctx := context.Background()
	q := NewLeased[string](time.Minute)
	q.Publish("a")
	q.Publish("b")

	batch, err := q.Receive(ctx, 10)
	if err != nil || len(batch) != 2 {
		t.Fatalf("batch = %+v, %v", batch, err)
	}
	q.Ack(batch[0])

	// 第二則沒有 Ack，租約到期後重新出現，投遞次數加一。
	q.requeueExpired(time.Now().Add(2 * time.Minute))
	again, _ := q.Receive(ctx, 10)
	if len(again) != 1 || again[0].Body != "b" || again[0].Deliveries != 2 {
		t.Fatalf("redelivered = %+v", again)
	}
	// 第一個 consumer 晚一步才 Ack：訊息已經交給下一個 consumer，舊的 Ack 不能結束新的租約。
	if q.Ack(batch[1]) {
		t.Fatal("the first consumer acked a message whose lease had already moved to another consumer")
	}
	if !q.Ack(again[0]) {
		t.Fatal("ack of the redelivered message should succeed")
	}
	if s := q.Stats(); s.Published != 2 || s.Acked != 2 || s.Redelivered != 1 || s.InFlight != 0 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestReceiveBlocksUntilPublish(t *testing.T) {
	q := NewLeased[int](time.Minute)
	got := make(chan []Message[int], 1)
	go func() {
		batch, _ := q.Receive(context.Background(), 1)
		got <- batch
	}()
	time.Sleep(10 * time.Millisecond)
	q.Publish(42)
	select {
	case batch := <-got:
		if batch[0].Body != 42 {
			t.Fatalf("batch = %+v", batch)
		}
	case <-time.After(time.Second):
		t.Fatal("Receive did not wake up")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := q.Receive(ctx, 1); err == nil {
		t.Fatal("Receive should return when the context is cancelled")
	}
}
