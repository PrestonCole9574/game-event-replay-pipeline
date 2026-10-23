package replay

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

func TestStreamFor(t *testing.T) {
	tests := []struct {
		event string
		want  Stream
		ok    bool
	}{
		{"player_asset.created", PlayerAssets, true},
		{"live_event.ended", LiveEvents, true},
		{"moderation.submitted", ModerationQueue, true},
		{"player.logged_in", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.event, func(t *testing.T) {
			got, ok := StreamFor(tt.event)
			if got != tt.want || ok != tt.ok {
				t.Fatalf("StreamFor(%q) = %q, %v; want %q, %v", tt.event, got, ok, tt.want, tt.ok)
			}
		})
	}
}

func TestRunnerReplaysGameStreamsAndRedrivesWithSameKey(t *testing.T) {
	const key = "test-key"
	var mu sync.Mutex
	var authorizations []string
	infrai := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		authorizations = append(authorizations, r.Header.Get("Authorization"))
		mu.Unlock()
		switch r.URL.Path {
		case "/v1/account/webhooks/deliveries/hook-7":
			fmt.Fprint(w, `{"ok":true,"data":{"deliveries":[{"id":"d1","event":"player_asset.created","payload":{"asset_id":"a1"}},{"id":"d2","event":"live_event.started","payload":{"event_id":"e1"}},{"id":"d3","event":"moderation.reviewed","payload":{"case_id":"m1"}},{"id":"d4","event":"player.logged_in","payload":{}}]},"error":null,"metadata":{}}`)
		case "/v1/queue/dlq/redrive/game-events":
			if r.Method != http.MethodPost || r.Header.Get("Idempotency-Key") == "" {
				t.Errorf("redrive request missing method or idempotency key")
			}
			fmt.Fprint(w, `{"ok":true,"data":{"accepted":true},"error":null,"metadata":{}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer infrai.Close()

	var replayed int
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.Header.Get("X-Replay-ID") == "" {
			t.Errorf("forward request missing method or replay id")
		}
		replayed++
		w.WriteHeader(http.StatusNoContent)
	}))
	defer destination.Close()

	client := NewClient(key)
	client.BaseURL = infrai.URL
	runner := NewRunner(client)
	result, err := runner.Run(context.Background(), Job{
		WebhookID: "hook-7", DestinationURL: destination.URL,
		DeadLetterQueue: "game-events", RedrivePerSecond: 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if replayed != 3 || result.Replayed != 3 || result.ByStream[PlayerAssets] != 1 || result.ByStream[LiveEvents] != 1 || result.ByStream[ModerationQueue] != 1 || !result.Redriven {
		t.Fatalf("unexpected replay result: %+v, destination count %d", result, replayed)
	}
	for _, auth := range authorizations {
		if auth != "Bearer "+key {
			t.Fatalf("authorization = %q", auth)
		}
	}
}
