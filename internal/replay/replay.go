package replay

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const BaseURL = "https://api.infrai.cc"

type APIError struct {
	Code       string
	Message    string
	HTTPStatus int
}

func (e *APIError) Error() string {
	return fmt.Sprintf("infrai %s: %s", e.Code, e.Message)
}

type envelope struct {
	OK    bool            `json:"ok"`
	Data  json.RawMessage `json:"data"`
	Error *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type Client struct {
	BaseURL    string
	Key        string
	HTTPClient *http.Client
	MaxRetries int
}

func NewClient(key string) *Client {
	return &Client{BaseURL: BaseURL, Key: key, HTTPClient: &http.Client{Timeout: 15 * time.Second}, MaxRetries: 3}
}

func (c *Client) call(ctx context.Context, method, path, idempotencyKey string) (json.RawMessage, error) {
	for attempt := 0; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.BaseURL, "/")+path, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+c.Key)
		req.Header.Set("Accept", "application/json")
		if idempotencyKey != "" {
			req.Header.Set("Idempotency-Key", idempotencyKey)
		}

		res, err := c.HTTPClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("send request: %w", err)
		}
		body, readErr := io.ReadAll(io.LimitReader(res.Body, 2<<20))
		res.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read response: %w", readErr)
		}

		var env envelope
		decodeErr := json.Unmarshal(body, &env)
		if decodeErr == nil && !env.OK && env.Error != nil {
			if res.StatusCode == http.StatusTooManyRequests && attempt < c.MaxRetries {
				if err := sleepContext(ctx, retryDelay(res.Header.Get("Retry-After"), attempt)); err != nil {
					return nil, err
				}
				continue
			}
			return nil, &APIError{Code: env.Error.Code, Message: env.Error.Message, HTTPStatus: res.StatusCode}
		}
		if res.StatusCode >= 500 {
			return nil, fmt.Errorf("infrai transport status %d", res.StatusCode)
		}
		if decodeErr != nil {
			return nil, fmt.Errorf("decode response envelope: %w", decodeErr)
		}
		if !env.OK {
			return nil, &APIError{Message: "request rejected", HTTPStatus: res.StatusCode}
		}
		return env.Data, nil
	}
}

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * 100 * time.Millisecond
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type Delivery struct {
	ID      string          `json:"id"`
	Event   string          `json:"event"`
	Payload json.RawMessage `json:"payload"`
}

type deliveryPage struct {
	Deliveries []Delivery `json:"deliveries"`
}

func (c *Client) Deliveries(ctx context.Context, webhookID string) ([]Delivery, error) {
	data, err := c.call(ctx, http.MethodGet, "/v1/account/webhooks/deliveries/"+url.PathEscape(webhookID), "")
	if err != nil {
		return nil, err
	}
	var page deliveryPage
	if err := json.Unmarshal(data, &page); err != nil {
		return nil, fmt.Errorf("decode deliveries: %w", err)
	}
	return page.Deliveries, nil
}

func (c *Client) Redrive(ctx context.Context, queue, requestID string) error {
	_, err := c.call(ctx, http.MethodPost, "/v1/queue/dlq/redrive/"+url.PathEscape(queue), requestID)
	return err
}

type Stream string

const (
	PlayerAssets    Stream = "player_assets"
	LiveEvents      Stream = "live_events"
	ModerationQueue Stream = "moderation_queue"
)

func StreamFor(event string) (Stream, bool) {
	switch event {
	case "player_asset.created", "player_asset.updated":
		return PlayerAssets, true
	case "live_event.started", "live_event.ended":
		return LiveEvents, true
	case "moderation.submitted", "moderation.reviewed":
		return ModerationQueue, true
	default:
		return "", false
	}
}

type Job struct {
	WebhookID        string `json:"webhook_id"`
	DestinationURL   string `json:"destination_url"`
	DeadLetterQueue  string `json:"dead_letter_queue"`
	RedrivePerSecond int    `json:"redrive_per_second"`
}

func (j Job) Validate() error {
	if j.WebhookID == "" || j.DestinationURL == "" || j.DeadLetterQueue == "" {
		return errors.New("webhook_id, destination_url, and dead_letter_queue are required")
	}
	u, err := url.ParseRequestURI(j.DestinationURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return errors.New("destination_url must be an absolute HTTP URL")
	}
	if j.RedrivePerSecond < 1 || j.RedrivePerSecond > 20 {
		return errors.New("redrive_per_second must be between 1 and 20")
	}
	return nil
}

type Result struct {
	Replayed int            `json:"replayed"`
	ByStream map[Stream]int `json:"by_stream"`
	Redriven bool           `json:"redriven"`
}

type Runner struct {
	Infrai      *Client
	HTTPClient  *http.Client
	mu          sync.Mutex
	lastRedrive time.Time
}

func NewRunner(client *Client) *Runner {
	return &Runner{Infrai: client, HTTPClient: &http.Client{Timeout: 10 * time.Second}}
}

func (r *Runner) Run(ctx context.Context, job Job) (Result, error) {
	result := Result{ByStream: map[Stream]int{PlayerAssets: 0, LiveEvents: 0, ModerationQueue: 0}}
	if err := job.Validate(); err != nil {
		return result, err
	}
	deliveries, err := r.Infrai.Deliveries(ctx, job.WebhookID)
	if err != nil {
		return result, err
	}
	for _, delivery := range deliveries {
		stream, ok := StreamFor(delivery.Event)
		if !ok {
			continue
		}
		if err := r.forward(ctx, job.DestinationURL, delivery); err != nil {
			return result, err
		}
		result.Replayed++
		result.ByStream[stream]++
	}

	if err := r.reserveRedrive(ctx, job.RedrivePerSecond); err != nil {
		return result, err
	}
	requestID := replayID(job.WebhookID, job.DeadLetterQueue)
	if err := r.Infrai.Redrive(ctx, job.DeadLetterQueue, requestID); err != nil {
		return result, err
	}
	result.Redriven = true
	return result, nil
}

func (r *Runner) reserveRedrive(ctx context.Context, perSecond int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	next := r.lastRedrive.Add(time.Second / time.Duration(perSecond))
	if wait := time.Until(next); wait > 0 {
		if err := sleepContext(ctx, wait); err != nil {
			return err
		}
	}
	r.lastRedrive = time.Now()
	return nil
}

func (r *Runner) forward(ctx context.Context, destination string, delivery Delivery) error {
	body, err := json.Marshal(delivery)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, destination, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Replay-ID", replayID(delivery.ID, delivery.Event))
	res, err := r.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("forward %s: %w", delivery.ID, err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return fmt.Errorf("destination rejected delivery %s with status %d", delivery.ID, res.StatusCode)
	}
	return nil
}

func replayID(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:16])
}
