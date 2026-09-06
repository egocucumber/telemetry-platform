package alerter

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/egocucumber/telemetry-platform/internal/domain"
)

type Notifier interface {
	Name() string
	Notify(ctx context.Context, a domain.Alert) error
}

type LogNotifier struct{ Log *slog.Logger }

func (LogNotifier) Name() string { return "log" }

func (n LogNotifier) Notify(ctx context.Context, a domain.Alert) error {
	n.Log.WarnContext(ctx, "ALERT",
		"state", a.State, "severity", a.Severity, "rule", a.RuleName,
		"device", a.DeviceID, "metric", a.Metric, "value", a.Value, "threshold", a.Threshold)
	return nil
}

type WebhookNotifier struct {
	URL    string
	Secret string
	Client *http.Client
}

func (WebhookNotifier) Name() string { return "webhook" }

type webhookPayload struct {
	ID         string    `json:"id"`
	State      string    `json:"state"`
	Severity   string    `json:"severity"`
	Rule       string    `json:"rule"`
	RuleID     int64     `json:"rule_id"`
	DeviceID   string    `json:"device_id"`
	Metric     string    `json:"metric"`
	Value      float64   `json:"value"`
	Threshold  float64   `json:"threshold"`
	OccurredAt time.Time `json:"occurred_at"`
}

func (n WebhookNotifier) Notify(ctx context.Context, a domain.Alert) error {
	body, err := json.Marshal(webhookPayload{
		ID: a.ID, State: string(a.State), Severity: string(a.Severity), Rule: a.RuleName, RuleID: a.RuleID,
		DeviceID: a.DeviceID, Metric: a.Metric, Value: a.Value, Threshold: a.Threshold, OccurredAt: a.OccurredAt,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Alert-Id", a.ID)
	if n.Secret != "" {
		mac := hmac.New(sha256.New, []byte(n.Secret))
		mac.Write(body)
		req.Header.Set("X-Signature-256", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	}
	resp, err := n.Client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("webhook %s: status %d", n.URL, resp.StatusCode)
	}
	return nil
}

type TelegramNotifier struct {
	Token  string
	ChatID string
	Client *http.Client
}

func (TelegramNotifier) Name() string { return "telegram" }

func (n TelegramNotifier) Notify(ctx context.Context, a domain.Alert) error {
	icon := "🔥"
	if a.State == domain.AlertResolved {
		icon = "✅"
	}
	text := fmt.Sprintf("%s %s [%s]\n%s\n%s %s = %.2f (threshold %.2f)\n%s",
		icon, a.State, a.Severity, a.RuleName, a.DeviceID, a.Metric, a.Value, a.Threshold,
		a.OccurredAt.Format(time.RFC3339))

	form := url.Values{"chat_id": {n.ChatID}, "text": {text}}
	endpoint := "https://api.telegram.org/bot" + n.Token + "/sendMessage"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewBufferString(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := n.Client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("telegram: status %d", resp.StatusCode)
	}
	return nil
}
