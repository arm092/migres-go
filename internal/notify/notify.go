package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/arm092/migres-go/internal/config"
)

type Notifier struct {
	Config config.Config
	mu     sync.Mutex
	last   map[string]time.Time
	client *http.Client
}

func New(c config.Config) *Notifier {
	return &Notifier{Config: c, last: map[string]time.Time{}, client: &http.Client{Timeout: 5 * time.Second}}
}
func (n *Notifier) Send(ctx context.Context, kind, message string) {
	if !n.Config.Notifications.Enabled {
		return
	}
	n.mu.Lock()
	if time.Since(n.last[kind]) < n.Config.Notifications.RateLimit {
		n.mu.Unlock()
		return
	}
	n.last[kind] = time.Now()
	n.mu.Unlock()
	title := fmt.Sprintf("Migres [%s] %s", n.Config.Environment, kind)
	var wg sync.WaitGroup
	send := func(url string, payload any) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			body, err := json.Marshal(payload)
			if err != nil {
				return
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
			if err != nil {
				return
			}
			req.Header.Set("Content-Type", "application/json")
			resp, err := n.client.Do(req)
			if err == nil {
				resp.Body.Close()
			}
		}()
	}
	if p := n.Config.Notifications.Teams; p.Enabled && p.WebhookURL != "" {
		send(p.WebhookURL, map[string]any{"text": title + "\n" + message})
	}
	if p := n.Config.Notifications.Slack; p.Enabled && p.WebhookURL != "" {
		send(p.WebhookURL, map[string]any{"text": title + "\n" + message})
	}
	if p := n.Config.Notifications.Telegram; p.Enabled && p.BotToken != "" {
		send("https://api.telegram.org/bot"+p.BotToken+"/sendMessage", map[string]any{"chat_id": p.ChatID, "text": title + "\n" + message})
	}
	wg.Wait()
}
