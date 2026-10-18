package notify

import (
	"context"
	"encoding/json"
	"github.com/arm092/migres-go/internal/config"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

type redirectTransport struct{ target *url.URL }

func (r redirectTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	u := *req.URL
	u.Scheme = r.target.Scheme
	u.Host = r.target.Host
	clone.URL = &u
	return http.DefaultTransport.RoundTrip(clone)
}
func TestProvidersAndRateLimit(t *testing.T) {
	received := make(chan map[string]any, 10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Error("method")
		}
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
		}
		payload["path"] = r.URL.Path
		received <- payload
		w.WriteHeader(200)
	}))
	defer server.Close()
	c := config.Default()
	c.Environment = "test-env"
	c.Notifications.Enabled = true
	c.Notifications.RateLimit = time.Hour
	c.Notifications.Teams = config.Provider{Enabled: true, WebhookURL: server.URL + "/teams"}
	c.Notifications.Slack = config.Provider{Enabled: true, WebhookURL: server.URL + "/slack"}
	c.Notifications.Telegram = config.Provider{Enabled: true, BotToken: "fake-token", ChatID: "123"}
	n := New(c)
	target, _ := url.Parse(server.URL)
	n.client.Transport = redirectTransport{target}
	n.Send(context.Background(), "started", "Pipeline started")
	if len(received) != 3 {
		t.Fatalf("missing providers %d", len(received))
	}
	paths := map[string]bool{}
	for i := 0; i < 3; i++ {
		p := <-received
		paths[p["path"].(string)] = true
		text, _ := p["text"].(string)
		if !strings.Contains(text, "test-env") || !strings.Contains(text, "Pipeline started") {
			t.Fatalf("invalid message %+v", p)
		}
		if p["path"] == "/botfake-token/sendMessage" && p["chat_id"] != "123" {
			t.Fatal("telegram chat")
		}
	}
	if !paths["/teams"] || !paths["/slack"] || !paths["/botfake-token/sendMessage"] {
		t.Fatal(paths)
	}
	n.Send(context.Background(), "started", "same kind")
	if len(received) != 0 {
		t.Fatal("rate limit ignored")
	}
	n.Send(context.Background(), "blocked", "different kind")
	if len(received) != 3 {
		t.Fatal("different event suppressed")
	}
}
