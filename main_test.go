package main

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/gorilla/websocket"
)

func TestLoadConfig(t *testing.T) {
	// Test with missing file - should return default
	cfg := loadConfig("nonexistent.json")
	if cfg == nil || len(cfg.Remotes) != 1 {
		t.Error("Expected default config with 1 remote")
	}
	if cfg.Remotes[0].Name != "Twitch" {
		t.Errorf("Expected default remote name 'Twitch', got '%s'", cfg.Remotes[0].Name)
	}
}

func TestSaveAndLoadConfig(t *testing.T) {
	tmpFile := "test_config.json"
	defer os.Remove(tmpFile)

	cfg := &Config{
		Remotes: []Remote{
			{Name: "Test", Provider: "twitch", StreamKey: "test-key", Enabled: true},
		},
	}

	saveConfig(tmpFile, cfg)
	loaded := loadConfig(tmpFile)

	if len(loaded.Remotes) != 1 {
		t.Error("Expected 1 remote after load")
	}
	if loaded.Remotes[0].Name != "Test" {
		t.Errorf("Expected remote name 'Test', got '%s'", loaded.Remotes[0].Name)
	}
}

func TestBuildURL(t *testing.T) {
	tests := []struct {
		provider  string
		streamKey string
		expected  string
	}{
		{"twitch", "key123", "rtmp://live.twitch.tv/app/key123"},
		{"kick", "id:key", "rtmp://ingest.kick.com/live/id:key"},
		{"youtube", "yt-key", "rtmp://a.rtmp.youtube.com/live2/yt-key"},
		{"custom", "rtmp://custom.com/key", "rtmp://custom.com/key"},
	}

	for _, tt := range tests {
		result := buildURL(tt.provider, tt.streamKey)
		if result != tt.expected {
			t.Errorf("buildURL(%s, %s) = %s, expected %s", tt.provider, tt.streamKey, result, tt.expected)
		}
	}
}

func TestHandleRemotesGet(t *testing.T) {
	p := &StreamProxy{
		remotes: []Remote{
			{Name: "Test", Provider: "twitch", Enabled: true},
		},
		clients: make(map[*websocket.Conn]bool),
	}

	req := httptest.NewRequest("GET", "/api/remotes", nil)
	rec := httptest.NewRecorder()
	p.handleRemotes(rec, req)

	if rec.Code != 200 {
		t.Errorf("Expected 200, got %d", rec.Code)
	}

	var remotes []Remote
	if err := json.Unmarshal(rec.Body.Bytes(), &remotes); err != nil {
		t.Errorf("Failed to decode JSON: %v", err)
	}
	if len(remotes) != 1 {
		t.Errorf("Expected 1 remote, got %d", len(remotes))
	}
}

func TestHandleRemotesPost(t *testing.T) {
	p := &StreamProxy{
		remotes: []Remote{},
		clients: make(map[*websocket.Conn]bool),
	}
	cfg = &Config{Remotes: []Remote{}}

	remotes := []Remote{
		{Name: "New", Provider: "kick", StreamKey: "key", Enabled: true},
	}
	body, _ := json.Marshal(remotes)

	req := httptest.NewRequest("POST", "/api/remotes", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	p.handleRemotes(rec, req)

	if rec.Code != 200 {
		t.Errorf("Expected 200, got %d", rec.Code)
	}
	if len(p.remotes) != 1 {
		t.Errorf("Expected 1 remote after POST, got %d", len(p.remotes))
	}
}