package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"path/filepath"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/notedit/rtmp/format/rtmp"
)

type Remote struct {
	Name         string    `json:"name"`
	URL          string    `json:"url"`
	StreamKey    string    `json:"streamKey"`
	Enabled      bool      `json:"enabled"`
	Provider     string    `json:"provider"`
	Bitrate      int       `json:"bitrate"`
	Status       string    `json:"status"`
	LastUpdate   time.Time `json:"lastUpdate"`
	OutputWidth  int       `json:"outputWidth"`
	OutputHeight int       `json:"outputHeight"`
}

type StreamProxy struct {
	remotes   []Remote
	mu        sync.RWMutex
	clients   map[*websocket.Conn]bool
	clientsMu sync.RWMutex
}

type Config struct {
	Remotes []Remote `json:"remotes"`
}

var proxy *StreamProxy
var configPath string
var cfg *Config
var rtmpPort int
var httpPort int

func loadConfig(path string) *Config {
	data, err := os.ReadFile(path)
	if err != nil {
		return &Config{
			Remotes: []Remote{
				{Name: "Twitch", Provider: "twitch", StreamKey: "", Enabled: true},
			},
		}
	}
	var c Config
	if err := json.Unmarshal(data, &c); err != nil {
		return &Config{
			Remotes: []Remote{
				{Name: "Twitch", Provider: "twitch", StreamKey: "", Enabled: true},
			},
		}
	}
	return &c
}

func saveConfig(path string, c *Config) {
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		log.Printf("Error saving config: %v", err)
		return
	}
	os.WriteFile(path, data, 0644)
}

func buildURL(provider, streamKey string) string {
	switch provider {
	case "twitch":
		return "rtmp://live.twitch.tv/app/" + streamKey
	case "kick":
		return "rtmp://ingest.kick.com/live/" + streamKey
	case "youtube":
		return "rtmp://a.rtmp.youtube.com/live2/" + streamKey
	default:
		return streamKey
	}
}

func (p *StreamProxy) relayStream(srcConn *rtmp.Conn) {
	p.mu.RLock()
	var wg sync.WaitGroup
	for _, remote := range p.remotes {
		if remote.Enabled && remote.StreamKey != "" {
			wg.Add(1)
			go func(r Remote) {
				defer wg.Done()
				url := r.URL
				if r.Provider != "custom" {
					url = buildURL(r.Provider, r.StreamKey)
				}
				if url == "" {
					log.Printf("No URL for %s", r.Name)
					return
				}
				client := rtmp.NewClient()
				dstConn, nc, err := client.Dial(url, 0)
				if err != nil {
					log.Printf("Error connecting to %s: %v", r.Name, err)
					p.updateRemoteStatus(r.Name, "error", 0)
					return
				}
				defer nc.Close()

				if err := dstConn.Prepare(rtmp.StageGotPublishOrPlayCommand, rtmp.PrepareWriting); err != nil {
					log.Printf("Error preparing publish to %s: %v", r.Name, err)
					p.updateRemoteStatus(r.Name, "error", 0)
					return
				}

				p.updateRemoteStatus(r.Name, "active", 0)

				var bytesSent int64
				var bytesMu sync.Mutex
				startTime := time.Now()

				go func() {
					ticker := time.NewTicker(2 * time.Second)
					defer ticker.Stop()
					for range ticker.C {
						bytesMu.Lock()
						currentBytes := bytesSent
						bytesMu.Unlock()
						elapsed := time.Since(startTime).Seconds()
						if elapsed > 0 {
							bps := int(float64(currentBytes) / elapsed * 8)
							p.updateRemoteStatus(r.Name, "active", bps)
						}
					}
				}()

				for {
					pkt, err := srcConn.ReadPacket()
					if err != nil {
						return
					}
					if err := dstConn.WritePacket(pkt); err != nil {
						p.updateRemoteStatus(r.Name, "error", 0)
						return
					}
					bytesMu.Lock()
					bytesSent += int64(len(pkt.Data))
					bytesMu.Unlock()
				}
			}(remote)
		}
	}
	p.mu.RUnlock()
	wg.Wait()
}

func (p *StreamProxy) updateRemoteStatus(name string, status string, bitrate int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, r := range p.remotes {
		if r.Name == name {
			p.remotes[i].Status = status
			p.remotes[i].Bitrate = bitrate
			p.remotes[i].LastUpdate = time.Now()
			break
		}
	}
}

func (p *StreamProxy) startRTMPServer() {
	server := rtmp.NewServer()
	server.OnNewConn = func(c *rtmp.Conn) {
		log.Println("New RTMP connection")
	}
	server.HandleConn = func(c *rtmp.Conn, nc net.Conn) {
		log.Println("RTMP connection established")
		defer nc.Close()

		if err := c.Prepare(rtmp.StageGotPublishOrPlayCommand, rtmp.PrepareReading); err != nil {
			log.Printf("Error preparing connection: %v", err)
			return
		}

		p.relayStream(c)
	}

	addr := fmt.Sprintf(":%d", rtmpPort)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", addr, err)
	}
	defer listener.Close()

	for {
		nc, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}
		go server.HandleNetConn(nc)
	}
}

func (p *StreamProxy) handleWS(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket upgrade error: %v", err)
		return
	}

	p.clientsMu.Lock()
	p.clients[conn] = true
	p.clientsMu.Unlock()

	defer func() {
		p.clientsMu.Lock()
		delete(p.clients, conn)
		p.clientsMu.Unlock()
		conn.Close()
	}()

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			break
		}
		_ = msg
	}
}

func (p *StreamProxy) handleRemotes(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(p.remotes)
	case "POST":
		var remotes []Remote
		if err := json.NewDecoder(r.Body).Decode(&remotes); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		p.mu.Lock()
		p.remotes = remotes
		p.mu.Unlock()
		cfg.Remotes = remotes
		saveConfig(configPath, cfg)
		w.WriteHeader(200)
	}
}

func main() {
	flag.StringVar(&configPath, "config", "", "Path to config file (default: stream_proxy.json in binary dir)")
	flag.IntVar(&rtmpPort, "rtmp-port", 1935, "RTMP server port")
	flag.IntVar(&httpPort, "http-port", 9090, "HTTP server port")
	flag.Parse()

	if configPath == "" {
		exePath, _ := os.Executable()
		configPath = filepath.Join(filepath.Dir(exePath), "stream_proxy.json")
	}

	cfg = loadConfig(configPath)

	proxy = &StreamProxy{
		remotes: cfg.Remotes,
		clients: make(map[*websocket.Conn]bool),
	}

	go proxy.startRTMPServer()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", proxy.handleWS)
	mux.HandleFunc("/api/remotes", proxy.handleRemotes)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		data, _ := os.ReadFile("web/index.html")
		html := string(data)
		html = strings.Replace(html, "{{RTMP_PORT}}", fmt.Sprint(rtmpPort), 1)
		w.Write([]byte(html))
	})

	addr := fmt.Sprintf(":%d", httpPort)
	fmt.Printf("Stream proxy starting on %s (RTMP on :%d)\n", addr, rtmpPort)
	log.Fatal(http.ListenAndServe(addr, mux))
}