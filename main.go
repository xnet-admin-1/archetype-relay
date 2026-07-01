package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

const dataDir = "./data"
const pingInterval = 30 * time.Second
const pongWait = 60 * time.Second

type Client struct {
	conn *websocket.Conn
	name string
}

type Room struct {
	Code      string   `json:"code"`
	Players   []string `json:"players"`
	CreatedAt int64    `json:"created_at"`
	clients   []*Client
	mu        sync.Mutex
}

var (
	rooms   = make(map[string]*Room)
	roomsMu sync.Mutex
)

func genCode() string {
	const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
	b := make([]byte, 4)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return string(b)
}

func saveRoom(room *Room) {
	os.MkdirAll(dataDir, 0755)
	data, _ := json.Marshal(room)
	os.WriteFile(filepath.Join(dataDir, room.Code+".json"), data, 0644)
}

func deleteRoomFile(code string) {
	os.Remove(filepath.Join(dataDir, code+".json"))
}

func sendJSON(conn *websocket.Conn, v interface{}) {
	data, _ := json.Marshal(v)
	conn.WriteMessage(websocket.TextMessage, data)
}

func playerNames(room *Room) []string {
	names := make([]string, len(room.clients))
	for i, c := range room.clients {
		names[i] = c.name
	}
	return names
}

func startPing(conn *websocket.Conn, done <-chan struct{}) {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second)); err != nil {
				return
			}
		case <-done:
			return
		}
	}
}

func handleWS(w http.ResponseWriter, r *http.Request) {
	action := r.URL.Query().Get("action")
	code := r.URL.Query().Get("code")
	name := r.URL.Query().Get("name")
	if name == "" {
		name = "Player"
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	done := make(chan struct{})
	go startPing(conn, done)

	client := &Client{conn: conn, name: name}
	var room *Room

	roomsMu.Lock()
	if action == "host" {
		code = genCode()
		room = &Room{
			Code:      code,
			Players:   []string{name},
			CreatedAt: time.Now().Unix(),
			clients:   []*Client{client},
		}
		rooms[code] = room
		roomsMu.Unlock()
		saveRoom(room)
		sendJSON(conn, map[string]interface{}{"type": "room", "code": code})
	} else {
		room = rooms[code]
		roomsMu.Unlock()
		if room == nil {
			sendJSON(conn, map[string]string{"type": "error", "msg": "Room not found"})
			conn.Close()
			close(done)
			return
		}
		room.mu.Lock()
		room.clients = append(room.clients, client)
		room.Players = playerNames(room)
		// Send joined with player list
		sendJSON(conn, map[string]interface{}{
			"type":    "joined",
			"code":    code,
			"players": room.Players,
		})
		// Notify all others
		for _, c := range room.clients {
			if c != client {
				sendJSON(c.conn, map[string]string{"type": "peer_joined", "name": name})
			}
		}
		room.mu.Unlock()
		saveRoom(room)
	}

	// Relay loop
	defer func() {
		close(done)
		room.mu.Lock()
		for i, c := range room.clients {
			if c == client {
				room.clients = append(room.clients[:i], room.clients[i+1:]...)
				break
			}
		}
		// Notify others of departure
		for _, c := range room.clients {
			sendJSON(c.conn, map[string]string{"type": "peer_left", "name": name})
		}
		room.Players = playerNames(room)
		if len(room.clients) == 0 {
			room.mu.Unlock()
			roomsMu.Lock()
			delete(rooms, room.Code)
			roomsMu.Unlock()
			deleteRoomFile(room.Code)
		} else {
			room.mu.Unlock()
			saveRoom(room)
		}
		conn.Close()
	}()

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			break
		}
		conn.SetReadDeadline(time.Now().Add(pongWait))

		// Broadcast to all others in room
		room.mu.Lock()
		for _, c := range room.clients {
			if c != client {
				c.conn.WriteMessage(websocket.TextMessage, msg)
			}
		}
		room.mu.Unlock()
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())
	os.MkdirAll(dataDir, 0755)
	http.HandleFunc("/ws", handleWS)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		roomsMu.Lock()
		count := len(rooms)
		roomsMu.Unlock()
		fmt.Fprintf(w, `{"service":"archetype-relay","rooms":%d}`, count)
	})
	fmt.Println("Archetype relay on :9803")
	http.ListenAndServe(":9803", nil)
}
