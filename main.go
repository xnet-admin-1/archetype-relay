package main

import (
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

type Room struct {
	code    string
	clients []*websocket.Conn
	mu      sync.Mutex
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

func handleWS(w http.ResponseWriter, r *http.Request) {
	action := r.URL.Query().Get("action") // "host" or "join"
	code := r.URL.Query().Get("code")

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	var room *Room

	roomsMu.Lock()
	if action == "host" {
		code = genCode()
		room = &Room{code: code, clients: []*websocket.Conn{conn}}
		rooms[code] = room
		roomsMu.Unlock()
		conn.WriteJSON(map[string]string{"type": "room", "code": code})
	} else {
		room = rooms[code]
		roomsMu.Unlock()
		if room == nil {
			conn.WriteJSON(map[string]string{"type": "error", "msg": "Room not found"})
			conn.Close()
			return
		}
		room.mu.Lock()
		room.clients = append(room.clients, conn)
		room.mu.Unlock()
		conn.WriteJSON(map[string]string{"type": "joined", "code": code})
		// Notify host
		room.mu.Lock()
		if len(room.clients) > 0 {
			room.clients[0].WriteJSON(map[string]string{"type": "peer_joined"})
		}
		room.mu.Unlock()
	}

	// Relay messages to all other clients in room
	defer func() {
		room.mu.Lock()
		for i, c := range room.clients {
			if c == conn {
				room.clients = append(room.clients[:i], room.clients[i+1:]...)
				break
			}
		}
		if len(room.clients) == 0 {
			roomsMu.Lock()
			delete(rooms, room.code)
			roomsMu.Unlock()
		}
		room.mu.Unlock()
		conn.Close()
	}()

	conn.SetReadDeadline(time.Now().Add(5 * time.Minute))
	conn.SetPongHandler(func(string) error { conn.SetReadDeadline(time.Now().Add(5 * time.Minute)); return nil })

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			break
		}
		conn.SetReadDeadline(time.Now().Add(5 * time.Minute))
		// Broadcast to others
		room.mu.Lock()
		for _, c := range room.clients {
			if c != conn {
				c.WriteMessage(websocket.TextMessage, msg)
			}
		}
		room.mu.Unlock()
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())
	http.HandleFunc("/ws", handleWS)
	fmt.Println("Archetype relay on :9803")
	http.ListenAndServe(":9803", nil)
}
