package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/gorilla/websocket"
)

// Upgrader configures how we turn a normal HTTP connection into a live WebSocket
var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow connections from any origin (required for production)
	},
}

// Track all connected browsers
type WebClient struct {
	conn *websocket.Conn
	name string
}

var (
	webClients = make(map[*websocket.Conn]string)
	clientsMu  sync.Mutex
	broadcast  = make(chan string)
)

func main() {
	// 1. Serve the frontend HTML page on the homepage
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "index.html")
	})

	// 2. The live WebSocket endpoint the browser connects to
	http.HandleFunc("/ws", handleConnections)

	// 3. Start the background routine that sends messages to all open browsers
	go handleMessages()

	// 4. Fetch the dynamic port provided by the cloud server (Render)
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080" // Fallback to 8080 if running locally
	}

	fmt.Printf("🌍 GREEN Web Chat App is running on port %s\n", port)

	// Bind to the designated port to receive outside public traffic
	err := http.ListenAndServe(":"+port, nil)
	if err != nil {
		panic(err)
	}
}

func handleConnections(w http.ResponseWriter, r *http.Request) {
	// Upgrade the connection from HTTP to WebSocket
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Upgrade error: %v", err)
		return
	}
	defer ws.Close()

	// Wait for the first message, which will be the username
	_, nameBytes, err := ws.ReadMessage()
	if err != nil {
		return
	}
	username := string(nameBytes)

	clientsMu.Lock()
	webClients[ws] = username
	clientsMu.Unlock()

	broadcast <- fmt.Sprintf("✨ %s stepped into the room.", username)

	// Keep listening for messages from this browser tab
	for {
		_, msgBytes, err := ws.ReadMessage()
		if err != nil {
			clientsMu.Lock()
			leftUser := webClients[ws]
			delete(webClients, ws)
			clientsMu.Unlock()
			broadcast <- fmt.Sprintf("🏃 %s left the chat.", leftUser)
			break
		}

		broadcast <- fmt.Sprintf("💬 [%s]: %s", username, string(msgBytes))
	}
}

func handleMessages() {
	for {
		msg := <-broadcast
		fmt.Println(msg) // Log it to the VS Code terminal

		clientsMu.Lock()
		for client := range webClients {
			err := client.WriteMessage(websocket.TextMessage, []byte(msg))
			if err != nil {
				client.Close()
				delete(webClients, client)
			}
		}
		clientsMu.Unlock()
	}
}
