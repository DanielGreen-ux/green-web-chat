package main

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"sync"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

var (
	webClients = make(map[*websocket.Conn]string)
	clientsMu  sync.Mutex
	broadcast  = make(chan string)
)

// Embedded HTML layout
const htmlPage = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>GREEN Web Hub</title>
    <style> 
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: #e5ddd5; margin: 0; padding: 20px; display: flex; justify-content: center; }
        .chat-container { width: 100%; max-width: 450px; background: white; height: 85vh; border-radius: 10px; box-shadow: 0 4px 10px rgba(0,0,0,0.15); display: flex; flex-direction: column; overflow: hidden; }
        .header { background: #075e54; color: white; padding: 15px; font-size: 18px; font-weight: bold; text-align: center; }
        .messages { flex: 1; padding: 15px; overflow-y: auto; background: #efeae2; display: flex; flex-direction: column; gap: 8px; }
        .msg-row { background: white; padding: 8px 12px; border-radius: 8px; max-width: 80%; width: fit-content; box-shadow: 0 1px 1px rgba(0,0,0,0.1); font-size: 15px; }
        .input-area { padding: 10px; background: #f0f0f0; display: flex; gap: 10px; }
        input { flex: 1; padding: 10px; border: 1px solid #ccc; border-radius: 20px; outline: none; font-size: 15px; }
        button { background: #128c7e; color: white; border: none; padding: 0 20px; border-radius: 20px; cursor: pointer; font-weight: bold; }
        button:hover { background: #075e54; }
    </style>
</head>
<body>
<div class="chat-container">
    <div class="header">🟩 GREEN Web Hub</div>
    <div id="messages" class="messages"></div>
    <div class="input-area">
        <input type="text" id="msgInput" placeholder="Type a message..." onkeypress="handleKey(event)">
        <button onclick="sendMessage()">Send</button>
    </div>
</div>
<script>
    let ws;
    let username = prompt("Welcome to GREEN Hub! Enter your username:") || "Anonymous";
    const protocol = window.location.protocol === 'https:' ? 'wss://' : 'ws://';
    ws = new WebSocket(protocol + window.location.host + "/ws");
    ws.onopen = () => { ws.send(username); };
    ws.onmessage = (event) => {
        const msgDiv = document.getElementById("messages");
        if (msgDiv) {
            const newMsg = document.createElement("div");
            newMsg.className = "msg-row";
            newMsg.innerText = event.data;
            msgDiv.appendChild(newMsg);
            msgDiv.scrollTop = msgDiv.scrollHeight;
        }
    };
    function sendMessage() {
        const input = document.getElementById("msgInput");
        if (input && input.value.trim() !== "") {
            ws.send(input.value);
            input.value = "";
        }
    }
    function handleKey(event) { if (event.key === "Enter") { sendMessage(); } }
</script>
</body>
</html>`

func main() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(htmlPage))
	})

	http.HandleFunc("/ws", handleConnections)
	go handleMessages()

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	fmt.Printf("🌍 GREEN Web Chat App is running on port %s\n", port)
	err := http.ListenAndServe("0.0.0.0:"+port, nil)
	if err != nil {
		panic(err)
	}
}

func handleConnections(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Upgrade error: %v", err)
		return
	}
	defer ws.Close()

	_, nameBytes, err := ws.ReadMessage()
	if err != nil {
		return
	}
	username := string(nameBytes)

	clientsMu.Lock()
	webClients[ws] = username
	clientsMu.Unlock()

	broadcast <- fmt.Sprintf("✨ %s stepped into the room.", username)

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

func sendToTelegram(message string) {
	token := os.Getenv("TELEGRAM_BOT_TOKEN")
	chatID := os.Getenv("TELEGRAM_CHAT_ID")

	if token == "" || chatID == "" {
		log.Println("⚠️ Telegram forwarding skipped: Missing environment keys.")
		return
	}

	// This exact string layout forces the traffic gateway to connect correctly
	telegramURL := "https://telegram.org" + token + "/sendMessage"

	formData := url.Values{
		"chat_id": {chatID},
		"text":    {message},
	}

	go func() {
		resp, err := http.PostForm(telegramURL, formData)
		if err != nil {
			log.Printf("❌ Telegram API Connection Error: %v", err)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			log.Printf("❌ Telegram API Rejection Status Code: %d", resp.StatusCode)
		} else {
			log.Println("✅ Success: Notification forwarded smoothly to Telegram.")
		}
	}()
}

func handleMessages() {
	for {
		msg := <-broadcast
		fmt.Println(msg)

		// Fire off the background notification forwarding task
		sendToTelegram(msg)

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
