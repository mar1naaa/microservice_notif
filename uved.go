package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/segmentio/kafka-go"
)

type Claims struct {
	UserID int64  `json:"user_id"`
	Name   string `json:"name"`
	Email  string `json:"email"`
	Role   string `json:"role"`
	jwt.RegisteredClaims
}

type ViewEvent struct {
	Time      string `json:"time"`
	EventType string `json:"event_type"`
	UserID    any    `json:"user_id"`
	Email     any    `json:"email"`
	SKU       any    `json:"sku"`
	Name      any    `json:"name"`
	Reason    any    `json:"reason"`
}

var (
	clients   = make(map[chan string]bool)
	clientsMu sync.Mutex
	history   []string
	jwtKey    = []byte("my_secret_key")
)

func main() {
	go consumeKafka()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/events", eventsHandler)

	fs := http.FileServer(http.Dir("static"))
	mux.Handle("/", fs)

	log.Fatal(http.ListenAndServe(":9004", withCORS(mux)))
}

func consumeKafka() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     "test-topic",
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
	})
	defer reader.Close()

	reader.SetOffset(kafka.LastOffset)

	log.Println("Consumer запущен. Жду новые сообщения...")

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Println("Ошибка чтения сообщения:", err)
			continue
		}

		eventJSON := buildViewEvent(string(msg.Value))
		broadcast(eventJSON)
	}
}

func buildViewEvent(raw string) string {
	var m map[string]interface{}
	_ = json.Unmarshal([]byte(raw), &m)

	event := ViewEvent{
		Time:      time.Now().Format("2006-01-02 15:04:05"),
		EventType: getString(m, "event_type"),
		UserID:    getAny(m, "user_id"),
		Email:     getAny(m, "email"),
		SKU:       getAny(m, "sku"),
		Name:      getAny(m, "name"),
		Reason:    getAny(m, "reason"),
	}

	data, _ := json.Marshal(event)
	return string(data)
}

func getString(m map[string]interface{}, key string) string {
	v, ok := m[key]
	if !ok || v == nil {
		return ""
	}
	s, ok := v.(string)
	if ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

func getAny(m map[string]interface{}, key string) any {
	v, ok := m[key]
	if !ok || v == nil {
		return ""
	}
	return v
}

func broadcast(message string) {
	clientsMu.Lock()
	defer clientsMu.Unlock()

	history = append([]string{message}, history...)
	if len(history) > 100 {
		history = history[:100]
	}

	for ch := range clients {
		select {
		case ch <- message:
		default:
		}
	}
}

func eventsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	ch := make(chan string, 10)

	clientsMu.Lock()
	clients[ch] = true
	snapshot := append([]string(nil), history...)
	clientsMu.Unlock()

	defer func() {
		clientsMu.Lock()
		delete(clients, ch)
		clientsMu.Unlock()
		close(ch)
	}()

	for i := len(snapshot) - 1; i >= 0; i-- {
		fmt.Fprintf(w, "data: %s\n\n", snapshot[i])
	}
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case msg := <-ch:
			fmt.Fprintf(w, "data: %s\n\n", msg)
			flusher.Flush()
		}
	}
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}
