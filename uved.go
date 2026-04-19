package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os" // [NEW]
	"strings"
	"sync"
	"sync/atomic" // [NEW]
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jackc/pgx/v5/pgxpool" // [NEW]
	"github.com/segmentio/kafka-go"
)

// [NEW] тип ключа для context
type contextKey string

// [NEW] безопасный ключ вместо строки "claims"
const claimsKey contextKey = "claims"

type Claims struct {
	UserID int64  `json:"user_id"`
	Name   string `json:"name"`
	Email  string `json:"email"`
	Role   string `json:"role"`
	jwt.RegisteredClaims
}

// [UPDATED] было одно поле Name, теперь отдельно имя пользователя и название товара
type ViewEvent struct {
	ID          int64  `json:"id,omitempty"`
	Time        string `json:"time"`
	EventType   string `json:"event_type"`
	UserID      string `json:"user_id"`
	UserName    string `json:"user_name"`
	Email       string `json:"email"`
	SKU         string `json:"sku"`
	ProductName string `json:"product_name"`
	Reason      string `json:"reason"`
}

var (
	clients   = make(map[chan string]bool)
	clientsMu sync.Mutex
	history   []string
	jwtKey    = []byte("my_secret_key")

	startTime  = time.Now()  // [NEW]
	errorCount atomic.Int64  // [NEW]
	db         *pgxpool.Pool // [NEW]
)

func main() {
	initDB()            // [NEW]
	loadRecentHistory() // [NEW]

	go consumeKafka()

	mux := http.NewServeMux()

	// [NEW] новые API
	mux.HandleFunc("/health", healthHandler)
	mux.Handle("/api/stats", authMiddleware(http.HandlerFunc(statsHandler)))
	mux.Handle("/api/me", authMiddleware(http.HandlerFunc(meHandler)))
	mux.HandleFunc("/api/history", historyHandler)

	// [UPDATED] live stream только для новых событий
	mux.HandleFunc("/api/events", eventsHandler)

	fs := http.FileServer(http.Dir("static"))
	mux.Handle("/", fs)

	log.Fatal(http.ListenAndServe(":9004", withCORS(mux)))
}

// =========================
// [NEW] PostgreSQL через pgx
// =========================

func initDB() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:140701@localhost:5432/notif"
	}

	var err error
	db, err = pgxpool.New(context.Background(), dsn)
	if err != nil {
		log.Fatal("Ошибка создания pgx pool:", err)
	}

	if err = db.Ping(context.Background()); err != nil {
		log.Fatal("Ошибка подключения к PostgreSQL:", err)
	}

	log.Println("PostgreSQL (pgx) подключён")
}

func saveEvent(event ViewEvent, raw string) (int64, error) {
	query := `
		INSERT INTO notifications (
			event_time, event_type, user_id, user_name, email, sku, product_name, reason, raw_json
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb
		)
		RETURNING id
	`

	var id int64
	err := db.QueryRow(
		context.Background(),
		query,
		event.Time,
		event.EventType,
		event.UserID,
		event.UserName,
		event.Email,
		event.SKU,
		event.ProductName,
		event.Reason,
		raw,
	).Scan(&id)

	if err != nil {
		return 0, err
	}

	return id, nil
}

func loadRecentHistory() {
	rows, err := db.Query(context.Background(), `
		SELECT id, event_time, event_type, user_id, user_name, email, sku, product_name, reason
		FROM notifications
		ORDER BY event_time DESC
		LIMIT 100
	`)
	if err != nil {
		log.Println("Ошибка загрузки истории:", err)
		return
	}
	defer rows.Close()

	var loaded []string

	for rows.Next() {
		var e ViewEvent
		var eventTime time.Time

		if err := rows.Scan(
			&e.ID,
			&eventTime,
			&e.EventType,
			&e.UserID,
			&e.UserName,
			&e.Email,
			&e.SKU,
			&e.ProductName,
			&e.Reason,
		); err != nil {
			log.Println("Ошибка scan истории:", err)
			continue
		}

		e.Time = eventTime.Format("2006-01-02 15:04:05")
		loaded = append(loaded, eventToJSON(e))
	}

	if err := rows.Err(); err != nil {
		log.Println("Ошибка rows истории:", err)
		return
	}

	clientsMu.Lock()
	history = loaded
	clientsMu.Unlock()
}

// =========================
// Kafka consumer
// =========================

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
			errorCount.Add(1) // [NEW]
			log.Println("Ошибка чтения сообщения:", err)
			continue
		}

		raw := string(msg.Value)

		// [UPDATED] теперь сначала строим структуру
		event := buildViewEvent(raw)

		// [NEW] сохраняем в PostgreSQL
		id, err := saveEvent(event, raw)
		if err != nil {
			errorCount.Add(1)
			log.Println("Ошибка сохранения события:", err)
			continue
		}
		event.ID = id

		eventJSON := eventToJSON(event)
		broadcast(eventJSON)
	}
}

// =========================
// Построение события
// =========================

func buildViewEvent(raw string) ViewEvent {
	var m map[string]interface{}
	_ = json.Unmarshal([]byte(raw), &m)

	eventType := getString(m, "event_type")
	name := getString(m, "name")
	userName := getString(m, "user_name")
	productName := getString(m, "product_name")

	// Логика разведения старого поля name по типу события
	if userName == "" && productName == "" && name != "" {
		switch eventType {
		case "user.logged_in", "user.registered", "user.login_failed":
			userName = name
		case "item.added", "product_viewed", "order.created":
			productName = name
		}
	}

	event := ViewEvent{
		Time:        time.Now().Format("2006-01-02 15:04:05"),
		EventType:   eventType,
		UserID:      getString(m, "user_id"),
		UserName:    userName,
		Email:       getString(m, "email"),
		SKU:         getString(m, "sku"),
		ProductName: productName,
		Reason:      getString(m, "reason"),
	}

	return event
}

func eventToJSON(event ViewEvent) string {
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

// =========================
// Broadcast
// =========================

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

// =========================
// SSE: только live-события
// =========================

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
	clientsMu.Unlock()

	defer func() {
		clientsMu.Lock()
		delete(clients, ch)
		clientsMu.Unlock()
		close(ch)
	}()

	// [UPDATED] историю теперь отдаёт /api/history
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

// =========================
// [NEW] REST API
// =========================

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	resp := map[string]any{
		"status":  "ok",
		"service": "notification-service",
		"time":    time.Now().Format(time.RFC3339),
	}

	_ = json.NewEncoder(w).Encode(resp)
}

func statsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	clientsMu.Lock()
	activeClients := len(clients)
	historySize := len(history)
	clientsMu.Unlock()

	resp := map[string]any{
		"active_clients": activeClients,
		"history_size":   historySize,
		"started_at":     startTime.Format(time.RFC3339),
		"uptime_sec":     int(time.Since(startTime).Seconds()),
		"error_count":    errorCount.Load(),
	}

	_ = json.NewEncoder(w).Encode(resp)
}

func meHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	claims, ok := r.Context().Value(claimsKey).(*Claims)
	if !ok || claims == nil {
		http.Error(w, "claims not found", http.StatusUnauthorized)
		return
	}

	resp := map[string]any{
		"user_id": claims.UserID,
		"name":    claims.Name,
		"email":   claims.Email,
		"role":    claims.Role,
	}

	_ = json.NewEncoder(w).Encode(resp)
}

func historyHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	rows, err := db.Query(context.Background(), `
		SELECT id, event_time, event_type, user_id, user_name, email, sku, product_name, reason
		FROM notifications
		ORDER BY event_time DESC
		LIMIT 100
	`)
	if err != nil {
		http.Error(w, "db query error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	events := make([]ViewEvent, 0)

	for rows.Next() {
		var e ViewEvent
		var eventTime time.Time

		if err := rows.Scan(
			&e.ID,
			&eventTime,
			&e.EventType,
			&e.UserID,
			&e.UserName,
			&e.Email,
			&e.SKU,
			&e.ProductName,
			&e.Reason,
		); err != nil {
			http.Error(w, "scan error: "+err.Error(), http.StatusInternalServerError)
			return
		}

		e.Time = eventTime.Format("2006-01-02 15:04:05")
		events = append(events, e)
	}

	if err := rows.Err(); err != nil {
		http.Error(w, "rows error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	_ = json.NewEncoder(w).Encode(events)
}

// =========================
// Auth middleware
// =========================

func authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			http.Error(w, "missing token", http.StatusUnauthorized)
			return
		}

		parts := strings.Split(authHeader, " ")
		if len(parts) != 2 || parts[0] != "Bearer" {
			http.Error(w, "invalid authorization header", http.StatusUnauthorized)
			return
		}

		tokenStr := parts[1]
		claims := &Claims{}

		token, err := jwt.ParseWithClaims(tokenStr, claims, func(token *jwt.Token) (interface{}, error) {
			return jwtKey, nil
		})
		if err != nil || !token.Valid {
			http.Error(w, "invalid token", http.StatusUnauthorized)
			return
		}

		ctx := context.WithValue(r.Context(), claimsKey, claims) // [UPDATED]
		next.ServeHTTP(w, r.WithContext(ctx))
	})
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
