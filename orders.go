package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

var pool *pgxpool.Pool
var kafkaWriter *kafka.Writer //создаём глобальную переменную отпраки
var jwtKey = []byte("my_secret_key")

type Claims struct {
	UserID int64  `json:"user_id"`
	Name   string `json:"name"`
	Email  string `json:"email"`
	Role   string `json:"role"`
	jwt.RegisteredClaims
}

type Order struct {
	ID              int64     `json:"id"`
	UserID          int64     `json:"user_id"`
	UserOrderNumber int       `json:"user_order_number"`
	TotalAmount     float64   `json:"total_amount"`
	PaymentStatus   string    `json:"payment_status"`
	CreatedAt       time.Time `json:"created_at"`
}

type CreateOrderRequest struct {
	TotalAmount float64 `json:"total_amount"`
}

type OrderCreatedEvent struct {
	EventType       string    `json:"event_type"`
	OrderID         int64     `json:"order_id"`
	UserID          int64     `json:"user_id"`
	UserOrderNumber int       `json:"user_order_number"`
	TotalAmount     float64   `json:"total_amount"`
	PaymentStatus   string    `json:"payment_status"`
	CreatedAt       time.Time `json:"created_at"`
}

func main() {
	var err error

	kafkaWriter = &kafka.Writer{ //создание врайтера для отправки сообшщения
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "test-topic",
		Balancer: &kafka.LeastBytes{},
	}
	defer kafkaWriter.Close()

	pool, err = pgxpool.New(
		context.Background(),
		"postgres://postgres:140701@localhost:5432/orders",
	)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	mux := http.NewServeMux()

	mux.Handle("/api/orders", authMiddleware(http.HandlerFunc(getOrdersHandler)))
	mux.Handle("/api/orders/create", authMiddleware(http.HandlerFunc(createOrderHandler)))

	fs := http.FileServer(http.Dir("static"))
	mux.Handle("/", fs)

	log.Println("Order service starting on :9002")
	log.Fatal(http.ListenAndServe(":9002", withCORS(mux)))
}

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

		ctx := context.WithValue(r.Context(), "claims", claims)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func getUserID(r *http.Request) (int64, bool) {
	claims, ok := r.Context().Value("claims").(*Claims)
	if !ok {
		return 0, false
	}
	return claims.UserID, true
}

func getOrdersHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET only", http.StatusMethodNotAllowed)
		return
	}

	userID, ok := getUserID(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	idStr := r.URL.Query().Get("id")

	if idStr != "" {
		orderID, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			http.Error(w, "invalid id", http.StatusBadRequest)
			return
		}

		var order Order
		err = pool.QueryRow(r.Context(), `
			SELECT id, user_id, user_order_number, total_amount, payment_status, created_at
			FROM orders
			WHERE id = $1 AND user_id = $2
		`, orderID, userID).Scan(
			&order.ID,
			&order.UserID,
			&order.UserOrderNumber,
			&order.TotalAmount,
			&order.PaymentStatus,
			&order.CreatedAt,
		)
		if err != nil {
			http.Error(w, "order not found", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		json.NewEncoder(w).Encode(order)
		return
	}

	rows, err := pool.Query(r.Context(), `
		SELECT id, user_id, user_order_number, total_amount, payment_status, created_at
		FROM orders
		WHERE user_id = $1
		ORDER BY created_at DESC
	`, userID)
	if err != nil {
		log.Println("orders query error:", err)
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	orders := []Order{}

	for rows.Next() {
		var order Order
		if err := rows.Scan(
			&order.ID,
			&order.UserID,
			&order.UserOrderNumber,
			&order.TotalAmount,
			&order.PaymentStatus,
			&order.CreatedAt,
		); err != nil {
			log.Println("scan order error:", err)
			http.Error(w, "scan error", http.StatusInternalServerError)
			return
		}

		orders = append(orders, order)
	}

	if err := rows.Err(); err != nil {
		log.Println("rows error:", err)
		http.Error(w, "rows error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(orders)
}

func publishOrderCreated(ctx context.Context, order Order) error { /////////////
	event := OrderCreatedEvent{
		EventType:       "order.created",
		OrderID:         order.ID,
		UserID:          order.UserID,
		UserOrderNumber: order.UserOrderNumber,
		TotalAmount:     order.TotalAmount,
		PaymentStatus:   order.PaymentStatus,
		CreatedAt:       order.CreatedAt,
	}

	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	return kafkaWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(strconv.FormatInt(order.ID, 10)),
		Value: data,
	})
}

func createOrderHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	userID, ok := getUserID(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	var req CreateOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "broken json", http.StatusBadRequest)
		return
	}

	if req.TotalAmount < 0 {
		http.Error(w, "total_amount must be >= 0", http.StatusBadRequest)
		return
	}

	tx, err := pool.Begin(r.Context())
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback(r.Context())

	kafkaWriter = &kafka.Writer{ //создание врайтера для отправки сообшщения
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "test-topic",
		Balancer: &kafka.LeastBytes{},
	}
	defer kafkaWriter.Close() //

	var nextNumber int
	err = tx.QueryRow(r.Context(), `
		SELECT COALESCE(MAX(user_order_number), 0) + 1
		FROM orders
		WHERE user_id = $1
	`, userID).Scan(&nextNumber)
	if err != nil {
		log.Println("next order number error:", err)
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}

	var order Order
	err = tx.QueryRow(r.Context(), `
		INSERT INTO orders (user_id, user_order_number, total_amount)
		VALUES ($1, $2, $3)
		RETURNING id, user_id, user_order_number, total_amount, payment_status, created_at
	`, userID, nextNumber, req.TotalAmount).Scan(
		&order.ID,
		&order.UserID,
		&order.UserOrderNumber,
		&order.TotalAmount,
		&order.PaymentStatus,
		&order.CreatedAt,
	)
	if err != nil {
		log.Println("create order error:", err)
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}

	if err := tx.Commit(r.Context()); err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}

	if err := publishOrderCreated(r.Context(), order); err != nil { ///////////
		log.Println("kafka publish error:", err)
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(order)
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}
