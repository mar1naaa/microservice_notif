package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

var pool *pgxpool.Pool

const sessionID = "marina-dev"

type Order struct {
	ID          int64     `json:"id"`
	SessionID   string    `json:"session_id"`
	TotalAmount float64   `json:"total_amount"`
	Status_pay  string    `json:"payment_status"`
	CreatedAt   time.Time `json:"created_at"`
}

type CreateOrderRequest struct {
	TotalAmount float64 `json:"total_amount"`
}

func main() {
	var err error

	pool, err = pgxpool.New(
		context.Background(),
		"postgres://postgres:140701@localhost:5432/orders",
	)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	mux := http.NewServeMux()

	mux.HandleFunc("/api/orders", getOrderHandler)
	mux.HandleFunc("/api/orders/create", createOrderHandler)

	fs := http.FileServer(http.Dir("static"))
	mux.Handle("/", fs)

	log.Println("Order service starting on :9002")
	log.Fatal(http.ListenAndServe(":9002", withCORS(mux)))
}

func getOrderHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET only", http.StatusMethodNotAllowed)
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
			SELECT id, session_id, total_amount, payment_status, created_at
			FROM orders
			WHERE id = $1
		`, orderID).Scan(
			&order.ID,
			&order.SessionID,
			&order.TotalAmount,
			&order.Status_pay,
			&order.CreatedAt,
		)
		if err != nil {
			log.Println("get order error:", err)
			http.Error(w, "order not found", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		json.NewEncoder(w).Encode(order)
		return
	}

	rows, err := pool.Query(r.Context(), `
		SELECT id, session_id, total_amount, payment_status, created_at
		FROM orders
		WHERE session_id = $1
		ORDER BY created_at DESC
	`, sessionID)
	if err != nil {
		log.Println("list orders error:", err)
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	orders := []Order{}

	for rows.Next() {
		var order Order
		if err := rows.Scan(
			&order.ID,
			&order.SessionID,
			&order.TotalAmount,
			&order.Status_pay,
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

func createOrderHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	var req CreateOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}

	if req.TotalAmount < 0 {
		http.Error(w, "total_amount must be >= 0", http.StatusBadRequest)
		return
	}

	var order Order
	err := pool.QueryRow(r.Context(), `
		INSERT INTO orders (session_id, total_amount, payment_status)
		VALUES ($1, $2, 'paid')
		RETURNING id, session_id, total_amount, payment_status, created_at
	`, sessionID, req.TotalAmount).Scan(
		&order.ID,
		&order.SessionID,
		&order.TotalAmount,
		&order.Status_pay,
		&order.CreatedAt,
	)
	if err != nil {
		log.Println("create order error:", err)
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(order)
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}
