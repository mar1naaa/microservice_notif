package main

import (
	"context"
	"encoding/json"
	"fmt"
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
	ID              int64       `json:"id"`
	UserID          int64       `json:"user_id"`
	UserOrderNumber int         `json:"user_order_number"`
	TotalAmount     float64     `json:"total_amount"`
	PaymentStatus   string      `json:"payment_status"`
	CreatedAt       time.Time   `json:"created_at"`
	PickupAdress    string      `json:"pickup_address"`
	ArrivalDate     time.Time   `json:"arrival_date"`
	Items           []OrderItem `json:"items"`
}

type CreateOrderRequest struct {
	TotalAmount float64 `json:"total_amount"`
}

type OrderItem struct {
	SKU      string  `json:"sku"`
	Name     string  `json:"name"`
	ImageURL string  `json:"image_url"`
	Qty      int     `json:"qty"`
	Price    float64 `json:"price"`
	Sum      float64 `json:"sum"`
}

type CartResponse struct {
	Items      []OrderItem `json:"items"`
	TotalQty   int         `json:"total_qty"`
	TotalPrice float64     `json:"total_price"`
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

	// fs := http.FileServer(http.Dir("static"))
	// mux.Handle("/", fs)

	fs := http.FileServer(http.Dir("static"))
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".html") {
			w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
			w.Header().Set("Pragma", "no-cache")
			w.Header().Set("Expires", "0")
		}
		fs.ServeHTTP(w, r)
	}))

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

func loadOrderItems(ctx context.Context, orderID int64) ([]OrderItem, error) {
	rows, err := pool.Query(ctx, `
		SELECT sku, name, image_url, qty, price
		FROM order_items
		WHERE order_id = $1
		ORDER BY id
	`, orderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := []OrderItem{}

	for rows.Next() {
		var item OrderItem
		if err := rows.Scan(
			&item.SKU,
			&item.Name,
			&item.ImageURL,
			&item.Qty,
			&item.Price,
		); err != nil {
			return nil, err
		}

		item.Sum = item.Price * float64(item.Qty)
		items = append(items, item)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return items, nil
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

	// Один конкретный заказ
	if idStr != "" {
		orderID, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			http.Error(w, "invalid id", http.StatusBadRequest)
			return
		}

		var order Order
		err = pool.QueryRow(r.Context(), `
			SELECT id, user_id, user_order_number, total_amount, payment_status, created_at, pickup_address, arrival_date
			FROM orders
			WHERE id = $1 AND user_id = $2
		`, orderID, userID).Scan(
			&order.ID,
			&order.UserID,
			&order.UserOrderNumber,
			&order.TotalAmount,
			&order.PaymentStatus,
			&order.CreatedAt,
			&order.PickupAdress,
			&order.ArrivalDate,
		)
		if err != nil {
			http.Error(w, "order not found", http.StatusNotFound)
			return
		}

		items, err := loadOrderItems(r.Context(), order.ID)
		if err != nil {
			http.Error(w, "db error", http.StatusInternalServerError)
			return
		}
		order.Items = items

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		json.NewEncoder(w).Encode(order)
		return
	}

	// Список всех заказов пользователя
	rows, err := pool.Query(r.Context(), `
		SELECT id, user_id, user_order_number, total_amount, payment_status, created_at, pickup_address, arrival_date
		FROM orders
		WHERE user_id = $1
		ORDER BY created_at DESC
	`, userID)
	if err != nil {
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
			&order.PickupAdress,
			&order.ArrivalDate,
		); err != nil {
			http.Error(w, "db error", http.StatusInternalServerError)
			return
		}

		items, err := loadOrderItems(r.Context(), order.ID)
		if err != nil {
			http.Error(w, "db error", http.StatusInternalServerError)
			return
		}
		order.Items = items

		orders = append(orders, order)
	}

	if err := rows.Err(); err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
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

func fetchCart(ctx context.Context, authHeader string) (*CartResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost:9001/api/cart", nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", authHeader)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("basket service returned status %d", resp.StatusCode)
	}

	var cart CartResponse
	if err := json.NewDecoder(resp.Body).Decode(&cart); err != nil {
		return nil, err
	}

	return &cart, nil
}

// func createOrderHandler(w http.ResponseWriter, r *http.Request) {
// 	if r.Method != http.MethodPost {
// 		http.Error(w, "POST only", http.StatusMethodNotAllowed)
// 		return
// 	}

// 	userID, ok := getUserID(r)
// 	if !ok {
// 		http.Error(w, "unauthorized", http.StatusUnauthorized)
// 		return
// 	}

// 	var req CreateOrderRequest
// 	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
// 		http.Error(w, "broken json", http.StatusBadRequest)
// 		return
// 	}

// 	if req.TotalAmount < 0 {
// 		http.Error(w, "total_amount must be >= 0", http.StatusBadRequest)
// 		return
// 	}

// 	tx, err := pool.Begin(r.Context())
// 	if err != nil {
// 		http.Error(w, "db error", http.StatusInternalServerError)
// 		return
// 	}
// 	defer tx.Rollback(r.Context())

// 	kafkaWriter = &kafka.Writer{ //создание врайтера для отправки сообшщения
// 		Addr:     kafka.TCP("localhost:9092"),
// 		Topic:    "test-topic",
// 		Balancer: &kafka.LeastBytes{},
// 	}
// 	defer kafkartResponsaWriter.Close() //

// 	var nextNumber int
// 	err = tx.QueryRow(r.Context(), `
// 		SELECT COALESCE(MAX(user_order_number), 0) + 1
// 		FROM orders
// 		WHERE user_id = $1
// 	`, userID).Scan(&nextNumber)
// 	if err != nil {
// 		log.Println("next order number error:", err)
// 		http.Error(w, "db error", http.StatusInternalServerError)
// 		return
// 	}

// 	var order Order
// 	err = tx.QueryRow(r.Context(), `
// 		INSERT INTO orders (user_id, user_order_number, total_amount)
// 		VALUES ($1, $2, $3)
// 		RETURNING id, user_id, user_order_number, total_amount, payment_status, created_at
// 	`, userID, nextNumber, req.TotalAmount).Scan(
// 		&order.ID,
// 		&order.UserID,
// 		&order.UserOrderNumber,
// 		&order.TotalAmount,
// 		&order.PaymentStatus,
// 		&order.CreatedAt,
// 	)
// 	if err != nil {
// 		log.Println("create order error:", err)
// 		http.Error(w, "db error", http.StatusInternalServerError)
// 		return
// 	}

// 	if err := tx.Commit(r.Context()); err != nil {
// 		http.Error(w, "db error", http.StatusInternalServerError)
// 		return
// 	}

// 	if err := publishOrderCreated(r.Context(), order); err != nil { ///////////
// 		log.Println("kafka publish error:", err)
// 	}

// 	w.Header().Set("Content-Type", "application/json; charset=utf-8")
// 	w.WriteHeader(http.StatusCreated)
// 	json.NewEncoder(w).Encode(order)
// }

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

	authHeader := r.Header.Get("Authorization")

	cart, err := fetchCart(r.Context(), authHeader)
	if err != nil {
		log.Println("fetchCart error:", err)
		http.Error(w, "failed to fetch cart", http.StatusInternalServerError)
		return
	}

	if len(cart.Items) == 0 {
		http.Error(w, "cart is empty", http.StatusBadRequest)
		return
	}

	tx, err := pool.Begin(r.Context())
	if err != nil {
		log.Println("begin tx error:", err)
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback(r.Context())

	var nextNumber int
	err = tx.QueryRow(r.Context(), `
		SELECT COALESCE(MAX(user_order_number), 0) + 1
		FROM orders
		WHERE user_id = $1
	`, userID).Scan(&nextNumber)
	if err != nil {
		log.Println("nextNumber query error:", err)
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}

	var order Order
	err = tx.QueryRow(r.Context(), `
		INSERT INTO orders (user_id, user_order_number, total_amount)
		VALUES ($1, $2, $3)
		RETURNING id, user_id, user_order_number, total_amount, payment_status, created_at
	`, userID, nextNumber, cart.TotalPrice).Scan(
		&order.ID,
		&order.UserID,
		&order.UserOrderNumber,
		&order.TotalAmount,
		&order.PaymentStatus,
		&order.CreatedAt,
	)
	if err != nil {
		log.Println("insert order error:", err)
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}

	for _, item := range cart.Items {
		lineSum := item.Price * float64(item.Qty)

		_, err := tx.Exec(r.Context(), `
			INSERT INTO order_items (order_id, sku, name, image_url, qty, price, line_sum)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
		`, order.ID, item.SKU, item.Name, item.ImageURL, item.Qty, item.Price, lineSum)
		if err != nil {
			log.Println("insert order_item error:", err)
			http.Error(w, "db error", http.StatusInternalServerError)
			return
		}
	}

	if err := tx.Commit(r.Context()); err != nil {
		log.Println("commit error:", err)
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}

	order.Items = make([]OrderItem, 0, len(cart.Items))
	for _, item := range cart.Items {
		order.Items = append(order.Items, OrderItem{
			SKU:      item.SKU,
			Name:     item.Name,
			ImageURL: item.ImageURL,
			Qty:      item.Qty,
			Price:    item.Price,
			Sum:      item.Sum,
		})
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
