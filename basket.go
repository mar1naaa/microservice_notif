package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var pool *pgxpool.Pool
var jwtKey = []byte("my_secret_key")

type Claims struct {
	UserID int64  `json:"user_id"`
	Name   string `json:"name"`
	Email  string `json:"email"`
	Role   string `json:"role"`
	jwt.RegisteredClaims
}

type Item struct {
	SKU      string  `json:"sku"`
	Name     string  `json:"name"`
	ImageURL string  `json:"image_url"`
	Qty      int     `json:"qty"`
	Price    float64 `json:"price"`
	Sum      float64 `json:"sum"`
}

type CartResponse struct {
	Items      []Item  `json:"items"`
	TotalQty   int     `json:"total_qty"`
	TotalPrice float64 `json:"total_price"`
}

type UpdateRequest struct {
	SKU string `json:"sku"`
	Qty int    `json:"qty"`
}

func main() {
	var err error

	pool, err = pgxpool.New(
		context.Background(),
		"postgres://postgres:140701@localhost:5432/basket_service",
	)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	mux := http.NewServeMux()
	mux.Handle("/api/cart", authMiddleware(http.HandlerFunc(cartHandler)))
	mux.Handle("/api/add", authMiddleware(http.HandlerFunc(addHandler)))
	mux.Handle("/api/update", authMiddleware(http.HandlerFunc(updateHandler)))
	mux.Handle("/api/remove", authMiddleware(http.HandlerFunc(removeHandler)))
	mux.Handle("/api/clear", authMiddleware(http.HandlerFunc(clearHandler)))

	fs := http.FileServer(http.Dir("static"))
	mux.Handle("/", fs)

	log.Println("Basket server starting on :9001")
	log.Fatal(http.ListenAndServe(":9001", withCORS(mux)))
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

func cartHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID, ok := getUserID(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	rows, err := pool.Query(r.Context(), `
		SELECT sku, name, image_url, quantity, price_at_add
		FROM cart_items
		WHERE user_id = $1
		ORDER BY sku
	`, userID)
	if err != nil {
		log.Println("cart query error:", err)
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	items := []Item{}
	totalQty := 0
	totalPrice := 0.0

	for rows.Next() {
		var item Item
		if err := rows.Scan(&item.SKU, &item.Name, &item.ImageURL, &item.Qty, &item.Price); err != nil {
			log.Println("cart scan error:", err)
			http.Error(w, "scan error", http.StatusInternalServerError)
			return
		}

		item.Sum = item.Price * float64(item.Qty)
		totalQty += item.Qty
		totalPrice += item.Sum
		items = append(items, item)
	}

	if err := rows.Err(); err != nil {
		log.Println("rows error:", err)
		http.Error(w, "rows error", http.StatusInternalServerError)
		return
	}

	resp := CartResponse{
		Items:      items,
		TotalQty:   totalQty,
		TotalPrice: totalPrice,
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(resp)
}

func addHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	userID, ok := getUserID(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	var item Item
	if err := json.NewDecoder(r.Body).Decode(&item); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}

	_, err := pool.Exec(r.Context(), `
		INSERT INTO cart_items (user_id, sku, name, image_url, quantity, price_at_add)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (user_id, sku)
		DO UPDATE SET
			quantity = cart_items.quantity + EXCLUDED.quantity,
			name = EXCLUDED.name,
			image_url = EXCLUDED.image_url,
			price_at_add = EXCLUDED.price_at_add
	`, userID, item.SKU, item.Name, item.ImageURL, item.Qty, item.Price)
	if err != nil {
		log.Println("ADD DB ERROR:", err)
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusCreated)
	fmt.Fprint(w, `{"ok":true}`)
}

func updateHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID, ok := getUserID(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	var req UpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}

	if req.SKU == "" {
		http.Error(w, "sku required", http.StatusBadRequest)
		return
	}

	if req.Qty <= 0 {
		result, err := pool.Exec(r.Context(), `
			DELETE FROM cart_items
			WHERE user_id = $1 AND sku = $2
		`, userID, req.SKU)
		if err != nil {
			log.Println("delete in update error:", err)
			http.Error(w, "db error", http.StatusInternalServerError)
			return
		}

		if result.RowsAffected() == 0 {
			http.Error(w, "item not found", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		fmt.Fprintln(w, `{"status":"removed"}`)
		return
	}

	result, err := pool.Exec(r.Context(), `
		UPDATE cart_items
		SET quantity = $1
		WHERE user_id = $2 AND sku = $3
	`, req.Qty, userID, req.SKU)
	if err != nil {
		log.Println("update error:", err)
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}

	if result.RowsAffected() == 0 {
		http.Error(w, "item not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	fmt.Fprintln(w, `{"status":"updated"}`)
}

func removeHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID, ok := getUserID(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	sku := r.URL.Query().Get("sku")
	if sku == "" {
		http.Error(w, "sku required", http.StatusBadRequest)
		return
	}

	result, err := pool.Exec(r.Context(), `
		DELETE FROM cart_items
		WHERE user_id = $1 AND sku = $2
	`, userID, sku)
	if err != nil {
		log.Println("remove error:", err)
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}

	if result.RowsAffected() == 0 {
		http.Error(w, "item not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	fmt.Fprintln(w, `{"status":"removed"}`)
}

func clearHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID, ok := getUserID(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	_, err := pool.Exec(r.Context(), `
		DELETE FROM cart_items
		WHERE user_id = $1
	`, userID)
	if err != nil {
		log.Println("clear error:", err)
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	fmt.Fprintln(w, `{"status":"cleared"}`)
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "http://localhost:9000")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}
