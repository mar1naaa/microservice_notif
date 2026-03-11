package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

var pool *pgxpool.Pool

const cartID = "550e8400-e29b-41d4-a716-446655440000"

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
	mux.HandleFunc("/api/cart", cartHandler)
	mux.HandleFunc("/api/add", addHandler)
	mux.HandleFunc("/api/update", updateHandler)
	mux.HandleFunc("/api/remove", removeHandler)

	fs := http.FileServer(http.Dir("static"))
	mux.Handle("/", fs)

	log.Println("Basket server starting on :9001")
	log.Fatal(http.ListenAndServe(":9001", withCORS(mux)))
}

func cartHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	rows, err := pool.Query(r.Context(), `
		SELECT sku, name, image_url, quantity, price_at_add
		FROM cart_items
		WHERE cart_id = $1
		ORDER BY sku
	`, cartID)
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
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	var item Item
	if err := json.NewDecoder(r.Body).Decode(&item); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}

	_, err := pool.Exec(r.Context(), `
		INSERT INTO cart_items (cart_id, sku, name, image_url, quantity, price_at_add)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (cart_id, sku)
		DO UPDATE SET
			quantity = cart_items.quantity + EXCLUDED.quantity,
			name = EXCLUDED.name,
			image_url = EXCLUDED.image_url
	`, cartID, item.SKU, item.Name, item.ImageURL, item.Qty, item.Price)
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
			WHERE cart_id = $1 AND sku = $2
		`, cartID, req.SKU)
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
		WHERE cart_id = $2 AND sku = $3
	`, req.Qty, cartID, req.SKU)
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

	sku := r.URL.Query().Get("sku")
	if sku == "" {
		http.Error(w, "sku required", http.StatusBadRequest)
		return
	}

	result, err := pool.Exec(r.Context(), `
		DELETE FROM cart_items
		WHERE cart_id = $1 AND sku = $2
	`, cartID, sku)
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

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "http://localhost:9000")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}
