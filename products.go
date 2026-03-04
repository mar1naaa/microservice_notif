package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

var pool *pgxpool.Pool

type Product struct {
	SKU         string  `json:"sku" db:"sku"`
	Name        string  `json:"name" db:"name"`
	Brand       string  `json:"brand" db:"brand"`
	Description string  `json:"description" db:"description"`
	Price       float64 `json:"price" db:"price"`
	ImageURL    string  `json:"image_url" db:"image_url"`
	Quantity    int     `json:"quantity" db:"quantity"`
}

func main() {
	var err error
	pool, err = pgxpool.New(context.Background(), "postgres://postgres:140701@localhost:5432/Products")
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/products", productsHandler)
	fs := http.FileServer(http.Dir("static"))
	mux.Handle("/", fs)

	log.Println("Server starting on :9000")
	http.ListenAndServe(":9000", mux)
}

func productsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	rows, err := pool.Query(context.Background(), `
		SELECT sku, name, brand, description, price, image_url, quantity 
		FROM products 
		ORDER BY sku
	`)
	if err != nil {
		log.Println(err)
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()
	products := []Product{}
	for rows.Next() {
		var p Product
		if err := rows.Scan(&p.SKU, &p.Name, &p.Brand, &p.Description, &p.Price, &p.ImageURL, &p.Quantity); err != nil {
			log.Println(err)
			http.Error(w, "scan error", http.StatusInternalServerError)
			return
		}
		products = append(products, p)
	}
	json.NewEncoder(w).Encode(products)
}
