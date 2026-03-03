package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
)

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
	// Создание пула соединений
	pool, err := pgxpool.New(context.Background(), "postgres://postgres:140701@localhost:5432/Products")
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	rows, err := pool.Query(context.Background(), `
		SELECT sku, name, brand, description, price, image_url, quantity 
		FROM products 
		ORDER BY sku
	`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	products := []Product{}
	for rows.Next() {
		var p Product
		err := rows.Scan(&p.SKU, &p.Name, &p.Brand, &p.Description, &p.Price, &p.ImageURL, &p.Quantity)
		if err != nil {
			log.Fatal(err)
		}
		products = append(products, p)
	}
	if rows.Err() != nil {
		log.Fatal(rows.Err())
	}

	jsonData, err := json.MarshalIndent(products, "", "  ")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("\nТовары в JSON:")
	fmt.Println(string(jsonData))
}
