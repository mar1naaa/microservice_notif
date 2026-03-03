package main

import (
	"html/template"
	"log"
	"net/http"
)

type Product struct {
	Name     string
	ImageURL string
}

type PageData struct {
	Products []Product
}

func main() {
	// 1) mux — маршрутизатор
	mux := http.NewServeMux()

	// 2) Раздача статических файлов:
	// URL:  /static/...
	// Disk: ./uploads/...
	fs := http.FileServer(http.Dir("./uploads"))
	mux.Handle("/static/", http.StripPrefix("/static/", fs))

	// 3) Главная страница (HTML)
	tmpl := template.Must(template.ParseFiles("./templates/index.html"))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		data := PageData{
			Products: []Product{
				{Name: "Товар 1", ImageURL: "/static/products/bronz.jpg"},
			},
		}
		_ = tmpl.Execute(w, data)
	})

	// 4) Запуск сервера
	addr := ":8080"
	log.Println("Server started on http://localhost" + addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}
