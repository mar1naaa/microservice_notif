package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"golang.org/x/crypto/bcrypt"
)

var pool *pgxpool.Pool
var kafkaWriter *kafka.Writer
var jwtKey = []byte("my_secret_key")

type User struct {
	ID           int64     `json:"id"`
	Name         string    `json:"name"`
	Email        string    `json:"email"`
	PasswordHash string    `json:"-"`
	Role         string    `json:"role"`
	CreatedAt    time.Time `json:"created_at"`
}

type RegisterRequest struct {
	Name     string `json:"name"`
	Email    string `json:"email"`
	Password string `json:"password"`
}

type LoginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type LoginResponse struct {
	Token string      `json:"token"`
	User  UserProfile `json:"user"`
}

type UserProfile struct {
	ID    int64  `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
	Role  string `json:"role"`
}

type Claims struct {
	UserID int64  `json:"user_id"`
	Name   string `json:"name"`
	Email  string `json:"email"`
	Role   string `json:"role"`
	jwt.RegisteredClaims
}

type LoginEvent struct {
	EventType string    `json:"event_type"`
	UserID    int64     `json:"user_id"`
	Name      string    `json:"name"`
	Email     string    `json:"email"`
	Role      string    `json:"role"`
	CreatedAt time.Time `json:"created_at"`
}

type RegisterEvent struct {
	EventType string    `json:"event_type"`
	UserID    int64     `json:"user_id"`
	Name      string    `json:"name"`
	Email     string    `json:"email"`
	Role      string    `json:"role"`
	CreatedAt time.Time `json:"created_at"`
}

type LoginFailedEvent struct {
	EventType string    `json:"event_type"`
	Email     string    `json:"email"`
	Reason    string    `json:"reason"`
	CreatedAt time.Time `json:"created_at"`
}

func main() {
	var err error

	kafkaWriter = &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "test-topic",
		Balancer: &kafka.LeastBytes{},
	}
	defer kafkaWriter.Close()

	pool, err = pgxpool.New(
		context.Background(),
		"postgres://postgres:140701@localhost:5432/users",
	)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	mux := http.NewServeMux()

	mux.HandleFunc("/api/register", regHandler)
	mux.HandleFunc("/api/login", logHandler)
	mux.Handle("/api/me", authMiddleware(http.HandlerFunc(meHandler)))

	fs := http.FileServer(http.Dir("static"))
	mux.Handle("/", fs)

	log.Println("Auth service starting on :9003")
	log.Fatal(http.ListenAndServe(":9003", withCORS(mux)))
}

func publishRegisterEvent(ctx context.Context, userID int64, user RegisterRequest) error { /////////////
	event := RegisterEvent{
		EventType: "user.registered",
		UserID:    userID,
		Name:      user.Name,
		Email:     user.Email,
		Role:      "user",
		CreatedAt: time.Now(),
	}
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	return kafkaWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(fmt.Sprintf("%d", userID)),
		Value: data,
	})
}

func publishLoginEvent(ctx context.Context, userID int64, user User) error { /////////////
	event := LoginEvent{
		EventType: "user.logged_in",
		UserID:    userID,
		Name:      user.Name,
		Email:     user.Email,
		Role:      user.Role,
		CreatedAt: time.Now(),
	}
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	return kafkaWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(fmt.Sprintf("%d", userID)),
		Value: data,
	})
}

func publishLoginFailedEvent(ctx context.Context, email, reason string) error { /////////////
	event := LoginFailedEvent{
		EventType: "user.login_failed",
		Email:     email,
		Reason:    reason,
		CreatedAt: time.Now(),
	}
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	return kafkaWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(email),
		Value: data,
	})
}

func regHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	var user RegisterRequest

	if err := json.NewDecoder(r.Body).Decode(&user); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}

	var exists bool
	err := pool.QueryRow(r.Context(),
		`SELECT EXISTS(SELECT 1 FROM users WHERE email = $1)`,
		user.Email,
	).Scan(&exists)

	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}

	if exists {
		http.Error(w, "email already exists", http.StatusConflict)
		return
	}

	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(user.Password), bcrypt.DefaultCost)
	if err != nil {
		http.Error(w, "hash error", http.StatusInternalServerError)
		return
	}

	var userID int64
	err = pool.QueryRow(r.Context(), `
    INSERT INTO users (name, email, password_hash)
    VALUES ($1, $2, $3)
    RETURNING id
`, user.Name, user.Email, string(hashedPassword)).Scan(&userID)

	if err != nil {
		log.Println("ADD DB ERROR:", err)
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}

	if err := publishRegisterEvent(r.Context(), userID, user); err != nil { ///////////
		log.Println("kafka publish error:", err)
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusCreated)
	fmt.Fprint(w, `{"ok":true}`)
}

func logHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var req LoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	var user User
	err := pool.QueryRow(r.Context(), `
        SELECT id, name, email, password_hash, role, created_at
        FROM users
        WHERE email = $1
    `, req.Email).Scan(
		&user.ID,
		&user.Name,
		&user.Email,
		&user.PasswordHash,
		&user.Role,
		&user.CreatedAt,
	)
	if err != nil {
		if err := publishLoginFailedEvent(r.Context(), req.Email, "user_not_found"); err != nil {
			log.Println("kafka publish error:", err)
		}
		http.Error(w, "invalid email or password", http.StatusUnauthorized)
		return
	}

	err = bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.Password))
	if err != nil {
		if err := publishLoginFailedEvent(r.Context(), req.Email, "wrong_password"); err != nil {
			log.Println("kafka publish error:", err)
		}
		http.Error(w, "invalid email or password", http.StatusUnauthorized)
		return
	}

	token, err := generateJWT(user)
	if err != nil {
		http.Error(w, "token error", http.StatusInternalServerError)
		return
	}

	response := LoginResponse{
		Token: token,
		User: UserProfile{
			ID:    user.ID,
			Name:  user.Name,
			Email: user.Email,
			Role:  user.Role,
		},
	}

	if err := publishLoginEvent(r.Context(), user.ID, user); err != nil { ///////////
		log.Println("kafka publish error:", err)
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(response)
}

func generateJWT(user User) (string, error) {
	expirationTime := time.Now().Add(24 * time.Hour)

	claims := &Claims{
		UserID: user.ID,
		Name:   user.Name,
		Email:  user.Email,
		Role:   user.Role,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(expirationTime),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(jwtKey)
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

func meHandler(w http.ResponseWriter, r *http.Request) {
	claims, ok := r.Context().Value("claims").(*Claims)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	profile := UserProfile{
		ID:    claims.UserID,
		Name:  claims.Name,
		Email: claims.Email,
		Role:  claims.Role,
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(profile)
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
