package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	writer := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "test-topic",
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	msg := fmt.Sprintf("hello from go at %s", time.Now().Format("2006-01-02 15:04:05"))

	err := writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte("demo-key"),
		Value: []byte(msg),
	})
	if err != nil {
		log.Fatal("ошибка отправки сообщения: ", err)
	}

	fmt.Println("Сообщение отправлено в Kafka:", msg)
}
