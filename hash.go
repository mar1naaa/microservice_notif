package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"golang.org/x/crypto/bcrypt"
)

func main() {
	reader := bufio.NewReader(os.Stdin)

	fmt.Print("Введите пароль: ")
	password, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Ошибка чтения:", err)
		return
	}

	password = strings.TrimSpace(password)

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		fmt.Println("Ошибка хеширования:", err)
		return
	}

	fmt.Println("Bcrypt hash:")
	fmt.Println(string(hash))
}
