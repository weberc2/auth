package main

import (
	"log"
)

func main() {
	c, err := LoadConfig()
	if err != nil {
		log.Fatalf("loading config: %v", err)
	}

	if err := c.Run(); err != nil {
		log.Fatal(err)
	}
}
