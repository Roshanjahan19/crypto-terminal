package main

import (
	"encoding/json"
	"fmt"
	"log"

	//"net/http"

	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/gorilla/websocket"
)

// var upgrader = websocket.Upgrader{
// 	ReadBufferSize:  1024,
// 	WriteBufferSize: 1024,
// }

// func homePage(w http.ResponseWriter, r *http.Request) {

// 	fmt.Fprintf(w, "hello!! this is the responsewriter for homepage")

// }

// func listenForMessage(conn *websocket.Conn) {
// 	for {
// 		messageType, p, err := conn.ReadMessage()
// 		if err != nil {
// 			log.Print(err)
// 			return
// 		}
// 		log.Print(string(p))

// 		if err := conn.WriteMessage(messageType, p); err != nil {
// 			log.Print(err)
// 			return
// 		}
// 	}
// }

// func wsEndpoint(w http.ResponseWriter, r *http.Request) {
// 	upgrader.CheckOrigin = func(r *http.Request) bool { return true }

// 	ws, err := upgrader.Upgrade(w, r, nil)

// 	if err != nil {
// 		log.Print(err)
// 	}

// 	log.Print("client connected!!")
// 	listenForMessage(ws)

// }

// func routes() {

//		http.HandleFunc("/", homePage)
//		http.HandleFunc("/ws", wsEndpoint)
//	}

type subscribe struct {
	Type       string   `json:"type"`
	ProductIDs []string `json:"product_ids"`
	Channels   []string `json:"channels"`
}

type coinbaseMatch struct {
	Type      string `json:"type"`
	Size      string `json:"size"`
	Price     string `json:"price"`
	ProductID string `json:"product_id"`
	Time      string `json:"time"`
}

type Tick struct {
	Symbol string
	Price  float64
	Size   float64
	Time   time.Time
}

func normalizeMatch(raw coinbaseMatch) (Tick, error) {
	price, err := strconv.ParseFloat(raw.Price, 64)
	if err != nil {
		return Tick{}, err
	}

	size, err := strconv.ParseFloat(raw.Size, 64)
	if err != nil {
		return Tick{}, err
	}

	parsedTime, err := time.Parse(time.RFC3339Nano, raw.Time)
	if err != nil {
		return Tick{}, err
	}

	return Tick{
		Symbol: raw.ProductID,
		Price:  price,
		Size:   size,
		Time:   parsedTime,
	}, nil
}

func runIngestor(ctx context.Context, rdb *redis.Client) error {
	url := "wss://ws-feed.exchange.coinbase.com"

	conn, resp, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return fmt.Errorf("dial failed: %w", err)
	}
	defer conn.Close()

	log.Printf("connected to coinbase %v", resp.Status)

	sub := subscribe{
		Type:       "subscribe",
		ProductIDs: []string{"BTC-USD"},
		Channels:   []string{"matches"},
	}

	if err := conn.WriteJSON(sub); err != nil {
		return fmt.Errorf("subscribe failed: %w", err)
	}

	for {
		_, p, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("read failed: %w", err)
		}

		var typeCheck struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(p, &typeCheck); err != nil {
			log.Printf("failed to check message type: %v", err)
			continue
		}

		if typeCheck.Type != "match" && typeCheck.Type != "last_match" {
			continue
		}

		var raw coinbaseMatch
		if err := json.Unmarshal(p, &raw); err != nil {
			log.Printf("failed to unmarshal match: %v", err)
			continue
		}

		tick, err := normalizeMatch(raw)
		if err != nil {
			log.Printf("failed to normalize tick: %v", err)
			continue
		}
		tickJSON, _ := json.Marshal(tick)
		rdb.XAdd(ctx, &redis.XAddArgs{Stream: "ticks:BTC-USD", Values: map[string]interface{}{"data": tickJSON}})
		rdb.Publish(ctx, "ticks:live:BTC-USD", tickJSON)

		log.Printf("%+v", tick)
	}
}

func main() {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()

	backoff := 1 * time.Second
	maxBackoff := 30 * time.Second

	for {
		err := runIngestor(ctx, rdb)
		log.Printf("ingestor stopped: %v", err)
		log.Printf("reconnecting in %v...", backoff)
		time.Sleep(backoff)

		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

//achievement = live persistent connection of real market ticks (no polling)
