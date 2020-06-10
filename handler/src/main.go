package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/MicrexIT/neo4j-driver-client"
	_ "github.com/micrexIT/phi-architecture-example-protobuf"
	"github.com/segmentio/kafka-go"
	"log"
	"os"
)

type Event struct {
	Name    string  `json:"name"`
	Version string  `json:"version"`
	Payload Payload `json:"payload"`
}

type Payload struct {
	Product  string `json:"product"`
	Customer string `json:"customer"`
	Visitor  string `json:"visitor"`
}

func main() {
	reader := kafkaReader()
	defer reader.Close()

	fmt.Println("start consuming ...")
	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println("start Reading msg ...")
		fmt.Print(string(msg.Value))
		if err := HandleMessage(&msg); err != nil {
			fmt.Println("error handling message")
		}
	}

}

func kafkaReader() *kafka.Reader {
	eventStore, topic, _, _, _ := environmentVariables()

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{eventStore},
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
	fmt.Println("returning kafka reader", r)
	return r
}

func HandleMessage(m *kafka.Message) error {
	event := decodeMsg(m)
	switch event.Name {
	case "product_bought":
		return productBought(event) // split in two: more meaningful; updateCust + updateProduct
	case "product_watched":
		return productWatched(event)
	default:
		fmt.Println("Unknown event: \n")
		fmt.Printf("%v", *event)
		return nil
	}
}

func productWatched(event *Event) error {
	_, _, boltUrl, username, password := environmentVariables()


	visitor := event.Payload.Visitor
	if visitor == "" {
		visitor = "anonymous"
	}

	query := fmt.Sprintf(`
			MERGE (p: Person {name: "%s"})
			MERGE (pt:Product {name: "%s"})
			MERGE (p)-[w:WATCHED]->(pt)
            ON MATCH SET w.items = w.items + 1
            ON CREATE SET w.items = 1
			RETURN p.name as customer, pt.name as product, w.items as watched`, visitor, event.Payload.Product)

	job := func(record neo4j.Record) error {
		return nil
	}

	client := neo4j.NewClient(
		boltUrl,
		username,
		password,
	)

	return client.Write(query, job)
}

func productBought(event *Event) error {
	_, _, boltUrl, username, password := environmentVariables()

	customer := event.Payload.Customer
	if customer == "" {
		customer = "anonymous"
	}

	query := fmt.Sprintf(`
		MERGE (p: Person {name: "%s"})
		MERGE (pt:Product {name: "%s"})
		MERGE (p)-[b:BOUGHT]->(pt) 
		ON MATCH SET b.items = b.items + 1 
		ON CREATE SET b.items = 1
		RETURN p.name as customer , pt.name as product, b.items as bought `,
		customer,
		event.Payload.Product,
	)

	job := func(record neo4j.Record) error {
		return nil
	}

	client := neo4j.NewClient(
		boltUrl,
		username,
		password,
	)

	return client.Write(query, job)
}

func decodeMsg(m *kafka.Message) *Event {
	event := Event{}
	err := json.Unmarshal(m.Value, &event)
	if err != nil {
		log.Fatalln(err)
	}

	return &event
}

func environmentVariables() (eventStore string, topic string, boltUrl string, username string, password string) {
	eventStore, ok := os.LookupEnv("EVENT_STORE")
	if !ok {
		eventStore = "event-store:9092"
	}

	topic, ok = os.LookupEnv("TOPIC")
	if !ok {
		topic = "events"
	}

	boltUrl, ok = os.LookupEnv("ENTITY_STORE")
	if !ok {
		boltUrl = "entity-store:7687"
	}

	username, ok = os.LookupEnv("ENTITY_STORE_USERNAME")
	if !ok {
		username = "neo4j"
	}

	password, ok = os.LookupEnv("ENTITY_STORE_PASSWORD")
	if !ok {
		username = "qwerqwer"
	}

	return

}
