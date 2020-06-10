package main

import (
	"context"
	"encoding/json"
	"fmt"
	schema "github.com/micrexIT/phi-architecture-example-protobuf"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
)

type Event struct {
	Name    string      `json:"name"`
	Version string      `json:"version"`
	Payload interface{} `json:"payload"`
}

type PublisherServer struct {
	schema.UnimplementedPublisherServer
}

func main() {
	port := 50052
	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption

	grpcServer := grpc.NewServer(opts...)
	schema.RegisterPublisherServer(grpcServer, newServer())
	grpcServer.Serve(lis)
}

func newServer() *PublisherServer {
	s := &PublisherServer{}
	return s
}

func (i *PublisherServer) PublishProduct(ctx context.Context, watched *schema.ProductWatched) (*schema.Status, error) {
	event := Event{
		Name:    "product_watched",
		Version: "v1.0",
		Payload: struct {
			Product string `json:"product"`
			Visitor string `json:"visitor"`
		}{
			Product: watched.GetProduct(),
			Visitor: watched.GetVisitor(),
		},
	}
	msg, err := createMessage(&event)

	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	err = pushMessage(ctx, msg)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	return &schema.Status{Status: 1}, nil
}

func (i *PublisherServer) PublishCustomer(ctx context.Context, bought *schema.CustomerBoughtMany) (*schema.Status, error) {
	for _, customerBought := range (*bought).CustomerBoughtMany {
		event := Event{
			Name:    "product_bought",
			Version: "v1.0",
			Payload: struct {
				Product  string `json:"product"`
				Customer string `json:"customer"`
			}{
				Product:  customerBought.GetProduct(),
				Customer: customerBought.GetCustomer(),
			},
		}
		msg, err := createMessage(&event)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}

		err = pushMessage(ctx, msg)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
	}

	return &schema.Status{Status: 1}, nil
}

func createMessage(event *Event) (*kafka.Message, error) {
	value, err := json.Marshal(*event)
	if err != nil {
		return nil, err
	}
	msg := kafka.Message{
		Value: value,
	}
	return &msg, nil
}

func pushMessage(ctx context.Context, msg *kafka.Message) error {
	kafkaWriter := getKafkaWriter()
	return kafkaWriter.WriteMessages(ctx, *msg)
}

func getKafkaWriter() *kafka.Writer {
	eventStore, ok := os.LookupEnv("EVENT_STORE")
	if !ok {
		eventStore = "event-store:9092"
	}

	topic, ok := os.LookupEnv("TOPIC")
	if !ok {
		topic = "events"
	}
	return kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{eventStore},
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	})
}
