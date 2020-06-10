package main

import (
	"fmt"
	"net"
	"os"

	"github.com/MicrexIT/neo4j-driver-client"

	schema "github.com/MicrexIT/phi-architecture-example-protobuf"
	"google.golang.org/grpc"

	"log"
)

type InspectorServer struct {
	schema.UnimplementedInspectorServer
}

func main() {
	fmt.Println("Service booting...")
	port := 50051
	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	schema.RegisterInspectorServer(grpcServer, newServer())
	err = grpcServer.Serve(lis)
	if err != nil {
		log.Fatalf("failed to serve grpc: %v", err)
	}
}

func newServer() *InspectorServer {
	s := &InspectorServer{}
	return s
}

func (i *InspectorServer) InspectProduct(_ *schema.Empty, stream schema.Inspector_InspectProductServer) error {
	query := `MATCH (:Person)-[b:BOUGHT|:WATCHED]->(pp:Product) RETURN pp.name as name , sum(b.items) as bought, count(b) - count(b.items)  as watched`
	inspect := func(record neo4j.Record) error {
		product := schema.Product{}
		product.Name = record["name"].(string)
		product.Bought = record["bought"].(int64)
		product.Watched = record["watched"].(int64)

		if err := stream.Send(&product); err != nil {
			fmt.Println("Error", err)
			return err
		}
		return nil
	}

	boltUrl, username, password := environmentVariables()
	client := neo4j.NewClient(
		boltUrl,
		username,
		password,
	)

	return client.Read(query, inspect)

}

func (i *InspectorServer) InspectCustomer(_ *schema.Empty, stream schema.Inspector_InspectCustomerServer) error {
	query := `MATCH (p:Person)-[b:BOUGHT]->(pp:Product) RETURN p.name as name , sum(b.items) as products`
	inspect := func(record neo4j.Record) error {
		customer := schema.Customer{}
		customer.Name = record["name"].(string)
		customer.Products = record["products"].(int64)
		if err := stream.Send(&customer); err != nil {
			fmt.Println("Error", err)
			return err
		}
		return nil
	}

	boltUrl, username, password := environmentVariables()
	client := neo4j.NewClient(
		boltUrl,
		username,
		password,
	)

	return client.Read(query, inspect)
}

func environmentVariables() ( boltUrl string, username string, password string) {
	boltUrl, ok := os.LookupEnv("ENTITY_STORE")
	if !ok {
		boltUrl = "entity-store:7687"
	}

	username, ok = os.LookupEnv("ENTITY_STORE_USERNAME")
	if !ok {
		username = "neo4j"
	}

	password, ok = os.LookupEnv("ENTITY_STORE_PASSWORD")
	if !ok {
		password = "qwerqwer"
	}

	return

}
