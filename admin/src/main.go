package main

import (
	"fmt"
	"github.com/MicrexIT/neo4j-driver-client"
	"github.com/gin-gonic/gin"
	neo4jLib "github.com/neo4j/neo4j-go-driver/neo4j"
	"io"
	"math/rand"

	// "io/ioutil"
	"log"
	"os"
)

// Customer is a customer of the shop
type Customer struct {
	Name     string `json:"name"`
	Products int64  `json:"products"`
}

type Product struct {
	Name    string `json:"name"`
	Bought  int64  `json:"bought"`
	Watched int64  `json:"watched"`
}

// GraphResponse is the graph response
type GraphResponse struct {
	Nodes []Node `json:"nodes"`
	Edges []Edge `json:"edges"`
}

// Node is the graph response node
type Node struct {
	Title string `json:"title"`
	Id    int64  `json:"id"`
	Label string `json:"label"`
}

// Edge is the graph response edge
type Edge struct {
	Source int64  `json:"source"`
	Target int64  `json:"target"`
	Type   string `json:"type"`
	Items  int64  `json:"items"`
	Id     int64  `json:"id"`
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	r := gin.Default()
	r.Use(CORSMiddleware())
	r.Use(gin.Logger())
	r.GET("/graph", func(c *gin.Context) {

		url, username, password := environmentVariables()
		client := neo4j.NewClient(
			url,
			username,
			password,
		)

		product := Product{}
		customer := Customer{}

		graphResp := GraphResponse{}

		query := `
			MATCH (p:Product)<-[e]-(c:Person)
			RETURN p.name as product, c.name as customer, e as edge`

		job := func(record neo4j.Record) error {
			product.Name = record["product"].(string)
			customer.Name = record["customer"].(string)
			relationship := record["edge"].(neo4jLib.Relationship)

			productIndex := graphResp.findOrCreateNode(product.Name, "product")
			customerIndex := graphResp.findOrCreateNode(customer.Name, "customer")

			_ = graphResp.createEdge(customerIndex, productIndex, relationship.Type(), relationship.Props(), rand.Int63())
			return nil
		}

		err := client.Read(query, job)
		c.JSON(200, graphResp)
		if err != nil && err != io.EOF {
			log.Println("error querying graph:", err)
			return
		} else if len(graphResp.Nodes) == 0 {
			fmt.Println("no nodes")
			return
		}

	})
	r.Run(":8080")

}

func (g *GraphResponse) findOrCreateNode(name string, label string) int64 {
	var targetIndex int64 = -1
	for i, node := range g.Nodes {
		if name == node.Title && node.Label == label {
			targetIndex = int64(i)
			break
		}
	}

	if targetIndex == -1 {
		targetIndex = int64(len(g.Nodes))
		g.Nodes = append(g.Nodes, Node{Title: name, Id: targetIndex, Label: label})
	}

	return targetIndex
}

func (g *GraphResponse) createEdge(sourceIndex int64, targetIndex int64, edgeType string, properties map[string]interface{}, id int64) error {
	var items int64 = 0
	if edgeType == "BOUGHT" {
		items = properties["items"].(int64)
	}
	e := Edge{Source: sourceIndex, Target: targetIndex, Type: edgeType, Items: items, Id: id}
	g.Edges = append(g.Edges, e)
	return nil
}

func CORSMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS, GET, PUT")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	}
}

func environmentVariables() (boltUrl string, username string, password string) {
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
