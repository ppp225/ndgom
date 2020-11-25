package ndgom_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/dgraph-io/dgo"
	"github.com/dgraph-io/dgo/protos/api"
	"google.golang.org/grpc"
)

type testStruct struct {
	UID  string      `json:"uid,omitempty"`
	Type []string    `json:"dgraph.type,omitempty" dgtype:"TestType"`
	Name string      `json:"testName,omitempty"`
	Attr string      `json:"testAttribute,omitempty"`
	Edge *testStruct `json:"testEdge,omitempty"`
}

const (
	predicateName = "testName"
	predicateAttr = "testAttribute"
	predicateEdge = "testEdge"
	firstName     = "first"
	secondName    = "second"
	thirdName     = "third"
	fourthName    = "4444"
	firstAttr     = "attribute"
	secondAttr    = "attributer"
	thirdAttr     = "attributest"
	fourthAttr    = "40404"
	testType      = "TestType"
)

// dgNewClient creates new *dgo.Dgraph Client
func dgNewClient() *dgo.Dgraph {
	conn, err := grpc.Dial("localhost:9080", grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	// defer conn.Close()
	return dgo.NewDgraphClient(
		api.NewDgraphClient(conn),
	)
}

// Usage: defer setupTeardown(dg)()
func setupTeardown(dg *dgo.Dgraph) func() {
	// Setup
	dgAddSchema(dg)

	// Teardown
	return func() {
		dgDropTestPredicates(dg)
	}
}

func dgAddSchema(dg *dgo.Dgraph) {
	ctx := context.Background()
	err := dg.Alter(ctx, &api.Operation{
		Schema: `
		<testName>: string @index(hash) @upsert .
		<testAttribute>: string .
		<testEdge>: [uid] .

		type TestType {
			testName: string
			testAttribute: string
			testEdge: uid
		  }
		`,
	})
	if err != nil {
		log.Fatal(err)
	}
}

func dgDropTestPredicates(dg *dgo.Dgraph) {
	ctx := context.Background()
	retries := 5
	for { // retry, as sometimes it races with txn.Discard. Err: "rpc error: code = Unknown desc = Pending transactions found. Please retry operation"
		err := dg.Alter(ctx, &api.Operation{
			DropAttr: predicateName,
		})
		if err != nil {
			retries--
			fmt.Printf("dgDropTestPredicates (retries left: %d) error: %+v \n", retries, err)
			time.Sleep(time.Millisecond * 101)
			if retries <= 0 {
				log.Fatalf("dgDropTestPredicates (has run out of retries) last error: %+v \n", err)
			}
			continue
		}
		break
	}
	err := dg.Alter(ctx, &api.Operation{
		DropAttr: predicateAttr,
	})
	if err != nil {
		log.Fatal(err)
	}
	err = dg.Alter(ctx, &api.Operation{
		DropAttr: predicateEdge,
	})
	if err != nil {
		log.Fatal(err)
	}
}
