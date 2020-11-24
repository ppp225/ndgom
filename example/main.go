package main

import (
	"fmt"
	"io/ioutil"
	"log"

	"github.com/dgraph-io/dgo"
	"github.com/dgraph-io/dgo/protos/api"
	"github.com/ppp225/ndgo"
	"github.com/ppp225/ndgom"
	"google.golang.org/grpc"
)

const (
	dbIP = "localhost:9080"
)

func dgNewClient() *dgo.Dgraph {
	// read db ip address from db.cfg file, if it exists
	ip := dbIP
	dat, err := ioutil.ReadFile("db.cfg")
	if err == nil {
		ip = string(dat)
	}
	fmt.Printf("db.cfg ip address: '%s' | Using address: '%s'\n", string(dat), ip)
	// make db connection
	conn, err := grpc.Dial(ip, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	// defer conn.Close()
	return dgo.NewDgraphClient(
		api.NewDgraphClient(conn),
	)
}

type erement struct {
	UID  string   `json:"uid,omitempty"`
	Type []string `json:"dgraph.type,omitempty"`
	Name string   `json:"elementName,omitempty"`
}

func main() {
	dg := dgNewClient()

	// add schema
	schema := `
		<elementName>: string @index(hash) @upsert .

		type Element {
			elementName: string
		  }
		`
	err := ndgom.Admin{}.MigrateSchema(dg, schema)
	log.Println(err)

	// add new element
	txn := ndgo.NewTxnWithoutContext(dg.NewTxn())
	defer txn.Discard()
	newEl := erement{
		Type: []string{"Element"},
		Name: "Test",
	}
	resp, err := txn.Seti(newEl)
	log.Println(resp)
	log.Println(err)
	txn.Commit()

	// query
	txn = ndgo.NewTxnWithoutContext(dg.NewTxn())
	defer txn.Discard()

	var e []erement
	err = ndgom.Get(txn, "Element", "elementName", "Test", &e)
	log.Println(e)
	log.Println(err)

	var e2 erement
	err = ndgom.GetOne(txn, "Element", "elementName", "Test", &e2)
	log.Println(e2)
	log.Println(err)
}
