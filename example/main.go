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

type DbElement struct {
	UID  string   `json:"uid,omitempty"`
	Type []string `json:"dgraph.type,omitempty" dgtype:"Element"`
	Name string   `json:"elementName,omitempty"`
}

func main() {
	dg := dgNewClient()

	// recreate db
	err := ndgom.Admin{}.DropDB(dg)
	if err != nil {
		panic(err)
	}
	schema := `
	<elementName>: string @index(hash) @upsert .

	type Element {
		elementName: string
	  }
	`
	err = ndgom.Admin{}.MigrateSchema(dg, schema)
	if err != nil {
		panic(err)
	}

	// add new element
	txn := ndgo.NewTxnWithoutContext(dg.NewTxn())
	defer txn.Discard()
	new1 := DbElement{
		Name: "Test",
	}
	err = ndgom.New(txn, &new1)
	if err != nil {
		panic(err)
	}

	new2 := DbElement{
		Name: "Test",
	}
	err = ndgom.New(txn, &new2)
	if err != nil {
		panic(err)
	}
	// log.Println(resp)
	txn.Commit()

	// query
	txn = ndgo.NewTxnWithoutContext(dg.NewTxn())
	defer txn.Discard()

	var e []DbElement
	err = ndgom.Get(txn, "elementName", "Test", &e)
	if err != nil {
		panic(err)
	}
	log.Println(e)

	var e2 DbElement
	err = ndgom.GetOne(txn, "elementName", "Test", &e2)
	if err != nil {
		panic(err)
	}
	log.Println(e2)

	var e3 DbElement
	err = ndgom.GetByID(txn, new2.UID, &e3)
	if err != nil {
		panic(err)
	}
	log.Println(e3)

}
