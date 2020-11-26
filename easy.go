package ndgom

import (
	"context"
	"reflect"
	"time"

	"github.com/dgraph-io/dgo"
	"github.com/ppp225/ndgo"
)

var (
	dg         *dgo.Dgraph
	txnTimeout = 60 * time.Second
)

// Easy groups Easy{}.API methods, the easiest one to use.
// Usage: ndgom.Easy{}. ...
type Easy struct{}

// Init initializes dgraph instance with a maximum txn timeout.
// If timeout is 0, default value of 60 seconds will be used.
func (Easy) Init(d *dgo.Dgraph, timeout time.Duration) {
	dg = d
	if timeout > 0 {
		txnTimeout = timeout
	}
}

// GetByID makes db query by uid and unmarshals result as object
func (Easy) GetByID(result interface{}) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), txnTimeout)
	defer cancel()
	txn := ndgo.NewTxn(ctx, dg.NewTxn())
	defer txn.Discard()

	uid := getUID(result)
	return Simple{}.GetByID(txn, uid, result)
}

func getUID(obj interface{}) (uid string) {
	t := reflect.TypeOf(obj).Elem()
	// validate if fields exist how we need them
	if err := fieldsOK(t); err != nil {
		panic(err)
	}
	// validate and set uid
	uid = reflect.ValueOf(obj).Elem().FieldByName("UID").String()
	if len(uid) < 3 || uid[:2] != "0x" {
		panic("uid is required and should have format 0x123")
	}
	return uid
}
