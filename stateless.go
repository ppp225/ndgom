package ndgom

import (
	"encoding/json"

	"github.com/ppp225/ndgo"
)

// Stateless groups stateless lower abstraction helpers. Does not use magic like reflection.
// Usage: ndgom.Stateless{}.
type Stateless struct{}

// GetByID makes db query by uid and unmarshals result as object
func (Stateless) GetByID(txn *ndgo.Txn, uid, dgTypes string, result interface{}) (err error) {
	resp, err := ndgo.Query{}.GetUIDExpandType("q", "uid", uid, "", "", "uid dgraph.type", dgTypes).Run(txn)
	if err != nil {
		return err
	}
	return json.Unmarshal(ndgo.Unsafe{}.FlattenRespToObject(resp.GetJson()), &result)
}

// Get makes db query and unmarshals results as array
func (Stateless) Get(txn *ndgo.Txn, predicate, value, dgTypes string, result interface{}) (err error) {
	resp, err := ndgo.Query{}.GetPredExpandType("q", "eq", predicate, value, "", "", "uid dgraph.type", dgTypes).Run(txn)
	if err != nil {
		return err
	}
	return json.Unmarshal(ndgo.Unsafe{}.FlattenRespToArray(resp.GetJson()), &result)
}

// GetOne makes db query and unmarshals first result as object
func (Stateless) GetOne(txn *ndgo.Txn, predicate, value, dgTypes string, result interface{}) (err error) {
	resp, err := ndgo.Query{}.GetPredExpandType("q", "eq", predicate, value, ",first:1", "", "uid dgraph.type", dgTypes).Run(txn)
	if err != nil {
		return err
	}
	return json.Unmarshal(ndgo.Unsafe{}.FlattenRespToObject(resp.GetJson()), &result)
}

// New creates new node and returns uid map of created node(s)
func (Stateless) New(txn *ndgo.Txn, obj interface{}) (uid map[string]string, err error) {
	resp, err := txn.Seti(obj)
	return resp.GetUids(), err
}
