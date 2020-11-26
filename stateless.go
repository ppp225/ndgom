package ndgom

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/ppp225/ndgo"
)

// Stateless groups stateless lower abstraction helpers.
// Does not use magic like reflection.
// Each helper does one db operation.
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
func (Stateless) New(txn *ndgo.Txn, obj interface{}) (uidMap map[string]string, err error) {
	resp, err := txn.Seti(obj)
	return resp.GetUids(), err
}

// Upd updates node of specified uid.
// Updated object should have set uid to `uid(U)`. Actual uid to update should be in the method.
// Doesn't result in complete updated object! (like Stateless{}.Get/New does)
func (Stateless) Upd(txn *ndgo.Txn, uid, dgTypes string, obj interface{}) (err error) {
	jsonBytes, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	// check if obj has uid set to correctly work with upsert
	if !bytes.Contains(jsonBytes, []byte(`"uid":"uid(U)"`)) {
		return fmt.Errorf("ndgom.Stateless{}.Upd: %w", ErrUpsertUID)
	}
	// construct upsert
	q := fmt.Sprintf(`
	query {
	  U as q(func: uid(%s)) @filter(eq(dgraph.type, %s)) {
	    uid
	    dgraph.type
	    expand(_all_)
	  }
	}`, uid, dgTypes)
	// only update if uid of specified type found
	cond := "@if(eq(len(U), 1))"
	resp, err := txn.DoSetb(q, cond, jsonBytes, nil)
	if err != nil {
		return err
	}
	// check if obj of requested uid/type existed. If it didn't, query result will be "q":[{}]
	existingObj := ndgo.Unsafe{}.FlattenRespToObject(resp.GetJson())
	if len(existingObj) == 2 {
		return fmt.Errorf("ndgom.Stateless{}.Upd: %w", ErrNotExist)
	}
	return nil
}
