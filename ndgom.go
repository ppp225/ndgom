package ndgom

import (
	"encoding/json"

	"github.com/ppp225/ndgo"
)

// Get makes db query and unmarshals results as array
func Get(txn *ndgo.Txn, dgType, predicate, value string, result interface{}) (err error) {
	resp, err := ndgo.Query{}.GetPredExpandType("q", "eq", predicate, value, "", "", "uid dgraph.type", dgType).Run(txn)
	if err != nil {
		return err
	}
	return json.Unmarshal(ndgo.Unsafe{}.FlattenRespToArray(resp.GetJson()), &result)
}

// GetOne makes db query and unmarshals first result as object
func GetOne(txn *ndgo.Txn, dgType, idPredicate, idValue string, result interface{}) (err error) {
	resp, err := ndgo.Query{}.GetPredExpandType("q", "eq", idPredicate, idValue, ",first:1", "", "uid dgraph.type", dgType).Run(txn)
	if err != nil {
		return err
	}
	return json.Unmarshal(ndgo.Unsafe{}.FlattenRespToObject(resp.GetJson()), &result)
}
