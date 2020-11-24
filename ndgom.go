package ndgom

import (
	"encoding/json"
	"reflect"

	"github.com/ppp225/ndgo"
)

// Get makes db query and unmarshals results as array
func Get(txn *ndgo.Txn, predicate, value string, result interface{}) (err error) {
	dgType := getDgType(result)
	resp, err := ndgo.Query{}.GetPredExpandType("q", "eq", predicate, value, "", "", "uid dgraph.type", dgType).Run(txn)
	if err != nil {
		return err
	}
	return json.Unmarshal(ndgo.Unsafe{}.FlattenRespToArray(resp.GetJson()), &result)
}

// GetOne makes db query and unmarshals first result as object
func GetOne(txn *ndgo.Txn, idPredicate, idValue string, result interface{}) (err error) {
	dgType := getDgType(result)
	resp, err := ndgo.Query{}.GetPredExpandType("q", "eq", idPredicate, idValue, ",first:1", "", "uid dgraph.type", dgType).Run(txn)
	if err != nil {
		return err
	}
	return json.Unmarshal(ndgo.Unsafe{}.FlattenRespToObject(resp.GetJson()), &result)
}

// getDgType gets dgtype tag value of Type field of given object
func getDgType(obj interface{}) string {
	t := reflect.TypeOf(obj).Elem()
	switch t.Kind() {
	case reflect.Struct:
		return parseTagDgType(t)
	case reflect.Slice:
		t = t.Elem()
		if t.Kind() != reflect.Struct {
			panic("not supported")
		}
		return parseTagDgType(t)
	default:
		panic("not supported")
	}
}

// parseTagDgType gets dgtype tag value of Type field of given struct
func parseTagDgType(t reflect.Type) string {
	field, ok := t.FieldByName("Type")
	if ok {
		tag, ok := field.Tag.Lookup("dgtype")
		if ok {
			return tag
		}
	}
	return "_all_"
}
