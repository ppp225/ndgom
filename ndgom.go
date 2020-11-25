package ndgom

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/ppp225/ndgo"
)

// GetByID makes db query by uid and unmarshals result as object
func GetByID(txn *ndgo.Txn, uid string, result interface{}) (err error) {
	if err = validateInput(result); err != nil {
		return err
	}
	dgType := getDgType(result)
	resp, err := ndgo.Query{}.GetUIDExpandType("q", "uid", uid, "", "", "uid dgraph.type", dgType).Run(txn)
	if err != nil {
		return err
	}
	return json.Unmarshal(ndgo.Unsafe{}.FlattenRespToObject(resp.GetJson()), &result)
}

// Get makes db query and unmarshals results as array
func Get(txn *ndgo.Txn, predicate, value string, result interface{}) (err error) {
	if err = validateInput(result); err != nil {
		return err
	}
	dgType := getDgType(result)
	resp, err := ndgo.Query{}.GetPredExpandType("q", "eq", predicate, value, "", "", "uid dgraph.type", dgType).Run(txn)
	if err != nil {
		return err
	}
	return json.Unmarshal(ndgo.Unsafe{}.FlattenRespToArray(resp.GetJson()), &result)
}

// GetOne makes db query and unmarshals first result as object
func GetOne(txn *ndgo.Txn, predicate, value string, result interface{}) (err error) {
	if err = validateInput(result); err != nil {
		return err
	}
	dgType := getDgType(result)
	resp, err := ndgo.Query{}.GetPredExpandType("q", "eq", predicate, value, ",first:1", "", "uid dgraph.type", dgType).Run(txn)
	if err != nil {
		return err
	}
	return json.Unmarshal(ndgo.Unsafe{}.FlattenRespToObject(resp.GetJson()), &result)
}

// New creates new node. Do not set UID or Type.
// Can set Type if multiple needed.
func New(txn *ndgo.Txn, obj interface{}) (err error) {
	if err = validateInput(obj); err != nil {
		return err
	}
	setFieldsForNew(obj)
	resp, err := txn.Seti(obj)
	if err != nil {
		return err
	}
	setRespUID(resp.Uids["new"], obj)
	return err
}

func validateInput(obj interface{}) error {
	if reflect.TypeOf(obj).Kind() != reflect.Ptr {
		return fmt.Errorf("ndgom input needs to be *pointer, but is: %s", reflect.TypeOf(obj).Kind().String())
	}
	return nil
}

// setFieldsForNew sets uid to _:new and type to dgtype tag value
// panics when uid is set or when type is set but doesn't contain dgtype value.
func setFieldsForNew(obj interface{}) {
	t := reflect.TypeOf(obj).Elem()
	// validate if fields exist how we need them
	if err := fieldsOK(t); err != nil {
		panic(err)
	}
	// validate and set uid
	uidField := reflect.ValueOf(obj).Elem().FieldByName("UID")
	if uidField.String() != "" {
		panic("uid will be set for you when creating new objects, don't create it yourself")
	}
	uidField.SetString("_:new")
	// validate and set dgType
	dgTypeField := reflect.ValueOf(obj).Elem().FieldByName("Type")
	dgTypes := dgTypeField.Interface().([]string) // is slice
	objDgType := getDgType(obj)
	if len(dgTypes) == 0 { // if no types, set required one
		// create a new slice, get first index, and set required string value
		slice := reflect.MakeSlice(reflect.TypeOf([]string{}), 1, 1)
		v := slice.Index(0)
		v.Set(reflect.ValueOf(string(objDgType)))
		dgTypeField.Set(slice)
	} else { // if some types are user supplied
		// loop them and check if required one is set
		ok := false
		for _, t := range dgTypes {
			if t == objDgType {
				ok = true
			}
		}
		if !ok {
			panic(fmt.Sprintf("Trying to set new object with incorrect types. Object type=[%s], but what is set=%v", objDgType, dgTypes))
		}
	}
}

// setRespUID sets resp uid. No need to query db, as it's exact with what was mutated.
func setRespUID(uid string, obj interface{}) {
	// validate and set uid
	uidField := reflect.ValueOf(obj).Elem().FieldByName("UID")
	uidField.SetString(uid)
}

func fieldsOK(t reflect.Type) error {
	field, ok := t.FieldByName("UID")
	if ok {
		tag, ok := field.Tag.Lookup("json")
		if !(ok && (tag == "uid,omitempty" || tag == "uid")) {
			return fmt.Errorf("Missing struct fields. Need to have UID string json:uid")
		}
	}
	field, ok = t.FieldByName("Type")
	if ok {
		tag, ok := field.Tag.Lookup("json")
		if !(ok && (tag == "dgraph.type,omitempty" || tag == "dgraph.type")) {
			return fmt.Errorf("Missing struct fields. Need to have Type string json:dgraph.type dgtype:typeName")
		}
		tag, ok = field.Tag.Lookup("dgtype")
		if !(ok && tag != "") {
			return fmt.Errorf("Missing struct fields. Need to have Type string json:dgraph.type dgtype:typeName")
		}
	}
	return nil
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
