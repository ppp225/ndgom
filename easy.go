package ndgom

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/dgraph-io/dgo"
	log "github.com/ppp225/lvlog"
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

// Debug enables logging of debug information, like ignored fields during parsing etc.
// Uses the default std logger
func Debug() {
	log.SetLevel(log.NORM | log.DEBUG)
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

// Get makes db query and populates result with found value or values.
// If result is struct, returns first result. If is slice, returns all found results.
func (Easy) Get(result interface{}) (err error) {
	// pre
	if err = validateInput(result); err != nil {
		return err
	}
	kind := getKind(result)
	ctx, cancel := context.WithTimeout(context.Background(), txnTimeout)
	defer cancel()
	txn := ndgo.NewTxn(ctx, dg.NewTxn())
	defer txn.Discard()

	// get values from fields, and construct query based on them
	f := getPopulatedFields(result, kind)
	// check if any fields are populated
	if len(f) == 0 {
		return fmt.Errorf("need to specify at least one struct field for Get")
	}

	// check if uid query possible
	uid, ok := f["uid"]
	if ok {
		// this is basically identical to GetByID, but with filter and checking result Kind
		filter := ""
		if len(f) > 1 {
			filters := make([]string, 0, len(f)-1)
			for k, v := range f {
				if k == "uid" {
					continue
				}
				filters = append(filters, fmt.Sprintf(`eq(%s, %s)`, k, v))
			}
			filter = "@filter(" + strings.Join(filters, " AND ") + ")" // final format is "@filter(eq(fieldName, fieldVal) AND eq(f2,v2) ...)"
		}
		dgType := getDgType(result)
		resp, err := ndgo.Query{}.GetUIDExpandType("q", "uid", uid, ",first: 1", filter, "uid dgraph.type", dgType).Run(txn)
		if err != nil {
			return err
		}
		switch kind {
		case reflect.Struct:
			return json.Unmarshal(ndgo.Unsafe{}.FlattenRespToObject(resp.GetJson()), &result)
		case reflect.Slice:
			return json.Unmarshal(ndgo.Unsafe{}.FlattenRespToArray(resp.GetJson()), &result)
		}
	}

	// query
	if len(f) != 1 {
		return fmt.Errorf("One field must be populated for Get to work. Get on multiple fields not implemented yet")
	}
	for k, v := range f {
		switch kind {
		case reflect.Struct:
			return Simple{}.GetOne(txn, k, v, result)
		case reflect.Slice:
			return Simple{}.Get(txn, k, v, result)
		}
	}
	panic("internal error: should not have gotten here")
}

// New creates new node. Do not set UID.
// Type can be set, if node should have multiple, and must contain dgtype.
func (Easy) New(obj interface{}) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), txnTimeout)
	defer cancel()
	txn := ndgo.NewTxn(ctx, dg.NewTxn())
	defer txn.Discard()

	err = Simple{}.New(txn, obj)
	if err != nil {
		return err
	}
	return txn.Commit()
}

// Upd updates node based on uid and changed fields, and unmarshals updated result into supplied obj
func (Easy) Upd(obj interface{}) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), txnTimeout)
	defer cancel()
	txn := ndgo.NewTxn(ctx, dg.NewTxn())
	defer txn.Discard()

	err = Simple{}.Upd(txn, obj)
	if err != nil {
		return err
	}
	return txn.Commit()
}

// --------------------------------------- helpers ---------------------------------------

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

func getKind(obj interface{}) reflect.Kind {
	t := reflect.TypeOf(obj).Elem()
	switch t.Kind() {
	case reflect.Struct:
		return reflect.Struct
	case reflect.Slice:
		if t.Elem().Kind() != reflect.Struct {
			panic("not supported")
		}
		return reflect.Slice
	default:
		panic("not supported")
	}
}

func getPopulatedFields(obj interface{}, kind reflect.Kind) (fields map[string]string) {
	fieldValues := make(map[string][]string)

	switch kind {
	case reflect.Struct:
		insertPopulatedFieldsOfSingleStructIntoMap(obj, &fieldValues)
	case reflect.Slice:
		s := reflect.ValueOf(obj).Elem()
		for i := 0; i < s.Len(); i++ {
			insertPopulatedFieldsOfSingleStructIntoMap(s.Index(i), &fieldValues)
		}
	}

	// transforms array to quoted string i.e. ["val1" "val4" "some other val"], which is parsed correctly by dgraph
	fields = make(map[string]string)
	for k, v := range fieldValues {
		if k == "uid" { // uid is special case of course, must be without []
			fields[k] = strings.Join(v, ",")
			continue
		}
		fields[k] = fmt.Sprintf("%q", v)
	}
	return fields
}

func insertPopulatedFieldsOfSingleStructIntoMap(obj interface{}, fieldValues *map[string][]string) {
	// special case
	// when iterating over slice elements, we already have the elements as reflect.Value
	v, ok := obj.(reflect.Value)
	if !ok {
		v = reflect.ValueOf(obj).Elem()
	}

	vt := v.Type()
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		ft := f.Type()
		vtf := vt.Field(i)
		tag, ok := vtf.Tag.Lookup("json")
		if !ok {
			log.Debugf("ndgom.Get.getPopulatedFields: skipping field without json tag") // TODO: document all log.Debugf
			continue
		}
		predicateName := strings.Split(tag, ",")[0]
		// log.Debugf("Field: %s\tType: %v\tKind: %v\tValue: %v\tJsonFieldName:%v\n", vtf.Name, ft, ft.Kind(), f.Interface(), predicateName)

		s := ""
		switch ft.Kind() {
		case reflect.Slice:
			log.Debugf("ndgom.Get.getPopulatedFields: skipping slice field - not implemented")
		case reflect.String:
			s = f.String()
		default:
			log.Debugf("ndgom.Get.getPopulatedFields: skipping field od kind %s - not implemented", ft.Kind().String())
		}

		if s != "" {
			(*fieldValues)[predicateName] = append((*fieldValues)[predicateName], s)
		}
	}
	// log.Debugf("---\n")
}
