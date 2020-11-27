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
	log.SetLevel(log.DEBUG)
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

// GetByID makes db query by uid and unmarshals result as object
func (Easy) Get(result interface{}) (err error) {
	// pre
	if err = validateInput(result); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), txnTimeout)
	defer cancel()
	txn := ndgo.NewTxn(ctx, dg.NewTxn())
	defer txn.Discard()

	// get values from fields, and construct query based on them
	f := getPopulatedFields(result)
	// check if any fields are populated
	if len(f) == 0 {
		return fmt.Errorf("need to specify at least one struct field for Get")
	}

	// check if uid query possible
	uid, ok := f["uid"]
	if ok {
		if len(f) == 1 {
			return Simple{}.GetByID(txn, uid, result)
		}

		filters := make([]string, 0, len(f)-1)
		for k, v := range f {
			if k == "uid" {
				continue
			}
			filters = append(filters, fmt.Sprintf(`eq(%s, "%s")`, k, v))
		}
		filter := "@filter(" + strings.Join(filters, " AND ") + ")" // final format is "@filter(eq(fieldName, fieldVal) AND eq(f2,v2) ...)"

		dgType := getDgType(result)
		resp, err := ndgo.Query{}.GetUIDExpandType("q", "uid", uid, ",first: 1", filter, "uid dgraph.type", dgType).Run(txn)
		if err != nil {
			return err
		}
		return json.Unmarshal(ndgo.Unsafe{}.FlattenRespToObject(resp.GetJson()), &result)
	}

	// query
	if len(f) != 1 {
		return fmt.Errorf("One field must be populated for Get to work. Get on multiple fields not implemented yet")
	}
	for k, v := range f {
		return Simple{}.GetOne(txn, k, v, result)
	}
	panic("internal error: should not have gotten here")
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

func getPopulatedFields(obj interface{}) (fields map[string]string) {
	fields = make(map[string]string)
	// validate and set uid
	v := reflect.ValueOf(obj).Elem()
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
			log.Debugf("searching on slice not implemented")
		case reflect.String:
			s = f.String()
		default:
			panic("not implemented search for field of Kind " + ft.Kind().String())
		}

		if s != "" {
			fields[predicateName] = s
		}
	}
	// log.Debugf("---\n")
	return fields
}
