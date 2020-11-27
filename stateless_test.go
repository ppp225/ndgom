package ndgom_test

import (
	"testing"

	"github.com/dgraph-io/dgo"
	"github.com/ppp225/ndgo"
	"github.com/ppp225/ndgom"
	"github.com/stretchr/testify/require"
)

func slAddNewElement(t *testing.T, dg *dgo.Dgraph) (uid string) {
	var err error
	txn := ndgo.NewTxnWithoutContext(dg.NewTxn())
	defer txn.Discard()

	s := testStruct{
		UID:  "_:new",
		Type: []string{testType},
		Name: firstName,
		Attr: firstAttr,
	}

	uidMap, err := ndgom.Stateless{}.New(txn, &s)
	require.NoError(t, err)
	err = txn.Commit()
	require.NoError(t, err)

	newUID := uidMap["new"]
	require.NotEmpty(t, newUID)
	require.Equal(t, "0x", newUID[:2])
	return newUID
}

// makes expected.UID database query and checks if actual == expected
func slValidateIfElementMatchesDatabase(t *testing.T, dg *dgo.Dgraph, expected *testStruct) {
	var err error
	txn := ndgo.NewTxnWithoutContext(dg.NewTxn())
	defer txn.Discard()

	actual := testStruct{}
	err = ndgom.Stateless{}.GetByID(txn, expected.UID, testType, &actual)
	require.NoError(t, err)
	require.Exactly(t, *expected, actual)
}

func TestSlUpd(t *testing.T) {
	// pre
	var err error
	dg := dgNewClient()
	defer setupTeardown(dg)()
	// add element with default values
	uid1 := slAddNewElement(t, dg)

	// update one field
	txn := ndgo.NewTxnWithoutContext(dg.NewTxn())
	defer txn.Discard()

	upd1 := testStruct{
		UID:  "uid(U)",
		Name: "updatedName",
	}
	err = ndgom.Stateless{}.Upd(txn, uid1, testType, &upd1)
	require.NoError(t, err)
	err = txn.Commit()
	require.NoError(t, err)

	// check if updated correctly
	expected := testStruct{
		UID:  uid1,
		Type: []string{testType},
		Name: "updatedName",
		Attr: firstAttr,
	}
	slValidateIfElementMatchesDatabase(t, dg, &expected)
}

func TestSlUpdErr(t *testing.T) {
	// pre
	var err error
	dg := dgNewClient()
	defer setupTeardown(dg)()
	// add element with default values
	uid1 := slAddNewElement(t, dg)

	// ErrUpsertUID
	txn := ndgo.NewTxnWithoutContext(dg.NewTxn())
	defer txn.Discard()
	upd1 := testStruct{
		UID:  "0x123",
		Name: "updatedName",
	}
	err = ndgom.Stateless{}.Upd(txn, uid1, testType, upd1)
	require.ErrorIs(t, err, ndgom.ErrUpsertUID)

	// ErrNotExist
	txn = ndgo.NewTxnWithoutContext(dg.NewTxn())
	defer txn.Discard()
	upd2 := testStruct{
		UID:  "uid(U)",
		Name: "updatedName",
	}
	err = ndgom.Stateless{}.Upd(txn, uid1, "SomeOtherTypeThatDoesNotExist", upd2)
	require.ErrorIs(t, err, ndgom.ErrNotExist)

	// marshal err
	txn = ndgo.NewTxnWithoutContext(dg.NewTxn())
	defer txn.Discard()
	err = ndgom.Stateless{}.Upd(txn, uid1, testType, make(chan int))
	require.Errorf(t, err, "json: unsupported type: chan int")

	txn = ndgo.NewTxnWithoutContext(dg.NewTxn())
	txn.Discard()
	err = ndgom.Stateless{}.Upd(txn, uid1, testType, upd2)
	require.ErrorIs(t, err, dgo.ErrFinished)
}
