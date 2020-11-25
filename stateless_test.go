package ndgom_test

import (
	"log"
	"testing"

	"github.com/dgraph-io/dgo"
	"github.com/ppp225/ndgo"
	"github.com/ppp225/ndgom"
	"github.com/stretchr/testify/require"
)

func addNewElement(t *testing.T, dg *dgo.Dgraph) (uid string) {
	var err error
	// add new element
	txn := ndgo.NewTxnWithoutContext(dg.NewTxn())
	defer txn.Discard()

	s := testStruct{
		Name: firstName,
		Attr: firstAttr,
	}

	err = ndgom.New(txn, &s)
	require.NoError(t, err)

	// log.Println(resp)
	err = txn.Commit()
	require.NoError(t, err)

	require.NotEmpty(t, s.UID)
	require.Equal(t, "0x", s.UID[:2])
	return s.UID
}

func TestStatelessUpd(t *testing.T) {
	var err error
	dg := dgNewClient()
	defer setupTeardown(dg)()

	uid1 := addNewElement(t, dg)

	txn := ndgo.NewTxnWithoutContext(dg.NewTxn())
	defer txn.Discard()

	upd1 := testStruct{
		UID:  "uid(U)",
		Name: "updatedName",
	}

	err = ndgom.Stateless{}.Upd(txn, uid1, testType, upd1)
	require.NoError(t, err)

	err = txn.Commit()
	require.NoError(t, err)

	// query db and compare result
	txn = ndgo.NewTxnWithoutContext(dg.NewTxn())
	defer txn.Discard()

	expected := testStruct{
		UID:  "",
		Type: []string{testType},
		Name: "updatedName",
		Attr: firstAttr,
	}
	actual := testStruct{}
	err = ndgom.GetByID(txn, uid1, &actual)
	require.NoError(t, err)
	actual.UID = ""
	log.Print(uid1)
	log.Print(actual)
	require.Exactly(t, expected, actual)
}
