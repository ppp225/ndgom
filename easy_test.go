package ndgom_test

import (
	"testing"

	"github.com/dgraph-io/dgo"
	"github.com/ppp225/ndgom"
	"github.com/stretchr/testify/require"
)

var ea = ndgom.Easy{}

func eaAddNewElement(t *testing.T, dg *dgo.Dgraph) (s1 testStruct) {
	s1 = testStruct{
		UID:  "",
		Type: []string{testType},
		Name: firstName,
		Attr: firstAttr,
	}

	err := ea.New(&s1)
	require.NoError(t, err)

	require.NotEmpty(t, s1.UID)
	require.Equal(t, "0x", s1.UID[:2])
	return s1
}

// makes expected.UID database query and checks if actual == expected
func eaValidateIfElementMatchesDatabase(t *testing.T, dg *dgo.Dgraph, expected *testStruct) {
	var err error

	actual := testStruct{UID: expected.UID}
	err = ea.GetByID(&actual)
	require.NoError(t, err)
	require.Exactly(t, *expected, actual)
}

func TestEaNew(t *testing.T) {
	// pre
	dg := dgNewClient()
	defer setupTeardown(dg)()
	ea.Init(dg, 0)
	// add element with default values
	s1 := eaAddNewElement(t, dg)

	// check if added correctly
	expected := testStruct{
		UID:  s1.UID,
		Type: []string{testType},
		Name: firstName,
		Attr: firstAttr,
	}
	eaValidateIfElementMatchesDatabase(t, dg, &expected)
}

func TestEaNewErr(t *testing.T) {
	// pre
	dg := dgNewClient()
	defer setupTeardown(dg)()
	ea.Init(dg, 0)
	// add element with error values
	s1 := testStruct{
		UID:  "",
		Type: []string{testType},
		Name: firstName,
		Attr: firstAttr,
	}

	err := ea.New(s1)
	require.ErrorIs(t, err, ndgom.ErrWrongInput)

	err = ea.New("I am a proud string, that will go down in history as the one that tested this one use case no one cares about")
	require.ErrorIs(t, err, ndgom.ErrWrongInput)
}

func TestEaUpd(t *testing.T) {
	// pre
	var err error
	dg := dgNewClient()
	defer setupTeardown(dg)()
	ea.Init(dg, 0)
	// add element with default values
	s1 := eaAddNewElement(t, dg)

	upd1 := testStruct{
		UID:  s1.UID,
		Name: "updatedName",
	}
	err = ea.Upd(&upd1)
	require.NoError(t, err)

	// check if added correctly
	expected := testStruct{
		UID:  s1.UID,
		Type: []string{testType},
		Name: "updatedName",
		Attr: firstAttr,
	}
	eaValidateIfElementMatchesDatabase(t, dg, &expected)
}

func TestEaGet(t *testing.T) {
	// pre
	var err error
	dg := dgNewClient()
	defer setupTeardown(dg)()
	ea.Init(dg, 0)
	// add element with default values
	s1 := eaAddNewElement(t, dg)

	// test Get by name
	get1 := testStruct{
		Name: firstName,
	}
	ea.Get(&get1)
	require.NoError(t, err)
	require.Exactly(t, s1, get1)
}

// func TestEaGetErr(t *testing.T) {
//}
