package ndgom

import "fmt"

// This file groups common elements for all ndgom APIs
// Each API is hosted in it's own separate file, see them for implementation details:
// easy.go - all abstractions, easiest to use
// simple.go - a few abstractions, gives control over transactions to user
// stateless.go - minimal abstractions, gives nearly full control over what's happening

// Common Errors
var (
	ErrNotExist = fmt.Errorf("object of requested type and uid does not exist")
)

// User Errors
var (
	ErrWrongInput = fmt.Errorf("input needs to be *ptr")
)

// Stateless API Errors. Don't need to be handled in higher level APIs
var (
	// ErrUpsertUID happens when running upsert with wrong uid set in struct. Methods: Upd
	ErrUpsertUID = fmt.Errorf("uid of object must be set to uid(U)")
)
