package ndgom

import (
	"context"

	"github.com/dgraph-io/dgo"
	"github.com/dgraph-io/dgo/protos/api"
)

// Admin groups helpers with non common administrative uses, to keep API clean
// Usage: ndgom.Admin{}.
type Admin struct{}

// DropDB does indeed nuke all DB data, returning it to a virgin state
func (Admin) DropDB(dg *dgo.Dgraph) error {
	return dg.Alter(context.Background(), &api.Operation{DropAll: true})
}

// MigrateSchema runs schema migration
func (Admin) MigrateSchema(dg *dgo.Dgraph, schema string) error {
	return dg.Alter(context.Background(), &api.Operation{Schema: schema})
}
