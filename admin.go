package ndgom

import (
	"context"

	"github.com/dgraph-io/dgo"
	"github.com/dgraph-io/dgo/protos/api"
)

// Admin groups helpers with non common administrative uses, to keep API clean
// Usage: ndgom.Admin{}.
type Admin struct{}

// MigrateSchema runs schema migration
func (Admin) MigrateSchema(dg *dgo.Dgraph, dgSchema string) (err error) {
	ctx := context.Background()
	err = dg.Alter(ctx, &api.Operation{
		Schema: dgSchema,
	})
	return err
}
