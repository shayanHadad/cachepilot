package store

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
)

// EnsureIndexes creates any indexes the posts collection needs beyond
// MongoDB's default _id index.
//
// As of v1, the service only performs point-lookups by _id, and
// MongoDB indexes _id automatically on every collection — so this
// function currently has nothing to add. It exists as an explicit
// extension point: if a future query type is added (e.g. filtering
// by author_id or tags), the corresponding index should be created
// here rather than left implicit, so "which indexes does this
// service depend on" stays answerable by reading one function instead
// of being scattered or forgotten.
func EnsureIndexes(ctx context.Context, coll *mongo.Collection) error {
	// No additional indexes needed yet — see the doc comment above.
	//
	// Example of how a future index would be added here:
	//
	//   _, err := coll.Indexes().CreateOne(ctx, mongo.IndexModel{
	//       Keys: bson.D{{Key: "author_id", Value: 1}},
	//   })
	//   if err != nil {
	//       return fmt.Errorf("store: failed to create author_id index: %w", err)
	//   }

	_ = ctx
	_ = coll
	return nil
}
