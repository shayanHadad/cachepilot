package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/shayanHadad/cachepilot/internal/config"
)

// ErrPostNotFound is returned by GetPost when the requested id does
// not correspond to any document in the collection. Callers (the
// cache manager) should treat this differently from other errors: it
// means "this post doesn't exist", not "the database is unreachable".
var ErrPostNotFound = errors.New("store: post not found")

// Store wraps a MongoDB connection scoped to the posts collection.
type Store struct {
	client *mongo.Client
	posts  *mongo.Collection
}

// NewStore connects to MongoDB using cfg, verifies the connection
// with a ping (failing fast if MongoDB is unreachable), and returns a
// Store scoped to the posts collection in cfg.DB.
func NewStore(ctx context.Context, cfg config.MongoConfig) (*Store, error) {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(cfg.URI))
	if err != nil {
		return nil, fmt.Errorf("store: failed to connect: %w", err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("store: failed to ping MongoDB: %w", err)
	}

	return &Store{
		client: client,
		posts:  client.Database(cfg.DB).Collection("posts"),
	}, nil
}

// postDoc mirrors the on-disk BSON shape of a document in the posts
// collection. It exists only as a decode target — never marshaled to
// JSON directly. See the DEV NOTE below for why.
type postDoc struct {
	ID            primitive.ObjectID `bson:"_id"`
	AuthorID      string             `bson:"author_id"`
	Content       string             `bson:"content"`
	CreatedAt     primitive.DateTime `bson:"created_at"`
	LikesCount    int                `bson:"likes_count"`
	CommentsCount int                `bson:"comments_count"`
	MediaSizeKB   float64            `bson:"media_size_kb"`
	Tags          []string           `bson:"tags"`
}

// postJSON is the wire/cache/log shape of a post: plain JSON types
// only, matching the schema in the project's domain model docs
// (hex-string id, Unix-ms timestamp). This is what actually gets
// cached and what data-pipeline/ and ml-service/ will consume.
type postJSON struct {
	ID            string   `json:"_id"`
	AuthorID      string   `json:"author_id"`
	Content       string   `json:"content"`
	CreatedAt     int64    `json:"created_at"`
	LikesCount    int      `json:"likes_count"`
	CommentsCount int      `json:"comments_count"`
	MediaSizeKB   float64  `json:"media_size_kb"`
	Tags          []string `json:"tags"`
	// QueryType is derived, not stored in MongoDB: "text_post" when
	// MediaSizeKB is 0, "media_post" otherwise. Computed here so
	// every consumer of this JSON (cache manager, logger, offline
	// data pipeline) sees the same classification instead of each
	// re-deriving it independently.
	QueryType string `json:"query_type"`
}

// queryTypeFor derives the QueryType classification from a post's
// media size, per the project's domain model decision (a post is a
// "media_post" if it carries any non-text payload).
func queryTypeFor(mediaSizeKB float64) string {
	if mediaSizeKB > 0 {
		return "media_post"
	}
	return "text_post"
}

// GetPost fetches the post document with the given hex-encoded
// MongoDB _id and returns its JSON-marshaled bytes, ready to be
// stored directly in the cache.
func (s *Store) GetPost(ctx context.Context, id string) ([]byte, error) {
	objID, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return nil, fmt.Errorf("store: invalid post id %q: %w", id, err)
	}

	var doc postDoc
	err = s.posts.FindOne(ctx, bson.M{"_id": objID}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, ErrPostNotFound
		}
		return nil, fmt.Errorf("store: failed to fetch post %q: %w", id, err)
	}

	out := postJSON{
		ID:            doc.ID.Hex(),
		AuthorID:      doc.AuthorID,
		Content:       doc.Content,
		CreatedAt:     doc.CreatedAt.Time().UnixMilli(),
		LikesCount:    doc.LikesCount,
		CommentsCount: doc.CommentsCount,
		MediaSizeKB:   doc.MediaSizeKB,
		Tags:          doc.Tags,
		QueryType:     queryTypeFor(doc.MediaSizeKB),
	}

	data, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("store: failed to marshal post %q: %w", id, err)
	}

	return data, nil
}

// Close disconnects the underlying MongoDB client.
func (s *Store) Close(ctx context.Context) error {
	if err := s.client.Disconnect(ctx); err != nil {
		return fmt.Errorf("store: failed to disconnect: %w", err)
	}
	return nil
}
