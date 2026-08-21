package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/shayanHadad/cachepilot/internal/cache"
	"github.com/shayanHadad/cachepilot/internal/config"
	"github.com/shayanHadad/cachepilot/internal/features"
	"github.com/shayanHadad/cachepilot/internal/logger"
	"github.com/shayanHadad/cachepilot/internal/mlclient"
	"github.com/shayanHadad/cachepilot/internal/store"
)

// logBufferSize is how many log entries can be queued before the
// async logger starts dropping them under load.
const logBufferSize = 10000

// expiryCleanupInterval controls how often Manager sweeps out stale
// TTL bookkeeping for the "ml" policy.
const expiryCleanupInterval = 30 * time.Second

func main() {
	if err := run(); err != nil {
		log.Fatalf("fatal: %v", err)
	}
}

// run does the actual work and returns an error instead of calling
// os.Exit directly.
// that.
func run() error {
	cfg, err := config.Load("config.yaml")
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	// A single context tied to process shutdown signals. Everything
	// below (Mongo connect, HTTP server lifetime) hangs off this, so
	// a Ctrl+C or SIGTERM propagates cleanly through the whole chain.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := store.NewStore(ctx, cfg.Mongo)
	if err != nil {
		return fmt.Errorf("connecting to MongoDB: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := st.Close(closeCtx); err != nil {
			log.Printf("error closing store: %v", err)
		}
	}()

	lg, err := logger.NewLogger(cfg.Logging.Path, logBufferSize)
	if err != nil {
		return fmt.Errorf("creating logger: %w", err)
	}
	defer func() {
		if err := lg.Close(); err != nil {
			log.Printf("error closing logger: %v", err)
		}
	}()

	c, err := newCache(cfg.Cache)
	if err != nil {
		return fmt.Errorf("creating cache: %w", err)
	}

	// The tracker only matters for the "ml" policy, but it's cheap to
	// build unconditionally — keeps this function's control flow
	// simpler than branching the whole wiring on policy.
	tracker := features.NewTracker(expiryCleanupInterval)
	defer tracker.Stop()

	var decider cache.Decider
	if cfg.Cache.Policy == "ml" {
		mlc, err := mlclient.NewClient(cfg.MLService.GRPCAddr)
		if err != nil {
			return fmt.Errorf("connecting to ML service: %w", err)
		}
		defer func() {
			if err := mlc.Close(); err != nil {
				log.Printf("error closing ML client: %v", err)
			}
		}()
		decider = mlc
	}

	mgr, err := cache.NewManager(
		c,
		st,
		lg,
		cfg.Cache.Policy,
		decider,
		tracker,
		time.Duration(cfg.MLService.TimeoutMs)*time.Millisecond,
		expiryCleanupInterval,
	)
	if err != nil {
		return fmt.Errorf("creating cache manager: %w", err)
	}
	defer mgr.Close()

	return serve(ctx, cfg.Server.Port, mgr)
}

// newCache builds the underlying Cache implementation for the
// configured policy. Even under the "ml" policy, Manager still needs
// a concrete Cache to store admitted values in — LRU is used as that
// underlying storage, with Manager layering ML-driven admission and
// TTL logic on top of it.
func newCache(cfg config.CacheConfig) (cache.Cache, error) {
	switch cfg.Policy {
	case "lru", "ml":
		return cache.NewLRU(cfg.Capacity), nil
	case "lfu":
		return cache.NewLFU(cfg.Capacity), nil
	default:
		// config.Validate() already rejects unknown policies, so
		// reaching this is a bug elsewhere, not a normal runtime
		// condition — but returning an error here is still cheaper
		// than a panic if that assumption ever turns out to be wrong.
		return nil, fmt.Errorf("unknown cache policy %q", cfg.Policy)
	}
}

// serve starts the HTTP server and blocks until ctx is canceled
// (e.g. by SIGINT/SIGTERM), then shuts the server down gracefully.
func serve(ctx context.Context, port int, mgr *cache.Manager) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/get", handleGet(mgr))

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	serveErrCh := make(chan error, 1)
	go func() {
		log.Printf("listening on %s", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErrCh <- err
			return
		}
		serveErrCh <- nil
	}()

	select {
	case err := <-serveErrCh:
		return err
	case <-ctx.Done():
		log.Println("shutting down...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("server shutdown: %w", err)
		}
		return nil
	}
}

// handleGet returns the HTTP handler for GET /get?key=<id>.
func handleGet(mgr *cache.Manager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "missing key query parameter", http.StatusBadRequest)
			return
		}

		value, err := mgr.Get(r.Context(), key)
		if err != nil {
			if errors.Is(err, store.ErrPostNotFound) {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			log.Printf("error handling get %q: %v", key, err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(value)
	}
}
