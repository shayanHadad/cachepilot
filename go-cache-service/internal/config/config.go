package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"gopkg.in/yaml.v3"
)

// Config is the top-level configuration for the go-cache-service.
type Config struct {
	Server    ServerConfig    `yaml:"server"`
	Mongo     MongoConfig     `yaml:"mongo"`
	Cache     CacheConfig     `yaml:"cache"`
	MLService MLServiceConfig `yaml:"mlservice"`
	Logging   LoggingConfig   `yaml:"logging"`
}

// ServerConfig holds settings for the service's own HTTP listener.
type ServerConfig struct {
	// Port is the TCP port the HTTP server listens on.
	Port int `yaml:"port"`
}

// MongoConfig holds MongoDB connection settings.
type MongoConfig struct {
	URI string `yaml:"uri"`
	DB  string `yaml:"db"`
}

// CacheConfig holds cache policy and sizing settings.
type CacheConfig struct {
	// Policy selects which eviction policy the manager uses: "lru", "lfu", or "ml".
	Policy string `yaml:"policy"`
	// Capacity is the maximum number of keys the cache holds.
	Capacity int `yaml:"capacity"`
}

// MLServiceConfig holds settings for calling the ML decision service over gRPC.
type MLServiceConfig struct {
	GRPCAddr  string `yaml:"grpc_addr"`
	TimeoutMs int    `yaml:"timeout_ms"`
}

// LoggingConfig holds settings for the async JSONL request logger.
type LoggingConfig struct {
	Path string `yaml:"path"`
}

// defaults are applied for any field left unset (zero value) after
// parsing the YAML file, before env var overrides are applied.
var defaults = Config{
	Server: ServerConfig{
		Port: 8080,
	},
	Mongo: MongoConfig{
		URI: "mongodb://localhost:27017",
		DB:  "cachepilot",
	},
	Cache: CacheConfig{
		Policy:   "lru",
		Capacity: 1000,
	},
	MLService: MLServiceConfig{
		GRPCAddr:  "localhost:50051",
		TimeoutMs: 8,
	},
	Logging: LoggingConfig{
		Path: "../data/raw_logs/service.jsonl",
	},
}

// Load reads and parses the YAML config file at path, applies
// defaults for any unset fields, then applies environment variable
// overrides, and finally validates the
// result.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: failed to read %s: %w", path, err)
	}

	cfg := defaults // start from a copy of the defaults
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("config: failed to parse %s: %w", path, err)
	}

	applyEnvOverrides(&cfg)

	cfg.Logging.Path = resolveRelativeTo(path, cfg.Logging.Path)

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config: invalid configuration: %w", err)
	}

	return &cfg, nil
}

// resolveRelativeTo turns target into an absolute path. If target is
// already absolute, it's returned unchanged. Otherwise it's resolved
// relative to the directory containing configPath, not relative to
// the process's current working directory.
func resolveRelativeTo(configPath, target string) string {
	if filepath.IsAbs(target) {
		return target
	}
	return filepath.Join(filepath.Dir(configPath), target)
}

/* ============================================================
DEV NOTE (safe to delete — for learning purposes only)

Why resolve logging.path relative to config.yaml's own location
instead of just leaving it as-is (relative to the current working
directory)?

  - A relative path like "../data/raw_logs/service.jsonl" only means
    what you think it means if the process happens to be started
    from a specific directory. Run `go run ./cmd/server` from inside
    go-cache-service/, and cwd is go-cache-service/ — the log file
    ends up at go-cache-service/data/raw_logs/. Run the exact same
    binary from the repo root, or from inside an IDE that changes the
    working directory, or from inside a Docker container with a
    different WORKDIR, and the same "relative" path resolves
    somewhere completely different. This is exactly the bug that was
    hit during manual testing.
  - Resolving relative to the config file's own path instead removes
    that dependency entirely: no matter where you run the binary
    from, as long as you pass the same path to config.Load(), the log
    file always ends up in the same place relative to config.yaml.
    "Where do I run this from" stops being a variable that changes
    behavior.
  - This only had to be applied to logging.path today because it's
    the only field in Config that's a filesystem path. If more path
    fields get added later, run them through resolveRelativeTo too.
============================================================ */

// applyEnvOverrides overrides any field with the value of its
// corresponding CACHEPILOT_* environment variable, if set. This lets
// deployment environments (docker-compose, CI) override settings
// without editing config.yaml.
func applyEnvOverrides(cfg *Config) {
	if v := os.Getenv("CACHEPILOT_SERVER_PORT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Server.Port = n
		}
	}
	if v := os.Getenv("CACHEPILOT_MONGO_URI"); v != "" {
		cfg.Mongo.URI = v
	}
	if v := os.Getenv("CACHEPILOT_MONGO_DB"); v != "" {
		cfg.Mongo.DB = v
	}
	if v := os.Getenv("CACHEPILOT_CACHE_POLICY"); v != "" {
		cfg.Cache.Policy = v
	}
	if v := os.Getenv("CACHEPILOT_CACHE_CAPACITY"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Cache.Capacity = n
		}
	}
	if v := os.Getenv("CACHEPILOT_MLSERVICE_GRPC_ADDR"); v != "" {
		cfg.MLService.GRPCAddr = v
	}
	if v := os.Getenv("CACHEPILOT_MLSERVICE_TIMEOUT_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.MLService.TimeoutMs = n
		}
	}
	if v := os.Getenv("CACHEPILOT_LOGGING_PATH"); v != "" {
		cfg.Logging.Path = v
	}
}

// Validate checks that the configuration has sane values, returning
// an error describing the first problem found.
func (c *Config) Validate() error {
	if c.Server.Port <= 0 || c.Server.Port > 65535 {
		return fmt.Errorf("server.port must be between 1 and 65535, got %d", c.Server.Port)
	}

	switch c.Cache.Policy {
	case "lru", "lfu", "ml":
		// ok
	default:
		return fmt.Errorf("cache.policy must be one of lru|lfu|ml, got %q", c.Cache.Policy)
	}

	if c.Cache.Capacity <= 0 {
		return fmt.Errorf("cache.capacity must be > 0, got %d", c.Cache.Capacity)
	}

	if c.MLService.TimeoutMs < 5 || c.MLService.TimeoutMs > 10 {
		return fmt.Errorf(
			"mlservice.timeout_ms should be between 5 and 10 (per the project's fallback timeout decision), got %d",
			c.MLService.TimeoutMs,
		)
	}

	if c.Mongo.URI == "" {
		return fmt.Errorf("mongo.uri must not be empty")
	}
	if c.Mongo.DB == "" {
		return fmt.Errorf("mongo.db must not be empty")
	}
	if c.Logging.Path == "" {
		return fmt.Errorf("logging.path must not be empty")
	}

	return nil
}
