package config

import "testing"

// validConfig returns a configuration that passes Validate.
func validConfig() Config {
	return Config{
		Server:    ServerConfig{Port: 8080},
		Mongo:     MongoConfig{URI: "mongodb://localhost:27017", DB: "cachepilot"},
		Cache:     CacheConfig{Policy: "lru", Capacity: 250},
		MLService: MLServiceConfig{GRPCAddr: "localhost:50051", TimeoutMs: 8},
		Logging:   LoggingConfig{Path: "service.jsonl"},
	}
}

// TestValidateAcceptsEveryPolicy checks that all four policies pass.
func TestValidateAcceptsEveryPolicy(t *testing.T) {
	for _, policy := range []string{"lru", "lfu", "ml", "none"} {
		cfg := validConfig()
		cfg.Cache.Policy = policy
		if err := cfg.Validate(); err != nil {
			t.Errorf("policy %q rejected: %v", policy, err)
		}
	}
}

// TestValidateRejectsUnknownPolicy checks that a policy outside the
// list is still refused.
func TestValidateRejectsUnknownPolicy(t *testing.T) {
	cfg := validConfig()
	cfg.Cache.Policy = "fifo"
	if err := cfg.Validate(); err == nil {
		t.Fatal("policy \"fifo\" was accepted")
	}
}

// TestPolicyEnvOverride checks that CACHEPILOT_CACHE_POLICY reaches the
// configuration.
func TestPolicyEnvOverride(t *testing.T) {
	t.Setenv("CACHEPILOT_CACHE_POLICY", "none")
	cfg := validConfig()
	applyEnvOverrides(&cfg)
	if cfg.Cache.Policy != "none" {
		t.Fatalf("policy = %q, want none", cfg.Cache.Policy)
	}
}
