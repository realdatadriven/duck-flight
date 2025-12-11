package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Schema describes a single attachable schema block.
type Schema struct {
	Name        string `yaml:"name"`
	Description string `yaml:"description,omitempty"`
	BeforeSQL   string `yaml:"before_sql,omitempty"`
	MainSQL     string `yaml:"main_sql,omitempty"`
	AfterSQL    string `yaml:"after_sql,omitempty"`
}

// ServerConfig is the top-level configuration for the server (matches README example).
type ServerConfig struct {
	Description string   `yaml:"description,omitempty"`
	Schemas     []Schema `yaml:"schemas"`
}

// LoadConfig reads a YAML file into ServerConfig.
func LoadConfig(path string) (*ServerConfig, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open config %s: %w", path, err)
	}
	defer f.Close()

	var cfg ServerConfig
	dec := yaml.NewDecoder(f)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode yaml: %w", err)
	}
	return &cfg, nil
}
