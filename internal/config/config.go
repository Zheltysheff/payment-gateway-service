package config

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Service       ServiceConfig       `yaml:"service"`
	Postgres      PostgresConfig      `yaml:"postgres"`
	Kafka         KafkaConfig         `yaml:"kafka"`
	Observability ObservabilityConfig `yaml:"observability"`
}

type ServiceConfig struct {
	Name string     `yaml:"name"`
	HTTP HTTPConfig `yaml:"http"`
}

type HTTPConfig struct {
	Port int `yaml:"port"`
}

type PostgresConfig struct {
	Url  string             `yaml:"url"`
	Pool PostgresPoolConfig `yaml:"pool"`
}

type PostgresPoolConfig struct {
	MaxConns        int32         `yaml:"max_conns"`
	MinConns        int32         `yaml:"min_conns"`
	MaxConnLifetime time.Duration `yaml:"max_conn_lifetime"`
}

type KafkaConfig struct {
	Brokers []string          `yaml:"brokers"`
	Topics  KafkaTopicsConfig `yaml:"topics"`
}

type KafkaTopicsConfig struct {
	Commands string `yaml:"commands"`
	Events   string `yaml:"events"`
}

type ObservabilityConfig struct {
	OTLP OTLPConfig `yaml:"otlp"`
}

type OTLPConfig struct {
	Endpoint string `yaml:"endpoint"`
}

func Load(path string) (Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("Read config %s: %w", path, err)
	}

	var c Config
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		return Config{}, fmt.Errorf("Parse config %s: %w", path, err)
	}

	if err := c.Validate(); err != nil {
		return Config{}, fmt.Errorf("Validate config %s: %w", path, err)
	}
	return c, nil
}

func (c Config) Validate() error {
	var errs []error
	if c.Service.Name == "" {
		errs = append(errs, errors.New("service.name is required"))
	}
	if c.Service.HTTP.Port == 0 {
		errs = append(errs, errors.New("service.http.port is required"))
	}
	if c.Postgres.Url == "" {
		errs = append(errs, errors.New("postgres.url is required"))
	}
	if len(c.Kafka.Brokers) == 0 {
		errs = append(errs, errors.New("kafka.brokers must contain at least one entry"))
	}
	if c.Kafka.Topics.Commands == "" {
		errs = append(errs, errors.New("kafka.topics.commands is required"))
	}
	if c.Observability.OTLP.Endpoint == "" {
		errs = append(errs, errors.New("observability.otlp.endpoint is required"))
	}
	return errors.Join(errs...)
}
