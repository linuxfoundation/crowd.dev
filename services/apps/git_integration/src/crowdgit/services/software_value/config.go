package main

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
)

type DBConfig struct {
	User     string `koanf:"user"`
	Password string `koanf:"password"`
	DBName   string `koanf:"dbname"`
	Host     string `koanf:"host"`
	Port     int    `koanf:"port"`
	SSLMode  string `koanf:"sslmode"`
	PoolMax  int    `koanf:"pool_max"`
	ReadOnly bool   `koanf:"readonly"`
}

type Config struct {
	TargetPath       string   `koanf:"target.path"`
	SCCPath          string   `koanf:"scc.path"`
	InsightsDatabase DBConfig `koanf:"database.insights"`
}

func getConfig(targetPath string) (Config, error) {
	var config Config
	var err error

	// Load configuration from command line argument and environment variables
	config.TargetPath = targetPath
	// SCC path will be auto-discovered via exec.LookPath("scc")

	if _, err := os.Stat(config.TargetPath); os.IsNotExist(err) {
		return config, fmt.Errorf("target path '%s' does not exist: %w", config.TargetPath, err)
	} else if err != nil {
		return config, fmt.Errorf("error checking target path '%s': %w", config.TargetPath, err)
	}

	// Auto-discover scc binary in PATH (should be at /usr/local/bin/scc in container)
	config.SCCPath, err = exec.LookPath("scc")
	if err != nil {
		return config, fmt.Errorf("could not find 'scc' executable in PATH: %w", err)
	}

	config.InsightsDatabase.User = os.Getenv("INSIGHTS_DB_USERNAME")
	config.InsightsDatabase.Password = os.Getenv("INSIGHTS_DB_PASSWORD")
	config.InsightsDatabase.DBName = os.Getenv("INSIGHTS_DB_DATABASE")
	config.InsightsDatabase.Host = os.Getenv("INSIGHTS_DB_WRITE_HOST")
	if portStr := os.Getenv("INSIGHTS_DB_PORT"); portStr != "" {
		if port, err := strconv.Atoi(portStr); err == nil {
			config.InsightsDatabase.Port = port
		}
	}
	config.InsightsDatabase.SSLMode = os.Getenv("INSIGHTS_DB_SSLMODE")
	if poolMaxStr := os.Getenv("INSIGHTS_DB_POOL_MAX"); poolMaxStr != "" {
		if poolMax, err := strconv.Atoi(poolMaxStr); err == nil {
			config.InsightsDatabase.PoolMax = poolMax
		}
	}
	if readOnlyStr := os.Getenv("INSIGHTS_DB_READONLY"); readOnlyStr != "" {
		config.InsightsDatabase.ReadOnly = readOnlyStr == "true"
	}

	return config, nil
}
