package config

import (
	"os"
	"sync"

	"github.com/deejross/mydis/pkg/cluster"
	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
)

var (
	mu   = sync.RWMutex{}
	conf *cluster.Config
)

// Get reads in the configuration from the config file and returns a Config object.
// The result is cached for future calls, and cache is invalidated automatically if
// the config file is modified.
func Get() (*cluster.Config, error) {
	mu.RLock()
	if conf != nil {
		mu.RUnlock()
		return conf, nil
	}
	mu.RUnlock()

	if err := viper.ReadInConfig(); err != nil {
		return nil, err
	}

	config := &cluster.Config{}
	if err := viper.Unmarshal(config); err != nil {
		return nil, err
	}

	// determine the ListenPort if not set in config file
	if len(config.HTTPPort) == 0 {
		if port := os.Getenv("PORT"); len(port) > 0 {
			config.HTTPPort = port
		} else {
			config.HTTPPort = "8000"
		}
	}

	Set(config)
	return config, nil
}

// Set the current configuration. Will be overriden if config file changes.
// This is mostly used for unit/integration testing.
func Set(config *cluster.Config) {
	mu.Lock()
	conf = config
	mu.Unlock()
}

func init() {
	homeDir, _ := os.UserHomeDir()
	defaultConfigDir := homeDir + "/.mydis"

	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("/etc/mydis/")
	viper.AddConfigPath(".")
	viper.AddConfigPath(defaultConfigDir)
	viper.AutomaticEnv()
	viper.WatchConfig()
	viper.OnConfigChange(func(e fsnotify.Event) {
		mu.Lock()
		conf = nil
		mu.Unlock()
	})
}
