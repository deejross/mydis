package logger

import (
	"os"
	"strings"

	"github.com/hashicorp/go-hclog"
)

// New returns a new scoped logger with defaults that formats output based on the `LOG_OUTPUT`
// environment variable. Options include `console` (default) and `json`.
func New(name string) hclog.Logger {
	conf := *hclog.DefaultOptions
	output := strings.ToLower(os.Getenv("LOG_OUTPUT"))
	if output == "json" {
		conf.JSONFormat = true
	}

	level := strings.ToUpper(os.Getenv("LOG_LEVEL"))
	if len(level) == 0 {
		level = "DEBUG"
	}
	conf.Level = hclog.LevelFromString(level)
	conf.Name = name

	return hclog.New(&conf)
}
