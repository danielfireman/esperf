package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

type Config struct {
	Dict            string        `json:"dict"`
	Index           string        `json:"index"`
	Addr            string        `json:"addr"`
	ResultsPath     string        `json:"results_path"`
	ExpID           string        `json:"exp_id"`
	Timeout         time.Duration `json:"timeout"`
	Duration        time.Duration `json:"duration"`
	CollectInterval time.Duration `json:"cint"`
	Load            string        `json:"load"`
	StartTime       time.Time     `json:"start_time"`
}

func (c *Config) Write() error {
	b, err := json.MarshalIndent(c, "", "\t")
	if err != nil {
		return err
	}
	cFile, err := os.Create(filepath.Join(c.ResultsPath, "config_"+c.ExpID+".json"))
	if err != nil {
		return err
	}
	if _, err := cFile.Write(b); err != nil {
		return err
	}
	if err := cFile.Close(); err != nil {
		return err
	}
	return nil
}
