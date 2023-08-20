package config

import (
	"encoding/json"
	"io"
)

type AZStorageConfig struct {
	AccountName   string `json:"account_name"`
	AccountKey    string `json:"account_key"`
	ContainerName string `json:"container_name"`
}

// NewFromReader returns a new azure-storage-cli configuration struct from the contents of reader.
// reader.Read() is expected to return valid JSON
func NewFromReader(reader io.Reader) (AZStorageConfig, error) {
	bytes, err := io.ReadAll(reader)
	if err != nil {
		return AZStorageConfig{}, err
	}
	config := AZStorageConfig{}

	err = json.Unmarshal(bytes, &config)
	if err != nil {
		return AZStorageConfig{}, err
	}

	return config, nil
}
