package utils

import (
	"encoding/json"
	"strings"
)

// CappingUtils handles ad capping related operations
type CappingUtils struct{}

func NewCappingUtils() *CappingUtils {
	return &CappingUtils{}
}

type BaseRule struct {
	Company int    `json:"company,omitempty"`
	Type    int    `json:"type,omitempty"`
	AdID    int    `json:"ad_id,omitempty"`
	AdIDs   string `json:"ad_ids,omitempty"`
}

// ParseBaseRule parses JSON string into BaseRule struct
func (c *CappingUtils) ParseBaseRule(ruleStr string) (*BaseRule, error) {
	var rule BaseRule
	if err := json.Unmarshal([]byte(ruleStr), &rule); err != nil {
		return nil, err
	}
	return &rule, nil
}

// ValidateDuration checks if the duration is valid
func (c *CappingUtils) ValidateDuration(duration string) bool {
	if duration == "not" {
		return true
	}

	parts := strings.Split(duration, "_")
	if len(parts) != 2 {
		return false
	}

	// Add more validation as needed
	return true
}
