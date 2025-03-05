package types

import "time"

type AdCappingRule struct {
	ID         int       `json:"id"`
	Base       string    `json:"base"`
	BaseValues int       `json:"base_values"`
	Status     int       `json:"status"`
	Count      int       `json:"count"`
	Duration   string    `json:"duration"`
	Created    time.Time `json:"created"`
}

type HourlyStats struct {
	Hour  int   `json:"hour"`
	Count int64 `json:"count"`
}
