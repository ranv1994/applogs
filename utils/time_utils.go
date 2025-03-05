package utils

import "time"

// TimeUtils handles all time-related operations
type TimeUtils struct{}

func NewTimeUtils() *TimeUtils {
	return &TimeUtils{}
}

// RoundToTenMinutes rounds timestamp to nearest 10-minute interval
func (t *TimeUtils) RoundToTenMinutes(timestamp int64) int64 {
	return timestamp - (timestamp % 600)
}

// GetDateTimestamps returns start and end timestamps for a given date
func (t *TimeUtils) GetDateTimestamps(dateStr string) (int32, int32, error) {
	date, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		return 0, 0, err
	}

	startOfDay := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC)
	endOfDay := startOfDay.Add(24 * time.Hour).Add(-1 * time.Second)

	return int32(startOfDay.Unix()), int32(endOfDay.Unix()), nil
}

// GetTimeRangesForDay returns all 10-minute interval timestamps for a day
func (t *TimeUtils) GetTimeRangesForDay() []int32 {
	now := time.Now().UTC()
	startOfDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)

	var timestamps []int32
	for i := 0; i < 144; i++ {
		ts := startOfDay.Add(time.Duration(i*10) * time.Minute)
		timestamps = append(timestamps, int32(ts.Unix()))
	}
	return timestamps
}
