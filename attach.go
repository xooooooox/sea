package sea

import (
	"fmt"
	"regexp"
	"time"
)

// DatetimeUnixNano
func DatetimeUnixNano() string {
	now := time.Now()
	return fmt.Sprintf("%s %d", now.Format("2006-01-02 15:04:05"), now.UnixNano())
}

// StrIsNumber
func StrIsNumber(s string) bool {
	result, err := regexp.MatchString(`(-?[0-9]+)|(-?[0-9]+[\.]{1}[0-9]+)`, s)
	if err != nil {
		return false
	}
	return result
}
