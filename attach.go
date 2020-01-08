package sea

import (
	"fmt"
	"time"
)

// DatetimeUnixNano
func DatetimeUnixNano() string {
	now := time.Now()
	return fmt.Sprintf("%s %d", now.Format("2006-01-02 15:04:05"), now.UnixNano())
}
