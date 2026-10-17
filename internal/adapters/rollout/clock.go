package rollout

import (
	"time"

	rolloutapp "narumq/internal/application/rollout"
)

type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now().UTC() }

var _ rolloutapp.Clock = SystemClock{}
