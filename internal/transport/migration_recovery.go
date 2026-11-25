package transport

import (
	"time"

	"github.com/getlantern/photon/internal/recovery"
)

// resetMigrationRecovery is the post-validation network-change transition. Callers
// must commit address/CID ownership separately. Preserve objects aliased by path
// zero and retain logical transmission references so late old-path ACKs can still
// complete reliable data without affecting the new network's recovery estimates.
func (a *pathApplication) resetMigrationRecovery() {
	outstanding := a.sent.ResetForPathChange()
	queueLostItems(a.space, a.space, outstanding[recovery.Application])
	a.congestion.Reset(1200)
	*a.timer = recovery.Timer{}
	*a.pacer = recovery.Pacer{}
	a.probePending = false
	a.pacingAt = time.Time{}
}
