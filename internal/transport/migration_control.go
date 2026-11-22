package transport

import (
	"github.com/getlantern/photon/internal/wire"
	"time"
)

// receiveMigrationCIDControl applies connection-wide CID lifecycle information
// without admitting the candidate's application frames or recording its packet.
// Otherwise retirement/replacement can deadlock validation or fallback when all
// unused IDs are consumed. A separate history prevents duplicate candidate
// packets from repeatedly queuing retirement responses.
func (c *Conn) receiveMigrationCIDControl(packet *pathPacket, now time.Time) error {
	app := c.multipath.paths[packet.pathID].application
	if receivedPacketNumber(app.space.received, packet.number) || receivedPacketNumber(app.candidateCIDControls, packet.number) {
		return nil
	}
	handled := false
	for _, frame := range packet.frames {
		switch frame.(type) {
		case wire.NewConnectionIDFrame, wire.PathNewConnectionIDFrame, wire.RetireConnectionIDFrame, wire.PathRetireConnectionIDFrame:
			handled = true
			control := *packet
			control.receipt = NewPacket
			control.frames = []wire.Frame{frame}
			if err := c.dispatchMultipathPacket(&control, now); err != nil {
				return err
			}
		}
	}
	if handled {
		if app.candidateCIDControls == nil {
			app.candidateCIDControls, _ = NewReceived(64)
		}
		_, err := app.candidateCIDControls.Record(packet.number, now)
		return err
	}
	return nil
}
