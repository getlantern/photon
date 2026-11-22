//go:build !linux && !darwin

package transport

func configureReceiveECN(socketController) {}
func receivedECN([]byte, int) ECN          { return ECNUnknown }
func udpPayloadTruncated(int) bool         { return false }
