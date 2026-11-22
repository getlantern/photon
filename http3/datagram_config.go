package http3

import "fmt"

func datagramLimits(queue, payload int) (int, int, error) {
	if queue == 0 {
		queue = 16
	}
	if payload == 0 {
		payload = 1200
	}
	if queue < 1 || payload < 1 || payload > 65535 {
		return 0, 0, fmt.Errorf("http3: invalid datagram limits")
	}
	return queue, payload, nil
}
