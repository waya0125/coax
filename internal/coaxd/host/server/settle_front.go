// Copyright 2026 Till0196
// SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later

package server

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/Till0196/coax/internal/coaxd/device"
	"github.com/Till0196/coax/internal/coaxd/receiver"
)

// Experiment for issue #3: hold the front of a dvb stream back until the
// demodulator has gone a while without flagging a packet, and start the
// response just after the last flagged one. Nothing clean is thrown away;
// only the damaged head is.
const (
	settleCleanPackets = 2000 // about 180 ms of an ISDB-T multiplex
	settleLimit        = 2 * time.Second
	packetSize         = 188
)

func dvbSettleEnabled() bool { return os.Getenv("COAXD_DVB_SETTLE") != "0" }

// alignPackets finds the first offset at or after from where two packets in a
// row begin with the sync byte, or -1 when there is not enough to say.
func alignPackets(data []byte, from int) int {
	for i := from; i+packetSize < len(data); i++ {
		if data[i] == 0x47 && data[i+packetSize] == 0x47 {
			return i
		}
	}
	return -1
}

func settleFront(ctx context.Context, stream device.Stream, prefix []byte) []byte {
	data := prefix
	buffer := make([]byte, readSize)
	deadline := time.Now().Add(settleLimit)
	scanned, keepFrom, clean := -1, 0, 0
	for {
		if scanned < 0 {
			if at := alignPackets(data, 0); at >= 0 {
				scanned, keepFrom = at, at
			}
		}
		for scanned >= 0 && scanned+packetSize <= len(data) {
			if data[scanned] != 0x47 {
				at := alignPackets(data, scanned+1)
				if at < 0 {
					break
				}
				scanned, keepFrom, clean = at, at, 0
				continue
			}
			if data[scanned+1]&0x80 != 0 {
				clean = 0
				keepFrom = scanned + packetSize
			} else {
				clean++
			}
			scanned += packetSize
			if clean >= settleCleanPackets {
				return data[keepFrom:]
			}
		}
		if ctx.Err() != nil || time.Now().After(deadline) {
			return data[keepFrom:]
		}
		read, err := stream.Read(ctx, buffer)
		if read > 0 {
			data = append(data, buffer[:read]...)
			continue
		}
		if err != nil && !errors.Is(err, receiver.ErrWouldBlock) && !errors.Is(err, device.ErrWouldBlock) {
			return data[keepFrom:]
		}
		select {
		case <-ctx.Done():
			return data[keepFrom:]
		case <-time.After(idlePoll):
		}
	}
}
