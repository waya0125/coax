// Copyright 2026 Till0196
// SPDX-License-Identifier: Apache-2.0 OR GPL-2.0-or-later

package server

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/Till0196/coax/internal/coaxd/device"
)

// packets builds count transport packets, numbered in their payload, with the
// transport error indicator set on those damaged says.
func packets(from, count int, damaged func(int) bool) []byte {
	var out []byte
	for i := from; i < from+count; i++ {
		p := make([]byte, packetSize)
		p[0], p[1], p[2], p[3] = 0x47, 0x01, 0x00, 0x10
		if damaged(i) {
			p[1] |= 0x80
		}
		p[4], p[5], p[6] = byte(i>>16), byte(i>>8), byte(i)
		out = append(out, p...)
	}
	return out
}

func packetNumber(p []byte) int { return int(p[4])<<16 | int(p[5])<<8 | int(p[6]) }

// feed hands out a fixed sequence of reads, then would block for ever.
type feed struct{ reads [][]byte }

func (f *feed) Read(_ context.Context, buffer []byte) (int, error) {
	if len(f.reads) == 0 {
		return 0, device.ErrWouldBlock
	}
	n := copy(buffer, f.reads[0])
	if f.reads[0] = f.reads[0][n:]; len(f.reads[0]) == 0 {
		f.reads = f.reads[1:]
	}
	return n, nil
}
func (f *feed) Close() error { return nil }

// handedOver is what the client would receive: what settleFront returned,
// then the rest of the stream, which the copy goes on to read.
func handedOver(t *testing.T, all []byte) []byte {
	t.Helper()
	stream := &feed{reads: [][]byte{all[:100*packetSize], all[100*packetSize:]}}
	got := settleFront(context.Background(), stream, nil)
	for _, rest := range stream.reads {
		got = append(got, rest...)
	}
	return got
}

func TestSettleFrontDropsOnlyTheDamagedHead(t *testing.T) {
	// The head is flagged now and then up to packet 1500, as on a PT3 just
	// after lock, and clean from there on.
	damaged := func(i int) bool { return i <= 1500 && i%50 == 7 }
	all := packets(0, 5000, damaged)
	got := handedOver(t, all)
	if len(got)%packetSize != 0 || got[0] != 0x47 {
		t.Fatalf("not aligned on a packet: %d bytes", len(got))
	}
	if first := packetNumber(got); first != 1458 {
		t.Fatalf("starts at packet %d, want 1458 (just after the last flagged one)", first)
	}
	if !bytes.Equal(got, all[1458*packetSize:]) {
		t.Fatal("a clean packet after the damaged head was lost")
	}
}

func TestSettleFrontHandsOverAStreamThatNeverSettles(t *testing.T) {
	saved := settleLimit
	settleLimit = 50 * time.Millisecond
	defer func() { settleLimit = saved }()

	// A weak channel: every hundredth packet flagged, for as long as it runs.
	all := packets(0, 3000, func(i int) bool { return i%100 == 0 })
	got := handedOver(t, all)
	if !bytes.Equal(got, all) {
		t.Fatalf("handed over %d of %d bytes; a stream that never settles should go through whole", len(got), len(all))
	}
}

func TestSettleFrontPassesACleanStreamWhole(t *testing.T) {
	all := packets(0, 3000, func(int) bool { return false })
	got := handedOver(t, all)
	if !bytes.Equal(got, all) {
		t.Fatalf("handed over %d of %d bytes of a clean stream", len(got), len(all))
	}
}
