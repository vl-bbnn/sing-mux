package mux

import "testing"

func TestSmuxConfigProvidesConcurrentStreamCredit(t *testing.T) {
	config := smuxConfig()
	if config.MaxStreamBuffer < 256*1024 {
		t.Fatalf("MaxStreamBuffer = %d, want at least 256 KiB", config.MaxStreamBuffer)
	}
	if config.MaxReceiveBuffer < 48*config.MaxStreamBuffer {
		t.Fatalf(
			"MaxReceiveBuffer = %d, want credit for at least 48 streams of %d bytes",
			config.MaxReceiveBuffer,
			config.MaxStreamBuffer,
		)
	}
	if !config.KeepAliveDisabled {
		t.Fatal("smux keepalive must remain disabled")
	}
}
