package broker

import "time"

// monitor varre os prazos de sono com um único ticker, sem uma goroutine por
// sensor (RF-28). Ele não participa do caminho de leitura/entrega.
type monitor struct {
	status   *statusManager
	interval time.Duration
	stop     chan struct{}
	done     chan struct{}
}

func newMonitor(status *statusManager, interval time.Duration) *monitor {
	if interval <= 0 {
		interval = DefaultStatusScanInterval
	}
	return &monitor{
		status:   status,
		interval: interval,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
}

func (m *monitor) start() {
	go m.run()
}

func (m *monitor) run() {
	defer close(m.done)
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.status.expire(m.status.now())
		case <-m.stop:
			return
		}
	}
}

func (m *monitor) close() {
	close(m.stop)
	<-m.done
}
