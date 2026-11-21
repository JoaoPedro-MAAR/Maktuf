package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/metrics"
)

// commands são os executáveis que o runner compila.
var commands = []string{"broker", "juiz", "vip", "sensor", "consumidor"}

// build compila os executáveis de cmd/ em binDir.
func build(binDir string) error {
	root, err := moduleRoot()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(binDir, 0o750); err != nil {
		return err
	}
	args := []string{"build", "-o", binDir + string(os.PathSeparator)}
	for _, c := range commands {
		args = append(args, "./cmd/"+c)
	}
	cmd := exec.Command("go", args...)
	cmd.Dir = root
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("go build: %w\n%s", err, out)
	}
	return nil
}

// moduleRoot acha a raiz do módulo, para o build funcionar de qualquer
// diretório (inclusive dos testes).
func moduleRoot() (string, error) {
	out, err := exec.Command("go", "env", "GOMOD").Output()
	if err != nil {
		return "", fmt.Errorf("go env GOMOD: %w", err)
	}
	mod := strings.TrimSpace(string(out))
	if mod == "" || mod == os.DevNull {
		return "", errors.New("runner fora do modulo Maktuf")
	}
	return filepath.Dir(mod), nil
}

func freePort() (string, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	addr := ln.Addr().String()
	return addr, ln.Close()
}

// proc é um processo do cenário. Os mesmos args permitem religá-lo depois de
// um kill.
type proc struct {
	name        string
	bin         string
	args        []string
	logPath     string
	metricsAddr string

	mu   sync.Mutex
	cmd  *exec.Cmd
	done chan struct{}
}

func (p *proc) start() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cmd != nil {
		select {
		case <-p.done:
		default:
			return fmt.Errorf("%s ja esta rodando", p.name)
		}
	}
	logFile, err := os.OpenFile(p.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return err
	}
	cmd := exec.Command(p.bin, p.args...)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return fmt.Errorf("iniciar %s: %w", p.name, err)
	}
	done := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		_ = logFile.Close()
		close(done)
	}()
	p.cmd, p.done = cmd, done
	return nil
}

func (p *proc) running() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cmd == nil {
		return false
	}
	select {
	case <-p.done:
		return false
	default:
		return true
	}
}

// signal envia sig e espera o processo terminar; com SIGINT, mata após
// timeout.
func (p *proc) signal(sig os.Signal, timeout time.Duration) error {
	p.mu.Lock()
	cmd, done := p.cmd, p.done
	p.mu.Unlock()
	if cmd == nil || !p.running() {
		return nil
	}
	if err := cmd.Process.Signal(sig); err != nil {
		return err
	}
	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		_ = cmd.Process.Kill()
		<-done
		return fmt.Errorf("%s nao encerrou em %s; morto", p.name, timeout)
	}
}

func (p *proc) kill() error { return p.signal(syscall.SIGKILL, 5*time.Second) }

func (p *proc) stop(timeout time.Duration) error { return p.signal(syscall.SIGINT, timeout) }

// scrape lê o /metrics do processo.
func (p *proc) scrape(timeout time.Duration) ([]metrics.Family, error) {
	client := http.Client{Timeout: timeout}
	resp, err := client.Get("http://" + p.metricsAddr + metrics.Path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: /metrics status %d", p.name, resp.StatusCode)
	}
	return metrics.ParseText(resp.Body)
}

// cluster são os processos de um cenário.
type cluster struct {
	s       *Scenario
	out     string
	log     *slog.Logger
	procs   map[string]*proc
	brokers []string
	order   []string
}

func (c *cluster) proc(name string) *proc { return c.procs[name] }

// newCluster monta a linha de comando de cada processo, com portas livres e
// diretórios dentro de out.
func newCluster(s *Scenario, out, binDir string, log *slog.Logger) (*cluster, error) {
	c := &cluster{s: s, out: out, log: log, procs: map[string]*proc{}}
	add := func(name, bin string, args ...string) (*proc, error) {
		metricsAddr, err := freePort()
		if err != nil {
			return nil, err
		}
		p := &proc{
			name:        name,
			bin:         filepath.Join(binDir, bin),
			args:        append(args, "-metrics-addr", metricsAddr),
			logPath:     filepath.Join(out, strings.ReplaceAll(name, ":", "-")+".log"),
			metricsAddr: metricsAddr,
		}
		c.procs[name] = p
		c.order = append(c.order, name)
		return p, nil
	}
	ports := func(n int) ([]string, error) {
		out := make([]string, n)
		for i := range out {
			addr, err := freePort()
			if err != nil {
				return nil, err
			}
			out[i] = addr
		}
		return out, nil
	}
	cl := s.Cluster

	var judgeAddr string
	if cl.Judge {
		p, err := ports(1)
		if err != nil {
			return nil, err
		}
		judgeAddr = p[0]
		args := []string{"-addr", judgeAddr}
		args = durFlag(args, "-failure-timeout", cl.FailureTimeout)
		args = durFlag(args, "-check-interval", cl.CheckInterval)
		if _, err := add("juiz", "juiz", args...); err != nil {
			return nil, err
		}
	}

	brokerArgs := func(id, addr string) []string {
		args := []string{"-addr", addr, "-data-dir", filepath.Join(out, "dados", id), "-id", id}
		args = intFlag(args, "-subscriber-queue", cl.SubscriberQueue)
		args = intFlag(args, "-sensor-queue", cl.SensorQueue)
		args = durFlag(args, "-status-tolerance", cl.StatusTolerance)
		if judgeAddr != "" {
			args = append(args, "-judge-addr", judgeAddr)
			args = durFlag(args, "-heartbeat-interval", cl.HeartbeatInterval)
			args = durFlag(args, "-fence-timeout", cl.FenceTimeout)
		}
		return args
	}

	var clientAddr string
	if cl.Replicated {
		p, err := ports(4)
		if err != nil {
			return nil, err
		}
		b1, b2, r1, r2 := p[0], p[1], p[2], p[3]
		// b2 sobe antes como reserva; as flags de replicação são simétricas.
		if _, err := add("b2", "broker", append(brokerArgs("b2", b2),
			"-role", "reserva", "-replication-addr", r2, "-replica-addr", r1)...); err != nil {
			return nil, err
		}
		if _, err := add("b1", "broker", append(brokerArgs("b1", b1),
			"-role", "primario", "-replication-addr", r1, "-replica-addr", r2)...); err != nil {
			return nil, err
		}
		c.brokers = []string{"b1", "b2"}
		vp, err := ports(1)
		if err != nil {
			return nil, err
		}
		clientAddr = vp[0]
		if _, err := add("vip", "vip", "-addr", clientAddr, "-backends", b1+","+b2); err != nil {
			return nil, err
		}
	} else {
		p, err := ports(1)
		if err != nil {
			return nil, err
		}
		clientAddr = p[0]
		if _, err := add("b1", "broker", brokerArgs("b1", clientAddr)...); err != nil {
			return nil, err
		}
		c.brokers = []string{"b1"}
	}

	for _, cs := range s.Consumers {
		args := []string{"-addr", clientAddr, "-client-id", cs.ClientID, "-report", "0"}
		args = durFlag(args, "-process-delay", cs.ProcessDelay)
		if _, err := add("consumidor:"+cs.ClientID, "consumidor", args...); err != nil {
			return nil, err
		}
	}

	for i, g := range s.Sensors {
		args := []string{
			"-addr", clientAddr, "-count", strconv.Itoa(g.Count), "-id-prefix", g.Group,
			"-data-dir", filepath.Join(out, "dados", "sensores-"+g.Group),
			"-seed", strconv.FormatInt(s.Seed+int64(i), 10),
			"-continuous-ratio", strconv.FormatFloat(g.ContinuousRatio, 'g', -1, 64),
			"-report", "0",
			"-drain-timeout", s.Drain.String(),
		}
		if g.Stagger != nil {
			args = append(args, "-stagger="+strconv.FormatBool(*g.Stagger))
		}
		args = durFlag(args, "-low-interval", g.LowInterval)
		args = durFlag(args, "-continuous-interval", g.ContinuousInterval)
		args = intFlag(args, "-buffer-size", g.BufferSize)
		args = durFlag(args, "-publish-timeout", g.PublishTimeout)
		args = floatFlag(args, "-failure-prob", g.FailureProb)
		args = durFlag(args, "-failure-downtime", g.FailureDowntime)
		args = floatFlag(args, "-sleep-prob", g.SleepProb)
		args = durFlag(args, "-sleep-duration", g.SleepDuration)
		args = floatFlag(args, "-late-wake-prob", g.LateWakeProb)
		args = durFlag(args, "-late-wake-extra", g.LateWakeExtra)
		args = durFlag(args, "-burst-at", g.BurstAt)
		args = intFlag(args, "-burst-size", g.BurstSize)
		if _, err := add("sensores:"+g.Group, "sensor", args...); err != nil {
			return nil, err
		}
	}
	return c, nil
}

func durFlag(args []string, name string, d Duration) []string {
	if d == 0 {
		return args
	}
	return append(args, name, d.String())
}

func intFlag(args []string, name string, v int) []string {
	if v == 0 {
		return args
	}
	return append(args, name, strconv.Itoa(v))
}

func floatFlag(args []string, name string, v float64) []string {
	if v == 0 {
		return args
	}
	return append(args, name, strconv.FormatFloat(v, 'g', -1, 64))
}

// startInfra sobe Juiz, brokers, vip e consumidores e espera o sistema ficar
// pronto: /metrics respondendo, um primário atendendo e as inscrições feitas.
func (c *cluster) startInfra(ctx context.Context) error {
	for _, name := range c.order {
		if strings.HasPrefix(name, "sensores:") || strings.HasPrefix(name, "consumidor:") {
			continue
		}
		if err := c.procs[name].start(); err != nil {
			return err
		}
		if err := c.waitMetrics(ctx, c.procs[name]); err != nil {
			return err
		}
	}
	if err := c.waitUntil(ctx, "primario atendendo", func() bool { _, err := c.resolve("primario"); return err == nil }); err != nil {
		return err
	}
	for _, cs := range c.s.Consumers {
		p := c.procs["consumidor:"+cs.ClientID]
		if err := p.start(); err != nil {
			return err
		}
		if err := c.waitMetrics(ctx, p); err != nil {
			return err
		}
	}
	// Cada consumidor se inscreve em leituras e status.
	want := float64(2 * len(c.s.Consumers))
	return c.waitUntil(ctx, "inscricoes dos consumidores", func() bool {
		primary, err := c.resolve("primario")
		if err != nil {
			return false
		}
		fams, err := primary.scrape(time.Second)
		if err != nil {
			return false
		}
		return sumFamily(fams, "active_subscriptions", nil) >= want
	})
}

func (c *cluster) waitMetrics(ctx context.Context, p *proc) error {
	return c.waitUntil(ctx, "/metrics de "+p.name, func() bool {
		_, err := p.scrape(500 * time.Millisecond)
		return err == nil
	})
}

func (c *cluster) waitUntil(ctx context.Context, what string, cond func() bool) error {
	deadline := time.Now().Add(30 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout aguardando %s", what)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
	return nil
}

// resolve traduz o alvo de uma falha. primario e reserva são decididos na
// hora pelo broker_state de cada broker (1 primário; 2 ou 3 reserva).
func (c *cluster) resolve(target string) (*proc, error) {
	if target != "primario" && target != "reserva" {
		if p := c.procs[target]; p != nil {
			return p, nil
		}
		return nil, fmt.Errorf("alvo %q inexistente", target)
	}
	for _, name := range c.brokers {
		p := c.procs[name]
		if !p.running() {
			continue
		}
		fams, err := p.scrape(time.Second)
		if err != nil {
			continue
		}
		state := sumFamily(fams, "broker_state", nil)
		if (target == "primario" && state == 1) || (target == "reserva" && (state == 2 || state == 3)) {
			return p, nil
		}
	}
	return nil, fmt.Errorf("nenhum broker no papel %s", target)
}

// apply executa a falha e devolve o instante em que ela aconteceu.
func (c *cluster) apply(f Fault) (time.Time, string, error) {
	p, err := c.resolve(f.Target)
	if err != nil {
		return time.Now(), "", err
	}
	at := time.Now()
	switch f.Action {
	case "kill":
		err = p.kill()
	case "stop":
		err = p.stop(10 * time.Second)
	case "start":
		err = p.start()
	}
	return at, p.name, err
}

// stopSensors encerra os sensores com SIGINT: eles param de medir e drenam o
// buffer (até Drain).
func (c *cluster) stopSensors() {
	var wg sync.WaitGroup
	for _, name := range c.order {
		if !strings.HasPrefix(name, "sensores:") {
			continue
		}
		p := c.procs[name]
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.stop(c.s.Drain.D() + 10*time.Second); err != nil {
				c.log.Warn("sensor nao encerrou", "proc", p.name, "error", err)
			}
		}()
	}
	wg.Wait()
}

// stopAll encerra o resto, na ordem inversa de início.
func (c *cluster) stopAll() {
	for i := len(c.order) - 1; i >= 0; i-- {
		p := c.procs[c.order[i]]
		if err := p.stop(10 * time.Second); err != nil {
			c.log.Warn("processo nao encerrou", "proc", p.name, "error", err)
		}
	}
}

// sumFamily soma as séries (sem sufixo) de uma família que casam com labels.
func sumFamily(fams []metrics.Family, name string, labels map[string]string) float64 {
	var total float64
	for _, f := range fams {
		if f.Name != name {
			continue
		}
		for _, s := range f.Samples {
			if s.Suffix == "" && labelsMatch(s.Labels, labels) {
				total += s.Value
			}
		}
	}
	return total
}

func labelsMatch(have []metrics.Label, want map[string]string) bool {
	for k, v := range want {
		found := false
		for _, l := range have {
			if l.Name == k && l.Value == v {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
