package metrics

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// ContentType é o tipo do formato textual do Prometheus (versão 0.0.4).
const ContentType = "text/plain; version=0.0.4; charset=utf-8"

// Path é o caminho HTTP onde as métricas são servidas.
const Path = "/metrics"

const shutdownTimeout = 2 * time.Second

var (
	labelEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	helpEscaper  = strings.NewReplacer(`\`, `\\`, "\n", `\n`)
)

// WriteText escreve as famílias no formato textual do Prometheus: # HELP,
// # TYPE e uma linha por série. O texto é montado em memória e escrito de uma
// vez, então só a escrita final pode falhar.
func WriteText(w io.Writer, families []Family) error {
	var buf bytes.Buffer
	for _, f := range families {
		buf.WriteString("# HELP " + f.Name + " " + helpEscaper.Replace(f.Help) + "\n")
		buf.WriteString("# TYPE " + f.Name + " " + f.Type + "\n")
		for _, s := range f.Samples {
			buf.WriteString(f.Name + s.Suffix)
			writeLabels(&buf, s.Labels)
			buf.WriteString(" " + formatFloat(s.Value) + "\n")
		}
	}
	_, err := buf.WriteTo(w)
	return err
}

func writeLabels(buf *bytes.Buffer, labels []Label) {
	if len(labels) == 0 {
		return
	}
	buf.WriteByte('{')
	for i, l := range labels {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(l.Name + `="` + labelEscaper.Replace(l.Value) + `"`)
	}
	buf.WriteByte('}')
}

func formatFloat(v float64) string {
	switch {
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	case math.IsNaN(v):
		return "NaN"
	default:
		return strconv.FormatFloat(v, 'g', -1, 64)
	}
}

// Handler responde GET com as métricas do registro no formato textual.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet && req.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "metodo nao permitido", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", ContentType)
		if req.Method == http.MethodHead {
			return
		}
		_ = WriteText(w, r.Gather())
	})
}

// ListenAndServe serve o registro em addr, no caminho Path, até ctx terminar.
// O encerramento pelo contexto devolve nil; erro de bind é devolvido.
func ListenAndServe(ctx context.Context, addr string, r *Registry, log *slog.Logger) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return Serve(ctx, ln, r, log)
}

// Serve atende no listener informado; útil quando quem chama precisa saber a
// porta escolhida (addr ":0").
func Serve(ctx context.Context, ln net.Listener, r *Registry, log *slog.Logger) error {
	if log == nil {
		log = slog.Default()
	}
	mux := http.NewServeMux()
	mux.Handle(Path, r.Handler())
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
			_ = srv.Shutdown(shutdownCtx)
			cancel()
		case <-done:
		}
	}()
	defer close(done)

	log.Info("endpoint de observabilidade ativo", "addr", ln.Addr().String(), "path", Path)
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
