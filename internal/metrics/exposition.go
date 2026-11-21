package metrics

import (
	"bufio"
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

// ParseText lê o formato textual produzido por WriteText: linhas # HELP e
// # TYPE e uma série por linha, com rótulos escapados. Cada série é atribuída
// à última família declarada se o nome dela começar com o da família (com os
// sufixos de histograma); senão vira uma família sem tipo. Linhas que não
// seguem o formato são ignoradas.
func ParseText(r io.Reader) ([]Family, error) {
	var (
		out []Family
		cur = -1
	)
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			continue
		case strings.HasPrefix(line, "# HELP "):
			name, help, _ := strings.Cut(strings.TrimPrefix(line, "# HELP "), " ")
			cur = familyIndex(&out, name)
			out[cur].Help = unescapeHelp(help)
		case strings.HasPrefix(line, "# TYPE "):
			name, typ, _ := strings.Cut(strings.TrimPrefix(line, "# TYPE "), " ")
			cur = familyIndex(&out, name)
			out[cur].Type = typ
		case strings.HasPrefix(line, "#"):
			continue
		default:
			name, labels, value, ok := parseSample(line)
			if !ok {
				continue
			}
			suffix := ""
			if cur >= 0 && strings.HasPrefix(name, out[cur].Name) {
				suffix = name[len(out[cur].Name):]
			}
			if cur < 0 || (suffix != "" && suffix != "_bucket" && suffix != "_sum" && suffix != "_count") ||
				!strings.HasPrefix(name, out[cur].Name) {
				cur = familyIndex(&out, name)
				suffix = ""
			}
			out[cur].Samples = append(out[cur].Samples, Sample{Suffix: suffix, Labels: labels, Value: value})
		}
	}
	return out, sc.Err()
}

func familyIndex(fams *[]Family, name string) int {
	for i := range *fams {
		if (*fams)[i].Name == name {
			return i
		}
	}
	*fams = append(*fams, Family{Name: name})
	return len(*fams) - 1
}

// parseSample lê "nome{a="x",b="y"} valor".
func parseSample(line string) (string, []Label, float64, bool) {
	var (
		labels     []Label
		name, rest string
	)
	if i := strings.IndexByte(line, '{'); i >= 0 {
		name = line[:i]
		var ok bool
		labels, rest, ok = parseLabels(line[i+1:])
		if !ok {
			return "", nil, 0, false
		}
	} else {
		var found bool
		name, rest, found = strings.Cut(line, " ")
		if !found {
			return "", nil, 0, false
		}
		rest = " " + rest
	}
	value, err := strconv.ParseFloat(strings.TrimSpace(rest), 64)
	if err != nil || name == "" {
		return "", nil, 0, false
	}
	return name, labels, value, true
}

// parseLabels lê os rótulos até o '}' e devolve o resto da linha.
func parseLabels(s string) ([]Label, string, bool) {
	var labels []Label
	for {
		if strings.HasPrefix(s, "}") {
			return labels, s[1:], true
		}
		name, after, ok := strings.Cut(s, `="`)
		if !ok {
			return nil, "", false
		}
		var val strings.Builder
		i := 0
		for ; i < len(after); i++ {
			c := after[i]
			if c == '\\' && i+1 < len(after) {
				i++
				switch after[i] {
				case 'n':
					val.WriteByte('\n')
				default:
					val.WriteByte(after[i])
				}
				continue
			}
			if c == '"' {
				break
			}
			val.WriteByte(c)
		}
		if i >= len(after) {
			return nil, "", false
		}
		labels = append(labels, Label{Name: name, Value: val.String()})
		s = strings.TrimPrefix(after[i+1:], ",")
	}
}

func unescapeHelp(s string) string {
	return strings.NewReplacer(`\\`, `\`, `\n`, "\n").Replace(s)
}
