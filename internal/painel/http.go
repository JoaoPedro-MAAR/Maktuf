package painel

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/sensor"
)

// Handler devolve as rotas da página e da API.
//
//	GET    /                                  página
//	GET    /api/estado                        sensores, consumidor e infraestrutura
//	GET    /api/eventos                       entregas e ações ao vivo (Server-Sent Events)
//	POST   /api/sensores                      {"id"} cria um sensor
//	DELETE /api/sensores/{id}                 remove (o buffer fica em disco)
//	POST   /api/sensores/{id}/enviar          {"temperatura"}
//	POST   /api/sensores/{id}/rajada          {"quantidade","temperatura"}
//	POST   /api/sensores/{id}/dormir          {"segundos"}
//	POST   /api/sensores/{id}/desconectar
//	POST   /api/sensores/{id}/conectar
//	POST   /api/consumidor/inscrever          {"filtro"}
//	POST   /api/consumidor/cancelar           {"filtro"}
//	POST   /api/consumidor/pausar
//	POST   /api/consumidor/retomar
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	page, _ := fs.Sub(webFS, "web")
	mux.Handle("GET /", http.FileServer(http.FS(page)))
	mux.HandleFunc("GET /api/estado", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, http.StatusOK, s.state()) })
	mux.HandleFunc("GET /api/eventos", s.events)

	mux.HandleFunc("POST /api/sensores", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			ID string `json:"id"`
		}
		if !readJSON(w, r, &body) {
			return
		}
		if _, err := s.addSensor(body.ID); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		s.action(body.ID, "  sensor criado")
		writeJSON(w, http.StatusCreated, map[string]string{"texto": "sensor " + body.ID + " criado"})
	})
	mux.HandleFunc("DELETE /api/sensores/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if !s.removeSensor(id) {
			writeError(w, http.StatusNotFound, "sensor inexistente")
			return
		}
		s.action(id, "  sensor removido (o buffer continua em disco)")
		writeJSON(w, http.StatusOK, map[string]string{"texto": "removido"})
	})
	s.sensorRoute(mux, "enviar", func(r *http.Request, m *sensor.Manual) (string, error) {
		var body struct {
			Temperature *float32 `json:"temperatura"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Temperature == nil {
			return "", errBadRequest("informe a temperatura")
		}
		return m.Publish(s.ctx, *body.Temperature)
	})
	mux.HandleFunc("POST /api/sensores/{id}/rajada", func(w http.ResponseWriter, r *http.Request) {
		m := s.sensor(r.PathValue("id"))
		if m == nil {
			writeError(w, http.StatusNotFound, "sensor inexistente")
			return
		}
		var body struct {
			Count       int     `json:"quantidade"`
			Temperature float32 `json:"temperatura"`
		}
		if !readJSON(w, r, &body) {
			return
		}
		if body.Count <= 0 || body.Count > 100000 {
			writeError(w, http.StatusBadRequest, "quantidade entre 1 e 100000")
			return
		}
		// A rajada pode levar segundos: roda em segundo plano e o resultado
		// chega pelos eventos; o cartão do sensor mostra "ocupado".
		go func() {
			text, err := m.Burst(s.ctx, body.Count, body.Temperature)
			if errors.Is(err, sensor.ErrBusy) {
				text = "  ocupado: outra operacao deste sensor em andamento"
			}
			s.action(m.ID(), text)
		}()
		writeJSON(w, http.StatusAccepted, map[string]string{"texto": fmt.Sprintf("  rajada de %d iniciada", body.Count)})
	})
	s.sensorRoute(mux, "dormir", func(r *http.Request, m *sensor.Manual) (string, error) {
		var body struct {
			Seconds float64 `json:"segundos"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Seconds <= 0 {
			return "", errBadRequest("informe os segundos de sono")
		}
		return m.Sleep(s.ctx, time.Duration(body.Seconds*float64(time.Second)))
	})
	s.sensorRoute(mux, "desconectar", func(_ *http.Request, m *sensor.Manual) (string, error) { return m.Disconnect() })
	s.sensorRoute(mux, "conectar", func(_ *http.Request, m *sensor.Manual) (string, error) { return m.Connect(s.ctx) })

	filterRoute := func(op func(string) string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			var body struct {
				Filter string `json:"filtro"`
			}
			if !readJSON(w, r, &body) {
				return
			}
			text := op(body.Filter)
			s.action("consumidor", text)
			writeJSON(w, http.StatusOK, map[string]string{"texto": text})
		}
	}
	mux.HandleFunc("POST /api/consumidor/inscrever", filterRoute(func(f string) string { return s.consumer.Subscribe(s.ctx, f) }))
	mux.HandleFunc("POST /api/consumidor/cancelar", filterRoute(func(f string) string { return s.consumer.Unsubscribe(s.ctx, f) }))
	mux.HandleFunc("POST /api/consumidor/pausar", func(w http.ResponseWriter, _ *http.Request) {
		text := s.consumer.SetPaused(true)
		s.action("consumidor", text)
		writeJSON(w, http.StatusOK, map[string]string{"texto": text})
	})
	mux.HandleFunc("POST /api/consumidor/retomar", func(w http.ResponseWriter, _ *http.Request) {
		text := s.consumer.SetPaused(false)
		s.action("consumidor", text)
		writeJSON(w, http.StatusOK, map[string]string{"texto": text})
	})
	return mux
}

type badRequest string

func (e badRequest) Error() string { return string(e) }

func errBadRequest(msg string) error { return badRequest(msg) }

// sensorRoute registra POST /api/sensores/{id}/<acao>, que executa op e
// publica o texto como evento.
func (s *Server) sensorRoute(mux *http.ServeMux, action string, op func(*http.Request, *sensor.Manual) (string, error)) {
	mux.HandleFunc("POST /api/sensores/{id}/"+action, func(w http.ResponseWriter, r *http.Request) {
		m := s.sensor(r.PathValue("id"))
		if m == nil {
			writeError(w, http.StatusNotFound, "sensor inexistente")
			return
		}
		text, err := op(r, m)
		var bad badRequest
		switch {
		case errors.As(err, &bad):
			writeError(w, http.StatusBadRequest, bad.Error())
			return
		case errors.Is(err, sensor.ErrBusy):
			writeError(w, http.StatusConflict, "ocupado: outra operacao deste sensor em andamento (rajada?)")
			return
		case err != nil:
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		s.action(m.ID(), text)
		writeJSON(w, http.StatusOK, map[string]string{"texto": text})
	})
}

// events transmite entregas e ações por Server-Sent Events, começando pelos
// mais recentes.
func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming nao suportado")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	ch, recent := s.subscribe()
	defer s.unsubscribe(ch)
	send := func(ev uiEvent) bool {
		data, _ := json.Marshal(ev)
		if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
			return false
		}
		flusher.Flush()
		return true
	}
	for _, ev := range recent {
		if !send(ev) {
			return
		}
	}
	keepAlive := time.NewTicker(15 * time.Second)
	defer keepAlive.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-s.ctx.Done():
			return
		case ev := <-ch:
			if !send(ev) {
				return
			}
		case <-keepAlive.C:
			if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func readJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, "JSON invalido: "+err.Error())
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"erro": msg})
}
