package broker_test

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/broker"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/metrics"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/protocol"
)

func scrape(t *testing.T, srv *broker.Server) string {
	t.Helper()
	rec := httptest.NewRecorder()
	srv.Metrics().Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, metrics.Path, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /metrics = %d", rec.Code)
	}
	return rec.Body.String()
}

// metricValue procura a linha exata "série valor" na exposição.
func metricValue(text, series string) (float64, bool) {
	for _, line := range strings.Split(text, "\n") {
		if rest, ok := strings.CutPrefix(line, series+" "); ok {
			v, err := strconv.ParseFloat(rest, 64)
			return v, err == nil
		}
	}
	return 0, false
}

// waitMetric espera a série atingir o valor: as métricas são atualizadas logo
// depois do PUBACK e do DELIVER, então o cliente pode vê-los antes.
func waitMetric(t *testing.T, srv *broker.Server, series string, want float64) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		text := scrape(t, srv)
		got, ok := metricValue(text, series)
		if ok && got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s = %v (presente=%v), esperado %v\n%s", series, got, ok, want, text)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestMetricsCountPublishDeliveryAndDuplicates(t *testing.T) {
	ts := startServer(t)
	consumer := connectAs(t, ts.addr, protocol.RoleConsumer, "c1")
	consumer.subscribe(t, sensorTopic)
	sensor := connectAs(t, ts.addr, protocol.RoleSensor, "a")

	for seq := uint64(1); seq <= 3; seq++ {
		sensor.publish(t, reading(seq))
		consumer.ackDeliver(t, consumer.readDeliver(t))
	}
	sensor.publish(t, reading(2)) // seq já aceito (CE-16)
	if ack := sensor.publish(t, &protocol.Publish{Topic: "sensores/a/umidade", SensorID: "a", Seq: 4}); ack.Status != protocol.StatusNoSubscribers {
		t.Fatalf("status sem inscritos = %v", ack.Status)
	}

	srv := ts.srv
	waitMetric(t, srv, `messages_published_total{sensor_id="a",topic="sensores/a/temperatura"}`, 3)
	waitMetric(t, srv, `messages_published_total{sensor_id="a",topic="sensores/a/umidade"}`, 1)
	waitMetric(t, srv, `messages_duplicated_total{sensor_id="a"}`, 1)
	waitMetric(t, srv, `messages_dropped_no_subscribers_total{sensor_id="a"}`, 1)
	waitMetric(t, srv, `messages_delivered_total`, 3)
	waitMetric(t, srv, `ack_latency_seconds_count`, 5)
	waitMetric(t, srv, `delivery_latency_seconds_count`, 3)
	waitMetric(t, srv, `topic_count`, 1)
	waitMetric(t, srv, `active_subscriptions{topic="sensores/a/temperatura"}`, 1)
	waitMetric(t, srv, `active_sensors{state="online"}`, 1)
	waitMetric(t, srv, `broker_queue_depth{queue="subscriber",id="c1"}`, 0)

	text := scrape(t, srv)
	for _, want := range []string{
		"# TYPE ack_latency_seconds histogram",
		`ack_latency_seconds_quantile{quantile="0.99"}`,
		`delivery_latency_seconds_bucket{le="+Inf"} 3`,
		"replica_synced 0",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("exposicao sem %q:\n%s", want, text)
		}
	}
}

// RF-37: descartes por retenção aparecem por assinante, separados dos
// descartes sem inscritos (RNF-13).
func TestMetricsReportRetentionDrops(t *testing.T) {
	ts := startServerWithConfig(t, broker.Config{SubscriberQueueLimit: 2})
	slow := connectAs(t, ts.addr, protocol.RoleConsumer, "slow")
	slow.subscribe(t, sensorTopic)
	sensor := connectAs(t, ts.addr, protocol.RoleSensor, "a")

	for seq := uint64(1); seq <= 5; seq++ {
		sensor.publish(t, reading(seq))
	}

	waitMetric(t, ts.srv, `retention_drops_total{client_id="slow"}`, 3)
	waitMetric(t, ts.srv, `broker_queue_depth{queue="subscriber",id="slow"}`, 2)
	if _, ok := metricValue(scrape(t, ts.srv), `messages_dropped_no_subscribers_total{sensor_id="a"}`); ok {
		t.Fatalf("retencao nao pode contar como descarte sem inscritos")
	}
}
