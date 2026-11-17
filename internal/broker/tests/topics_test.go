package broker_test

import (
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/broker"
)

func TestValidFilter(t *testing.T) {
	valid := []string{"sensores", "sensores/a/temperatura", "sensores/+/temperatura", "+", "+/+", "a/+/c"}
	for _, f := range valid {
		if !broker.ValidFilter(f) {
			t.Errorf("ValidFilter(%q) = false, esperado true", f)
		}
	}

	invalid := []string{"", "/", "a//b", "/a", "a/", "a+b", "a/+b/c", "#", "a/#", "a/b#"}
	for _, f := range invalid {
		if broker.ValidFilter(f) {
			t.Errorf("ValidFilter(%q) = true, esperado false", f)
		}
	}
}

func TestTopicLifecycle(t *testing.T) {
	m := broker.NewTopicManager()
	const filter = "sensores/+/temperatura"

	if m.HasTopic(filter) {
		t.Fatal("topico nao deveria existir antes da primeira inscricao")
	}
	if !m.Subscribe("c1", filter) {
		t.Fatal("primeira inscricao deveria criar o topico")
	}
	if m.Subscribe("c1", filter) {
		t.Fatal("inscricao repetida nao deveria recriar o topico")
	}
	if m.Subscribe("c2", filter) {
		t.Fatal("segundo inscrito nao deveria recriar o topico")
	}
	if m.TopicCount() != 1 {
		t.Fatalf("TopicCount = %d, esperado 1", m.TopicCount())
	}

	if m.Unsubscribe("c1", filter) {
		t.Fatal("topico nao deveria ser removido com inscritos restantes")
	}
	if !m.HasTopic(filter) {
		t.Fatal("topico deveria existir enquanto houver inscritos")
	}
	if !m.Unsubscribe("c2", filter) {
		t.Fatal("topico deveria ser removido quando o ultimo inscrito sai")
	}
	if m.HasTopic(filter) || m.TopicCount() != 0 {
		t.Fatal("topico deveria ter sido removido")
	}
}

func TestUnsubscribeUnknown(t *testing.T) {
	m := broker.NewTopicManager()
	if m.Unsubscribe("c1", "sensores/a") {
		t.Fatal("cancelar inscricao em topico inexistente nao deveria remover nada")
	}

	m.Subscribe("c1", "sensores/a")
	if m.Unsubscribe("c2", "sensores/a") {
		t.Fatal("cancelar inscricao de outro cliente nao deveria remover o topico")
	}
	if !m.HasTopic("sensores/a") {
		t.Fatal("topico deveria continuar existindo")
	}
}

func TestSubscribers(t *testing.T) {
	m := broker.NewTopicManager()

	if got := m.Subscribers("sensores/a/temperatura"); len(got) != 0 {
		t.Fatalf("sem inscritos, obtido %v", got)
	}

	m.Subscribe("c1", "sensores/+/temperatura")
	m.Subscribe("c1", "sensores/a/temperatura")
	m.Subscribe("c2", "sensores/a/temperatura")
	m.Subscribe("c3", "sensores/b/temperatura")
	m.Subscribe("c4", "+/+")

	got := m.Subscribers("sensores/a/temperatura")
	want := []string{"c1", "c2"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Subscribers = %v, esperado %v", got, want)
	}

	got = m.Subscribers("sensores/b/temperatura")
	want = []string{"c1", "c3"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Subscribers = %v, esperado %v", got, want)
	}

	m.Unsubscribe("c1", "sensores/+/temperatura")
	got = m.Subscribers("sensores/b/temperatura")
	want = []string{"c3"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("apos cancelamento, Subscribers = %v, esperado %v", got, want)
	}
}

func TestTopicManagerConcurrent(t *testing.T) {
	m := broker.NewTopicManager()
	const workers = 16
	const iterations = 200

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			clientID := fmt.Sprintf("c%d", w)
			filter := fmt.Sprintf("sensores/%d/temperatura", w%4)
			for i := 0; i < iterations; i++ {
				m.Subscribe(clientID, filter)
				_ = m.Subscribers("sensores/0/temperatura")
				m.Unsubscribe(clientID, filter)
			}
		}(w)
	}
	wg.Wait()

	if n := m.TopicCount(); n != 0 {
		t.Fatalf("TopicCount = %d, esperado 0 apos todos cancelarem", n)
	}
}
