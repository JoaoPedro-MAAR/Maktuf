package protocol_test

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/protocol"
)

func samples() []protocol.Message {
	return []protocol.Message{
		&protocol.Connect{Role: protocol.RoleSensor, ClientID: "sensor-1"},
		&protocol.ConnAck{Status: protocol.StatusOK},
		&protocol.Publish{Topic: "sensores/a/temperatura", SensorID: "a", Seq: 1, AgeMS: 1000, Temperature: 25.5},
		&protocol.PubAck{Status: protocol.StatusPersisted, Seq: 1},
		&protocol.Subscribe{TopicFilter: "sensores/+/temperatura"},
		&protocol.SubAck{Status: protocol.StatusOK},
		&protocol.Unsubscribe{TopicFilter: "sensores/+/temperatura"},
		&protocol.UnsubAck{Status: protocol.StatusOK},
		&protocol.Deliver{Topic: "sensores/a/temperatura", SensorID: "a", Seq: 1, TimestampMS: 1700000000000, Temperature: 25.5},
		&protocol.Deliver{Topic: "sensores/a/status", SensorID: "a", Seq: 2, TimestampMS: 1700000000001, Status: protocol.SensorStateDormindo},
		&protocol.DeliverAck{Topic: "sensores/a/temperatura", SensorID: "a", Seq: 1},
		&protocol.Sleep{DurationMS: 60000},
		&protocol.Heartbeat{BrokerID: "broker-1", State: 1, LastSeq: 42},
		&protocol.Replica{Kind: protocol.ReplicaRecord, Index: 7, Record: []byte{3, 0, 1, 'a'}},
		&protocol.Replica{Kind: protocol.ReplicaRecord, Index: 8, Record: []byte{}},
		&protocol.Replica{Kind: protocol.ReplicaSyncStart, Index: 9},
		&protocol.Replica{Kind: protocol.ReplicaSyncEnd, Index: 9},
		&protocol.ReplicaAck{Status: protocol.StatusOK, Kind: protocol.ReplicaRecord, Index: 7},
		&protocol.ReplicaAck{Status: protocol.StatusTemporaryFailure, Kind: protocol.ReplicaSyncEnd, Index: 9},
		&protocol.Promote{BrokerID: "broker-2"},
	}
}

func TestRoundTrip(t *testing.T) {
	for i, want := range samples() {
		frame, err := protocol.Encode(want)
		if err != nil {
			t.Fatalf("caso %d (%s): Encode: %v", i, want.Type(), err)
		}

		got, err := protocol.Decode(frame)
		if err != nil {
			t.Fatalf("caso %d (%s): Decode: %v", i, want.Type(), err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("caso %d (%s): obtido %#v, esperado %#v", i, want.Type(), got, want)
		}
	}
}

func TestDeliverFlagsRoundTrip(t *testing.T) {
	want := &protocol.Deliver{
		Topic: "sensores/a/temperatura", SensorID: "a", Seq: 9,
		TimestampMS: 1700000000000, Temperature: 21.5,
		Flags: protocol.DeliverFlagGap,
	}
	frame, err := protocol.Encode(want)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if got := binary.BigEndian.Uint16(frame[6:8]); got != protocol.DeliverFlagGap {
		t.Fatalf("flags no quadro = 0x%04x, esperado 0x%04x", got, protocol.DeliverFlagGap)
	}

	got, err := protocol.Decode(frame)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("obtido %#v, esperado %#v", got, want)
	}
}

func TestNonDeliverKeepsReservedFlagsZero(t *testing.T) {
	frame, err := protocol.Encode(&protocol.PubAck{Status: protocol.StatusPersisted, Seq: 1})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if got := binary.BigEndian.Uint16(frame[6:8]); got != 0 {
		t.Fatalf("flags = 0x%04x, esperado 0", got)
	}
}

func TestReadMessageGlued(t *testing.T) {
	first := &protocol.Publish{Topic: "sensores/a", SensorID: "a", Seq: 1, AgeMS: 10, Temperature: 25.5}
	second := &protocol.Subscribe{TopicFilter: "sensores/+/temperatura"}

	var buf bytes.Buffer
	if err := protocol.WriteMessage(&buf, first); err != nil {
		t.Fatalf("WriteMessage first: %v", err)
	}
	if err := protocol.WriteMessage(&buf, second); err != nil {
		t.Fatalf("WriteMessage second: %v", err)
	}

	r := bufio.NewReader(&buf)
	gotFirst, err := protocol.ReadMessage(r)
	if err != nil {
		t.Fatalf("ReadMessage first: %v", err)
	}
	gotSecond, err := protocol.ReadMessage(r)
	if err != nil {
		t.Fatalf("ReadMessage second: %v", err)
	}

	if !reflect.DeepEqual(gotFirst, first) {
		t.Fatalf("primeira: obtido %#v, esperado %#v", gotFirst, first)
	}
	if !reflect.DeepEqual(gotSecond, second) {
		t.Fatalf("segunda: obtido %#v, esperado %#v", gotSecond, second)
	}
}

func TestReadMessagePartial(t *testing.T) {
	first := &protocol.Deliver{Topic: "sensores/a", SensorID: "a", Seq: 7, TimestampMS: 1700000000000, Temperature: 26}
	second := &protocol.PubAck{Status: protocol.StatusPersisted, Seq: 7}

	var buf bytes.Buffer
	if err := protocol.WriteMessage(&buf, first); err != nil {
		t.Fatalf("WriteMessage first: %v", err)
	}
	if err := protocol.WriteMessage(&buf, second); err != nil {
		t.Fatalf("WriteMessage second: %v", err)
	}

	r := bufio.NewReader(iotest.OneByteReader(&buf))
	gotFirst, err := protocol.ReadMessage(r)
	if err != nil {
		t.Fatalf("ReadMessage first: %v", err)
	}
	gotSecond, err := protocol.ReadMessage(r)
	if err != nil {
		t.Fatalf("ReadMessage second: %v", err)
	}

	if !reflect.DeepEqual(gotFirst, first) {
		t.Fatalf("primeira: obtido %#v, esperado %#v", gotFirst, first)
	}
	if !reflect.DeepEqual(gotSecond, second) {
		t.Fatalf("segunda: obtido %#v, esperado %#v", gotSecond, second)
	}
}

func TestMessageTooLarge(t *testing.T) {
	frame := make([]byte, protocol.HeaderSize)
	binary.BigEndian.PutUint32(frame[:4], protocol.MaxMessageSize+1)
	frame[4] = protocol.Version
	frame[5] = byte(protocol.TypeConnAck)

	if _, err := protocol.Decode(frame); !errors.Is(err, protocol.ErrMessageTooLarge) {
		t.Fatalf("Decode: esperado ErrMessageTooLarge, obtido %v", err)
	}

	r := bufio.NewReader(bytes.NewReader(frame[:4]))
	if _, err := protocol.ReadMessage(r); !errors.Is(err, protocol.ErrMessageTooLarge) {
		t.Fatalf("ReadMessage: esperado ErrMessageTooLarge, obtido %v", err)
	}
}

func TestUnsupportedVersion(t *testing.T) {
	frame := make([]byte, protocol.HeaderSize)
	binary.BigEndian.PutUint32(frame[:4], 4)
	frame[4] = protocol.Version + 1
	frame[5] = byte(protocol.TypeConnect)

	if _, err := protocol.Decode(frame); !errors.Is(err, protocol.ErrUnsupportedVersion) {
		t.Fatalf("esperado ErrUnsupportedVersion, obtido %v", err)
	}
}

func TestUnknownType(t *testing.T) {
	frame := make([]byte, protocol.HeaderSize)
	binary.BigEndian.PutUint32(frame[:4], 4)
	frame[4] = protocol.Version
	frame[5] = 0xFF

	if _, err := protocol.Decode(frame); !errors.Is(err, protocol.ErrUnknownType) {
		t.Fatalf("esperado ErrUnknownType, obtido %v", err)
	}
}

func TestTruncatedPayload(t *testing.T) {
	frame := make([]byte, 10)
	binary.BigEndian.PutUint32(frame[:4], 6)
	frame[4] = protocol.Version
	frame[5] = byte(protocol.TypePublish)
	frame[8] = 0
	frame[9] = 10

	if _, err := protocol.Decode(frame); !errors.Is(err, protocol.ErrTruncated) {
		t.Fatalf("esperado ErrTruncated, obtido %v", err)
	}
}

func TestTrailingBytes(t *testing.T) {
	frame := make([]byte, 10)
	binary.BigEndian.PutUint32(frame[:4], 6)
	frame[4] = protocol.Version
	frame[5] = byte(protocol.TypeConnAck)
	frame[6] = 0
	frame[7] = 0
	frame[8] = byte(protocol.StatusOK)
	frame[9] = 0

	if _, err := protocol.Decode(frame); !errors.Is(err, protocol.ErrTrailingBytes) {
		t.Fatalf("esperado ErrTrailingBytes, obtido %v", err)
	}
}

func TestEncodeStringTooLong(t *testing.T) {
	msg := &protocol.Subscribe{TopicFilter: strings.Repeat("a", 70000)}
	if _, err := protocol.Encode(msg); !errors.Is(err, protocol.ErrStringTooLong) {
		t.Fatalf("esperado ErrStringTooLong, obtido %v", err)
	}
}

func FuzzDecode(f *testing.F) {
	frame, err := protocol.Encode(&protocol.Publish{Topic: "t", SensorID: "s", Seq: 1, AgeMS: 2, Temperature: 3.5})
	if err != nil {
		f.Fatalf("Encode: %v", err)
	}
	f.Add(frame)
	f.Fuzz(func(_ *testing.T, data []byte) {
		_, _ = protocol.Decode(data)
	})
}

// Os kinds 1 a 3 da versão 2 (leitura, inscrição e cancelamento) foram
// substituídos pelo envio do registro do store e passam a ser recusados.
func TestReplicaRejectsRemovedKinds(t *testing.T) {
	for kind := uint8(1); kind <= 3; kind++ {
		frame := []byte{0, 0, 0, 5, protocol.Version, byte(protocol.TypeReplica), 0, 0, kind}
		if _, err := protocol.Decode(frame); !errors.Is(err, protocol.ErrUnknownReplicaKind) {
			t.Fatalf("kind %d: erro = %v, esperado %v", kind, err, protocol.ErrUnknownReplicaKind)
		}
	}
	if _, err := protocol.Encode(&protocol.Replica{Kind: 1}); !errors.Is(err, protocol.ErrUnknownReplicaKind) {
		t.Fatalf("Encode kind 1: erro = %v", err)
	}
}

func TestReplicaRecordTruncated(t *testing.T) {
	frame, err := protocol.Encode(&protocol.Replica{Kind: protocol.ReplicaRecord, Index: 1, Record: []byte{1, 2, 3}})
	if err != nil {
		t.Fatal(err)
	}
	// Corta o último byte do registro e ajusta o length do quadro.
	frame = frame[:len(frame)-1]
	binary.BigEndian.PutUint32(frame, uint32(len(frame)-4))
	if _, err := protocol.Decode(frame); !errors.Is(err, protocol.ErrTruncated) {
		t.Fatalf("erro = %v, esperado %v", err, protocol.ErrTruncated)
	}
}
