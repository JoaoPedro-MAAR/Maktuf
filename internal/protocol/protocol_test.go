package protocol

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
	"strings"
	"testing"
	"testing/iotest"
)

func samples() []Message {
	return []Message{
		&Connect{Role: RoleSensor, ClientID: "sensor-1"},
		&ConnAck{Status: StatusOK},
		&Publish{Topic: "sensores/a/temperatura", SensorID: "a", Seq: 1, AgeMS: 1000, Temperature: 25.5},
		&PubAck{Status: StatusPersisted, Seq: 1},
		&Subscribe{TopicFilter: "sensores/+/temperatura"},
		&SubAck{Status: StatusOK},
		&Unsubscribe{TopicFilter: "sensores/+/temperatura"},
		&UnsubAck{Status: StatusOK},
		&Deliver{Topic: "sensores/a/temperatura", SensorID: "a", Seq: 1, TimestampMS: 1700000000000, Temperature: 25.5},
		&DeliverAck{Topic: "sensores/a/temperatura", SensorID: "a", Seq: 1},
		&Sleep{DurationMS: 60000},
		&Heartbeat{BrokerID: "broker-1", State: 1, LastSeq: 42},
		&Replica{Kind: ReplicaReading, Topic: "sensores/a/temperatura", SensorID: "a", Seq: 1, TimestampMS: 1700000000000, Temperature: 25.5},
		&Replica{Kind: ReplicaSubscription, ClientID: "consumer-1", TopicFilter: "sensores/+/temperatura"},
		&Replica{Kind: ReplicaUnsubscribe, ClientID: "consumer-1", TopicFilter: "sensores/+/temperatura"},
		&ReplicaAck{Status: StatusOK, Kind: ReplicaReading, SensorID: "a", Seq: 1},
		&ReplicaAck{Status: StatusOK, Kind: ReplicaSubscription, ClientID: "consumer-1", TopicFilter: "sensores/+/temperatura"},
		&Promote{BrokerID: "broker-2"},
	}
}

func TestRoundTrip(t *testing.T) {
	for i, want := range samples() {
		frame, err := Encode(want)
		if err != nil {
			t.Fatalf("caso %d (%s): Encode: %v", i, want.Type(), err)
		}

		got, err := Decode(frame)
		if err != nil {
			t.Fatalf("caso %d (%s): Decode: %v", i, want.Type(), err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("caso %d (%s): obtido %#v, esperado %#v", i, want.Type(), got, want)
		}
	}
}

func TestReadMessageGlued(t *testing.T) {
	first := &Publish{Topic: "sensores/a", SensorID: "a", Seq: 1, AgeMS: 10, Temperature: 25.5}
	second := &Subscribe{TopicFilter: "sensores/+/temperatura"}

	var buf bytes.Buffer
	if err := WriteMessage(&buf, first); err != nil {
		t.Fatalf("WriteMessage first: %v", err)
	}
	if err := WriteMessage(&buf, second); err != nil {
		t.Fatalf("WriteMessage second: %v", err)
	}

	r := bufio.NewReader(&buf)
	gotFirst, err := ReadMessage(r)
	if err != nil {
		t.Fatalf("ReadMessage first: %v", err)
	}
	gotSecond, err := ReadMessage(r)
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
	first := &Deliver{Topic: "sensores/a", SensorID: "a", Seq: 7, TimestampMS: 1700000000000, Temperature: 26}
	second := &PubAck{Status: StatusPersisted, Seq: 7}

	var buf bytes.Buffer
	if err := WriteMessage(&buf, first); err != nil {
		t.Fatalf("WriteMessage first: %v", err)
	}
	if err := WriteMessage(&buf, second); err != nil {
		t.Fatalf("WriteMessage second: %v", err)
	}

	r := bufio.NewReader(iotest.OneByteReader(&buf))
	gotFirst, err := ReadMessage(r)
	if err != nil {
		t.Fatalf("ReadMessage first: %v", err)
	}
	gotSecond, err := ReadMessage(r)
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
	frame := make([]byte, HeaderSize)
	binary.BigEndian.PutUint32(frame[:4], MaxMessageSize+1)
	frame[4] = Version
	frame[5] = byte(TypeConnAck)

	if _, err := Decode(frame); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("Decode: esperado ErrMessageTooLarge, obtido %v", err)
	}

	r := bufio.NewReader(bytes.NewReader(frame[:4]))
	if _, err := ReadMessage(r); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("ReadMessage: esperado ErrMessageTooLarge, obtido %v", err)
	}
}

func TestUnsupportedVersion(t *testing.T) {
	frame := make([]byte, HeaderSize)
	binary.BigEndian.PutUint32(frame[:4], 4)
	frame[4] = Version + 1
	frame[5] = byte(TypeConnect)

	if _, err := Decode(frame); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("esperado ErrUnsupportedVersion, obtido %v", err)
	}
}

func TestUnknownType(t *testing.T) {
	frame := make([]byte, HeaderSize)
	binary.BigEndian.PutUint32(frame[:4], 4)
	frame[4] = Version
	frame[5] = 0xFF

	if _, err := Decode(frame); !errors.Is(err, ErrUnknownType) {
		t.Fatalf("esperado ErrUnknownType, obtido %v", err)
	}
}

func TestTruncatedPayload(t *testing.T) {
	frame := make([]byte, 10)
	binary.BigEndian.PutUint32(frame[:4], 6)
	frame[4] = Version
	frame[5] = byte(TypePublish)
	frame[8] = 0
	frame[9] = 10

	if _, err := Decode(frame); !errors.Is(err, ErrTruncated) {
		t.Fatalf("esperado ErrTruncated, obtido %v", err)
	}
}

func TestTrailingBytes(t *testing.T) {
	frame := make([]byte, 10)
	binary.BigEndian.PutUint32(frame[:4], 6)
	frame[4] = Version
	frame[5] = byte(TypeConnAck)
	frame[6] = 0
	frame[7] = 0
	frame[8] = byte(StatusOK)
	frame[9] = 0

	if _, err := Decode(frame); !errors.Is(err, ErrTrailingBytes) {
		t.Fatalf("esperado ErrTrailingBytes, obtido %v", err)
	}
}

func TestEncodeStringTooLong(t *testing.T) {
	msg := &Subscribe{TopicFilter: strings.Repeat("a", 70000)}
	if _, err := Encode(msg); !errors.Is(err, ErrStringTooLong) {
		t.Fatalf("esperado ErrStringTooLong, obtido %v", err)
	}
}

func FuzzDecode(f *testing.F) {
	frame, err := Encode(&Publish{Topic: "t", SensorID: "s", Seq: 1, AgeMS: 2, Temperature: 3.5})
	if err != nil {
		f.Fatalf("Encode: %v", err)
	}
	f.Add(frame)
	f.Fuzz(func(_ *testing.T, data []byte) {
		_, _ = Decode(data)
	})
}
