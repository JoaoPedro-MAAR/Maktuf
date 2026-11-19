package protocol

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

type Message interface {
	Type() Type
	encodePayload(*encoder)
	decodePayload(*decoder)
}

// flagMessage é implementada pelos tipos que usam o campo flags do cabeçalho.
// Nos demais tipos ele permanece reservado e é enviado como 0.
type flagMessage interface {
	headerFlags() uint16
	setHeaderFlags(uint16)
}

func Encode(msg Message) ([]byte, error) {
	e := &encoder{}
	msg.encodePayload(e)
	if e.err != nil {
		return nil, e.err
	}

	length := 4 + len(e.buf)
	if length > MaxMessageSize {
		return nil, fmt.Errorf("%w: %d bytes", ErrMessageTooLarge, length)
	}

	var flags uint16
	if fm, ok := msg.(flagMessage); ok {
		flags = fm.headerFlags()
	}

	frame := make([]byte, 0, 4+length)
	frame = binary.BigEndian.AppendUint32(frame, uint32(length))
	frame = append(frame, Version, byte(msg.Type()))
	frame = binary.BigEndian.AppendUint16(frame, flags)
	frame = append(frame, e.buf...)
	return frame, nil
}

func Decode(frame []byte) (Message, error) {
	if len(frame) < HeaderSize {
		return nil, ErrTruncated
	}

	length := binary.BigEndian.Uint32(frame[:4])
	if length > MaxMessageSize {
		return nil, ErrMessageTooLarge
	}
	if int(length) != len(frame)-4 {
		return nil, ErrTruncated
	}

	if version := frame[4]; version != Version {
		return nil, fmt.Errorf("%w: %d", ErrUnsupportedVersion, version)
	}

	msg := newMessage(Type(frame[5]))
	if msg == nil {
		return nil, fmt.Errorf("%w: 0x%02x", ErrUnknownType, frame[5])
	}
	if fm, ok := msg.(flagMessage); ok {
		fm.setHeaderFlags(binary.BigEndian.Uint16(frame[6:8]))
	}

	d := &decoder{buf: frame[HeaderSize:]}
	msg.decodePayload(d)
	if err := d.done(); err != nil {
		return nil, err
	}
	return msg, nil
}

func ReadMessage(r *bufio.Reader) (Message, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}

	length := binary.BigEndian.Uint32(header[:])
	if length > MaxMessageSize {
		return nil, ErrMessageTooLarge
	}

	frame := make([]byte, 4+int(length))
	copy(frame, header[:])
	if _, err := io.ReadFull(r, frame[4:]); err != nil {
		return nil, err
	}
	return Decode(frame)
}

func WriteMessage(w io.Writer, msg Message) error {
	frame, err := Encode(msg)
	if err != nil {
		return err
	}

	for len(frame) > 0 {
		n, err := w.Write(frame)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		frame = frame[n:]
	}
	return nil
}

func newMessage(t Type) Message {
	switch t {
	case TypeConnect:
		return &Connect{}
	case TypeConnAck:
		return &ConnAck{}
	case TypePublish:
		return &Publish{}
	case TypePubAck:
		return &PubAck{}
	case TypeSubscribe:
		return &Subscribe{}
	case TypeSubAck:
		return &SubAck{}
	case TypeUnsubscribe:
		return &Unsubscribe{}
	case TypeUnsubAck:
		return &UnsubAck{}
	case TypeDeliver:
		return &Deliver{}
	case TypeDeliverAck:
		return &DeliverAck{}
	case TypeSleep:
		return &Sleep{}
	case TypeHeartbeat:
		return &Heartbeat{}
	case TypeReplica:
		return &Replica{}
	case TypeReplicaAck:
		return &ReplicaAck{}
	case TypePromote:
		return &Promote{}
	default:
		return nil
	}
}

type encoder struct {
	buf []byte
	err error
}

func (e *encoder) u8(v uint8) {
	if e.err != nil {
		return
	}
	e.buf = append(e.buf, v)
}

func (e *encoder) u16(v uint16) {
	if e.err != nil {
		return
	}
	e.buf = binary.BigEndian.AppendUint16(e.buf, v)
}

func (e *encoder) u32(v uint32) {
	if e.err != nil {
		return
	}
	e.buf = binary.BigEndian.AppendUint32(e.buf, v)
}

func (e *encoder) u64(v uint64) {
	if e.err != nil {
		return
	}
	e.buf = binary.BigEndian.AppendUint64(e.buf, v)
}

func (e *encoder) i64(v int64) {
	e.u64(uint64(v))
}

func (e *encoder) f32(v float32) {
	e.u32(math.Float32bits(v))
}

func (e *encoder) str(s string) {
	if e.err != nil {
		return
	}
	if len(s) > math.MaxUint16 {
		e.err = fmt.Errorf("%w: %d bytes", ErrStringTooLong, len(s))
		return
	}
	e.u16(uint16(len(s)))
	e.buf = append(e.buf, s...)
}

type decoder struct {
	buf []byte
	off int
	err error
}

func (d *decoder) u8() uint8 {
	if d.err != nil {
		return 0
	}
	if d.off+1 > len(d.buf) {
		d.err = ErrTruncated
		return 0
	}
	v := d.buf[d.off]
	d.off++
	return v
}

func (d *decoder) u16() uint16 {
	if d.err != nil {
		return 0
	}
	if d.off+2 > len(d.buf) {
		d.err = ErrTruncated
		return 0
	}
	v := binary.BigEndian.Uint16(d.buf[d.off:])
	d.off += 2
	return v
}

func (d *decoder) u32() uint32 {
	if d.err != nil {
		return 0
	}
	if d.off+4 > len(d.buf) {
		d.err = ErrTruncated
		return 0
	}
	v := binary.BigEndian.Uint32(d.buf[d.off:])
	d.off += 4
	return v
}

func (d *decoder) u64() uint64 {
	if d.err != nil {
		return 0
	}
	if d.off+8 > len(d.buf) {
		d.err = ErrTruncated
		return 0
	}
	v := binary.BigEndian.Uint64(d.buf[d.off:])
	d.off += 8
	return v
}

func (d *decoder) i64() int64 {
	return int64(d.u64())
}

func (d *decoder) f32() float32 {
	return math.Float32frombits(d.u32())
}

func (d *decoder) str() string {
	n := d.u16()
	if d.err != nil {
		return ""
	}
	if d.off+int(n) > len(d.buf) {
		d.err = ErrTruncated
		return ""
	}
	s := string(d.buf[d.off : d.off+int(n)])
	d.off += int(n)
	return s
}

func (d *decoder) done() error {
	if d.err != nil {
		return d.err
	}
	if d.off != len(d.buf) {
		return ErrTrailingBytes
	}
	return nil
}
