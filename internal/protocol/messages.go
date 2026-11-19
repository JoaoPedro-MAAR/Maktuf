package protocol

type Connect struct {
	Role     Role
	ClientID string
}

func (*Connect) Type() Type { return TypeConnect }

func (m *Connect) encodePayload(e *encoder) {
	e.u8(uint8(m.Role))
	e.str(m.ClientID)
}

func (m *Connect) decodePayload(d *decoder) {
	m.Role = Role(d.u8())
	m.ClientID = d.str()
}

type ConnAck struct {
	Status Status
}

func (*ConnAck) Type() Type { return TypeConnAck }

func (m *ConnAck) encodePayload(e *encoder) {
	e.u8(uint8(m.Status))
}

func (m *ConnAck) decodePayload(d *decoder) {
	m.Status = Status(d.u8())
}

type Publish struct {
	Topic       string
	SensorID    string
	Seq         uint64
	AgeMS       uint32
	Temperature float32
}

func (*Publish) Type() Type { return TypePublish }

func (m *Publish) encodePayload(e *encoder) {
	e.str(m.Topic)
	e.str(m.SensorID)
	e.u64(m.Seq)
	e.u32(m.AgeMS)
	e.f32(m.Temperature)
}

func (m *Publish) decodePayload(d *decoder) {
	m.Topic = d.str()
	m.SensorID = d.str()
	m.Seq = d.u64()
	m.AgeMS = d.u32()
	m.Temperature = d.f32()
}

type PubAck struct {
	Status Status
	Seq    uint64
}

func (*PubAck) Type() Type { return TypePubAck }

func (m *PubAck) encodePayload(e *encoder) {
	e.u8(uint8(m.Status))
	e.u64(m.Seq)
}

func (m *PubAck) decodePayload(d *decoder) {
	m.Status = Status(d.u8())
	m.Seq = d.u64()
}

type Subscribe struct {
	TopicFilter string
}

func (*Subscribe) Type() Type { return TypeSubscribe }

func (m *Subscribe) encodePayload(e *encoder) {
	e.str(m.TopicFilter)
}

func (m *Subscribe) decodePayload(d *decoder) {
	m.TopicFilter = d.str()
}

type SubAck struct {
	Status Status
}

func (*SubAck) Type() Type { return TypeSubAck }

func (m *SubAck) encodePayload(e *encoder) {
	e.u8(uint8(m.Status))
}

func (m *SubAck) decodePayload(d *decoder) {
	m.Status = Status(d.u8())
}

type Unsubscribe struct {
	TopicFilter string
}

func (*Unsubscribe) Type() Type { return TypeUnsubscribe }

func (m *Unsubscribe) encodePayload(e *encoder) {
	e.str(m.TopicFilter)
}

func (m *Unsubscribe) decodePayload(d *decoder) {
	m.TopicFilter = d.str()
}

type UnsubAck struct {
	Status Status
}

func (*UnsubAck) Type() Type { return TypeUnsubAck }

func (m *UnsubAck) encodePayload(e *encoder) {
	e.u8(uint8(m.Status))
}

func (m *UnsubAck) decodePayload(d *decoder) {
	m.Status = Status(d.u8())
}

// DeliverFlagGap marca que leituras do mesmo sensor foram descartadas por
// retenção antes desta entrega (RF-37, CE-11).
const DeliverFlagGap uint16 = 0x0001

type Deliver struct {
	Topic       string
	SensorID    string
	Seq         uint64
	TimestampMS int64
	Temperature float32
	Flags       uint16
}

func (*Deliver) Type() Type { return TypeDeliver }

func (m *Deliver) headerFlags() uint16 { return m.Flags }

func (m *Deliver) setHeaderFlags(flags uint16) { m.Flags = flags }

func (m *Deliver) encodePayload(e *encoder) {
	e.str(m.Topic)
	e.str(m.SensorID)
	e.u64(m.Seq)
	e.i64(m.TimestampMS)
	e.f32(m.Temperature)
}

func (m *Deliver) decodePayload(d *decoder) {
	m.Topic = d.str()
	m.SensorID = d.str()
	m.Seq = d.u64()
	m.TimestampMS = d.i64()
	m.Temperature = d.f32()
}

type DeliverAck struct {
	Topic    string
	SensorID string
	Seq      uint64
}

func (*DeliverAck) Type() Type { return TypeDeliverAck }

func (m *DeliverAck) encodePayload(e *encoder) {
	e.str(m.Topic)
	e.str(m.SensorID)
	e.u64(m.Seq)
}

func (m *DeliverAck) decodePayload(d *decoder) {
	m.Topic = d.str()
	m.SensorID = d.str()
	m.Seq = d.u64()
}

type Sleep struct {
	DurationMS uint32
}

func (*Sleep) Type() Type { return TypeSleep }

func (m *Sleep) encodePayload(e *encoder) {
	e.u32(m.DurationMS)
}

func (m *Sleep) decodePayload(d *decoder) {
	m.DurationMS = d.u32()
}

type Heartbeat struct {
	BrokerID string
	State    uint8
	LastSeq  uint64
}

func (*Heartbeat) Type() Type { return TypeHeartbeat }

func (m *Heartbeat) encodePayload(e *encoder) {
	e.str(m.BrokerID)
	e.u8(m.State)
	e.u64(m.LastSeq)
}

func (m *Heartbeat) decodePayload(d *decoder) {
	m.BrokerID = d.str()
	m.State = d.u8()
	m.LastSeq = d.u64()
}

type Replica struct {
	Kind        ReplicaKind
	Topic       string
	SensorID    string
	Seq         uint64
	TimestampMS int64
	Temperature float32
	ClientID    string
	TopicFilter string
}

func (*Replica) Type() Type { return TypeReplica }

func (m *Replica) encodePayload(e *encoder) {
	e.u8(uint8(m.Kind))
	switch m.Kind {
	case ReplicaReading:
		e.str(m.Topic)
		e.str(m.SensorID)
		e.u64(m.Seq)
		e.i64(m.TimestampMS)
		e.f32(m.Temperature)
	case ReplicaSubscription, ReplicaUnsubscribe:
		e.str(m.ClientID)
		e.str(m.TopicFilter)
	default:
		e.err = ErrUnknownReplicaKind
	}
}

func (m *Replica) decodePayload(d *decoder) {
	m.Kind = ReplicaKind(d.u8())
	switch m.Kind {
	case ReplicaReading:
		m.Topic = d.str()
		m.SensorID = d.str()
		m.Seq = d.u64()
		m.TimestampMS = d.i64()
		m.Temperature = d.f32()
	case ReplicaSubscription, ReplicaUnsubscribe:
		m.ClientID = d.str()
		m.TopicFilter = d.str()
	default:
		if d.err == nil {
			d.err = ErrUnknownReplicaKind
		}
	}
}

type ReplicaAck struct {
	Status      Status
	Kind        ReplicaKind
	SensorID    string
	Seq         uint64
	ClientID    string
	TopicFilter string
}

func (*ReplicaAck) Type() Type { return TypeReplicaAck }

func (m *ReplicaAck) encodePayload(e *encoder) {
	e.u8(uint8(m.Status))
	e.u8(uint8(m.Kind))
	switch m.Kind {
	case ReplicaReading:
		e.str(m.SensorID)
		e.u64(m.Seq)
	case ReplicaSubscription, ReplicaUnsubscribe:
		e.str(m.ClientID)
		e.str(m.TopicFilter)
	default:
		e.err = ErrUnknownReplicaKind
	}
}

func (m *ReplicaAck) decodePayload(d *decoder) {
	m.Status = Status(d.u8())
	m.Kind = ReplicaKind(d.u8())
	switch m.Kind {
	case ReplicaReading:
		m.SensorID = d.str()
		m.Seq = d.u64()
	case ReplicaSubscription, ReplicaUnsubscribe:
		m.ClientID = d.str()
		m.TopicFilter = d.str()
	default:
		if d.err == nil {
			d.err = ErrUnknownReplicaKind
		}
	}
}

type Promote struct {
	BrokerID string
}

func (*Promote) Type() Type { return TypePromote }

func (m *Promote) encodePayload(e *encoder) {
	e.str(m.BrokerID)
}

func (m *Promote) decodePayload(d *decoder) {
	m.BrokerID = d.str()
}
