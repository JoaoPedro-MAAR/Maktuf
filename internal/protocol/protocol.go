package protocol

import "errors"

const (
	Version uint8 = 1

	MaxMessageSize = 1 << 20

	HeaderSize = 8
)

type Type uint8

const (
	TypeConnect     Type = 0x01
	TypeConnAck     Type = 0x02
	TypePublish     Type = 0x03
	TypePubAck      Type = 0x04
	TypeSubscribe   Type = 0x05
	TypeSubAck      Type = 0x06
	TypeUnsubscribe Type = 0x07
	TypeUnsubAck    Type = 0x08
	TypeDeliver     Type = 0x09
	TypeDeliverAck  Type = 0x0A
	TypeSleep       Type = 0x0B
	TypeHeartbeat   Type = 0x0C
	TypeReplica     Type = 0x0D
	TypeReplicaAck  Type = 0x0E
	TypePromote     Type = 0x0F
)

func (t Type) String() string {
	switch t {
	case TypeConnect:
		return "CONNECT"
	case TypeConnAck:
		return "CONNACK"
	case TypePublish:
		return "PUBLISH"
	case TypePubAck:
		return "PUBACK"
	case TypeSubscribe:
		return "SUBSCRIBE"
	case TypeSubAck:
		return "SUBACK"
	case TypeUnsubscribe:
		return "UNSUBSCRIBE"
	case TypeUnsubAck:
		return "UNSUBACK"
	case TypeDeliver:
		return "DELIVER"
	case TypeDeliverAck:
		return "DELIVER_ACK"
	case TypeSleep:
		return "SLEEP"
	case TypeHeartbeat:
		return "HEARTBEAT"
	case TypeReplica:
		return "REPLICA"
	case TypeReplicaAck:
		return "REPLICA_ACK"
	case TypePromote:
		return "PROMOTE"
	default:
		return "UNKNOWN"
	}
}

type Status uint8

const (
	StatusOK               Status = 0
	StatusPersisted        Status = 1
	StatusNoSubscribers    Status = 2
	StatusTemporaryFailure Status = 3
)

func (s Status) String() string {
	switch s {
	case StatusOK:
		return "OK"
	case StatusPersisted:
		return "PERSISTED"
	case StatusNoSubscribers:
		return "NO_SUBSCRIBERS"
	case StatusTemporaryFailure:
		return "TEMPORARY_FAILURE"
	default:
		return "UNKNOWN"
	}
}

type Role uint8

const (
	RoleSensor   Role = 1
	RoleConsumer Role = 2
)

func (r Role) String() string {
	switch r {
	case RoleSensor:
		return "SENSOR"
	case RoleConsumer:
		return "CONSUMER"
	default:
		return "UNKNOWN"
	}
}

type ReplicaKind uint8

const (
	ReplicaReading      ReplicaKind = 1
	ReplicaSubscription ReplicaKind = 2
	ReplicaUnsubscribe  ReplicaKind = 3
)

func (k ReplicaKind) String() string {
	switch k {
	case ReplicaReading:
		return "READING"
	case ReplicaSubscription:
		return "SUBSCRIPTION"
	case ReplicaUnsubscribe:
		return "UNSUBSCRIBE"
	default:
		return "UNKNOWN"
	}
}

var (
	ErrTruncated = errors.New("protocol: mensagem truncada")

	ErrTrailingBytes = errors.New("protocol: bytes extras no payload")

	ErrMessageTooLarge = errors.New("protocol: mensagem maior que o limite")

	ErrUnsupportedVersion = errors.New("protocol: versao nao suportada")

	ErrUnknownType = errors.New("protocol: tipo de mensagem desconhecido")

	ErrUnknownReplicaKind = errors.New("protocol: tipo de replicacao desconhecido")

	ErrStringTooLong = errors.New("protocol: string maior que 65535 bytes")
)
