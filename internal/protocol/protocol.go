package protocol

import "errors"

const (
	Version uint8 = 3

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

// SensorState é o estado publicado no tópico sensores/{id}/status (RF-21). O
// valor zero marca uma leitura comum, que não carrega estado.
type SensorState uint8

const (
	SensorStateUnknown  SensorState = 0
	SensorStateOnline   SensorState = 1
	SensorStateDormindo SensorState = 2
	SensorStateOffline  SensorState = 3
)

func (s SensorState) String() string {
	switch s {
	case SensorStateUnknown:
		return "UNKNOWN"
	case SensorStateOnline:
		return "online"
	case SensorStateDormindo:
		return "dormindo"
	case SensorStateOffline:
		return "offline"
	default:
		return "UNKNOWN"
	}
}

// BrokerState é o campo state do HEARTBEAT (task 012). O valor zero é usado
// pelo Juiz no eco do heartbeat.
type BrokerState uint8

const (
	BrokerStateUnknown BrokerState = 0
	// BrokerStatePrimary atende clientes.
	BrokerStatePrimary BrokerState = 1
	// BrokerStateBackupSynced é a reserva com o estado completo do primário.
	BrokerStateBackupSynced BrokerState = 2
	// BrokerStateBackupSyncing é a reserva sem sincronização válida; não pode
	// ser promovida.
	BrokerStateBackupSyncing BrokerState = 3
	// BrokerStateFenced é o primário isolado: perdeu o Juiz e a reserva, ou
	// o Juiz indicou outro primário, e deixou de atender clientes.
	BrokerStateFenced BrokerState = 4
)

func (s BrokerState) String() string {
	switch s {
	case BrokerStatePrimary:
		return "primario"
	case BrokerStateBackupSynced:
		return "reserva_sincronizada"
	case BrokerStateBackupSyncing:
		return "reserva_sincronizando"
	case BrokerStateFenced:
		return "isolado"
	default:
		return "desconhecido"
	}
}

// JudgeID é o broker_id que o Juiz usa no eco do HEARTBEAT.
const JudgeID = "juiz"

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

// ReplicaKind identifica o conteúdo de um REPLICA. Os valores 1 a 3 (leitura,
// inscrição e cancelamento da versão 2) foram substituídos pelo envio do
// registro do store e não são mais aceitos.
type ReplicaKind uint8

const (
	// ReplicaRecord carrega um registro do store do primário, já gravado em
	// disco, para a reserva gravar e aplicar no próprio store.
	ReplicaRecord ReplicaKind = 4
	// ReplicaSyncStart abre uma sincronização completa: a reserva descarta o
	// estado anterior e acumula os registros seguintes.
	ReplicaSyncStart ReplicaKind = 5
	// ReplicaSyncEnd fecha a sincronização: a reserva grava o estado acumulado
	// e só então responde REPLICA_ACK.
	ReplicaSyncEnd ReplicaKind = 6
)

func (k ReplicaKind) String() string {
	switch k {
	case ReplicaRecord:
		return "RECORD"
	case ReplicaSyncStart:
		return "SYNC_START"
	case ReplicaSyncEnd:
		return "SYNC_END"
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
