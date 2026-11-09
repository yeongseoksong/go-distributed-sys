package log

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	api "github.com/yeongseoksong/proglog/api/v1"
)

// DistributedLog는 Log를 Raft로 감싸 복제되는 로그로 만든다.
// 쓰기는 Raft 합의를 거쳐 모든 노드에 복제되고, 읽기는 로컬 log에서 바로 처리한다.
type DistributedLog struct {
	config Config
	// log는 이 노드가 실제로 서비스하는 데이터. FSM이 Raft 커맨드를 적용하는 대상이다.
	log *Log
	// raftLog는 Raft의 복제 로그(아직 커밋되지 않은 엔트리 포함). log와는 별개의 디렉터리를 쓴다.
	raftLog *logStore
	raft    *raft.Raft
}

func NewDistributedLog(dataDir string, config Config) (*DistributedLog, error) {
	l := &DistributedLog{
		config: config,
	}
	// 순서 주의: setupRaft가 FSM에 l.log를 넘기므로 로컬 로그가 먼저 있어야 한다.
	if err := l.setupLog(dataDir); err != nil {
		return nil, err
	}

	if err := l.setupRaft(dataDir); err != nil {
		return nil, err
	}

	return l, nil
}

func (l *DistributedLog) setupLog(dataDir string) error {
	logDir := filepath.Join(dataDir, "log")

	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}

	var err error
	l.log, err = NewLog(logDir, l.config)
	return err
}

// setupRaft는 Raft가 요구하는 다섯 부품(FSM, 로그 저장소, 안정 저장소, 스냅샷 저장소, 전송 계층)을
// 준비해서 Raft 인스턴스를 띄운다.
func (l *DistributedLog) setupRaft(dataDir string) error {
	// FSM은 커밋된 커맨드를 로컬 로그에 적용하는 역할. 모든 노드가 같은 순서로 같은 커맨드를
	// 적용하므로 결국 모든 노드의 log가 같아진다.
	fsm := &fsm{log: l.log}
	logDir := filepath.Join(dataDir, "raft", "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}
	logConfig := l.config
	// Raft 로그 인덱스는 1부터 시작한다(0은 "엔트리 없음"을 뜻하는 예약값).
	// 우리 Log는 기본 0부터라 여기만 1로 맞춰준다.
	logConfig.Segment.InitialOffset = 1

	var err error
	l.raftLog, err = newLogStore(logDir, logConfig)
	if err != nil {
		return err
	}

	// stableStore에는 currentTerm과 투표 기록이 들어간다. append-only인 우리 로그로는
	// 덮어쓸 수 없어서 mutable KV인 BoltDB를 쓴다.
	// 투표 기록이 크래시 후에도 남아야 한 term에 두 번 투표하는 사고(리더 둘)를 막을 수 있다.
	stableStore, err := raftboltdb.NewBoltStore(
		filepath.Join(dataDir, "raft", "stable"),
	)
	if err != nil {
		return err
	}

	// retain은 보관할 스냅샷 개수. 최신 것 하나만 있으면 복구에는 충분하다.
	retain := 1
	snapshotStore, err := raft.NewFileSnapshotStore(
		filepath.Join(dataDir, "raft"),
		retain,
		os.Stderr,
	)
	if err != nil {
		return err
	}

	// StreamLayer가 노드 간 연결을 만들고 받는다. agent 쪽에서 cmux로 걸러낸 리스너가 들어온다.
	maxPool := 5
	timeout := 10 * time.Second
	transport := raft.NewNetworkTransport(
		l.config.Raft.StreamLayer,
		maxPool,
		timeout,
		os.Stderr,
	)

	config := raft.DefaultConfig()
	// LocalID는 클러스터 안에서 이 노드를 가리키는 고유 ID.
	// Serf의 NodeName과 같은 값이어야 Join/Leave가 같은 노드를 가리킨다.
	config.LocalID = l.config.Raft.LocalID
	// 아래 타임아웃들은 0이면 라이브러리 기본값을 쓴다. 테스트에서만 짧게 줄여 선거를 빨리 끝낸다.
	if l.config.Raft.HeartbeatTimeout != 0 {
		config.HeartbeatTimeout = l.config.Raft.HeartbeatTimeout
	}
	if l.config.Raft.ElectionTimeout != 0 {
		config.ElectionTimeout = l.config.Raft.ElectionTimeout
	}
	if l.config.Raft.LeaderLeaseTimeout != 0 {
		config.LeaderLeaseTimeout = l.config.Raft.LeaderLeaseTimeout
	}
	if l.config.Raft.CommitTimeout != 0 {
		config.CommitTimeout = l.config.Raft.CommitTimeout
	}

	l.raft, err = raft.NewRaft(
		config,
		fsm,
		l.raftLog,
		stableStore,
		snapshotStore,
		transport,
	)

	if err != nil {
		return err
	}

	// 이 노드가 전에 클러스터에 속한 적이 있는지 확인한다.
	hasState, err := raft.HasExistingState(
		l.raftLog,
		stableStore,
		snapshotStore,
	)
	if err != nil {
		return err
	}

	// 부트스트랩은 "나 혼자 있는 클러스터"를 새로 만드는 것이라 딱 한 번, 최초 노드에서만 해야 한다.
	// !hasState 조건이 없으면 재시작한 노드가 클러스터를 또 만들어 쪼개진다(split-brain).
	if l.config.Raft.Bootstrap && !hasState {
		config := raft.Configuration{
			Servers: []raft.Server{{
				ID:      config.LocalID,
				Address: transport.LocalAddr(),
			}},
		}
		err = l.raft.BootstrapCluster(config).Error()
	}

	return err
}

// Append는 레코드를 Raft 합의를 거쳐 추가한다. 리더에서만 성공하고,
// 반환된 시점에는 과반 노드에 복제되어 로컬 FSM까지 적용된 상태다.
func (l *DistributedLog) Append(record *api.Record) (uint64, error) {
	res, err := l.apply(
		AppendRequestType,
		&api.ProduceRequest{Record: record},
	)

	if err != nil {
		return 0, err
	}

	return res.(*api.ProduceResponse).Offset, nil
}

// apply는 요청을 바이트로 직렬화해 Raft에 밀어 넣고, FSM이 돌려준 응답을 꺼낸다.
func (l *DistributedLog) apply(reqType RequestType, req proto.Message) (interface{}, error) {
	var buf bytes.Buffer
	// 맨 앞 1바이트에 요청 종류를 넣는다. FSM.Apply가 이 바이트를 보고 어떤 커맨드인지 판단한다.
	// 나중에 커맨드 종류가 늘어나도 같은 로그에 섞어 담을 수 있게 하는 장치.
	_, err := buf.Write([]byte{byte(reqType)})
	if err != nil {
		return nil, err
	}
	b, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	_, err = buf.Write(b)
	if err != nil {
		return nil, err
	}

	timeout := 10 * time.Second
	future := l.raft.Apply(buf.Bytes(), timeout)
	// future.Error()는 복제 자체의 실패(리더 아님, 타임아웃 등)를 알려준다.
	if future.Error() != nil {
		return nil, future.Error()
	}

	// Response()는 FSM.Apply가 반환한 값. 비즈니스 로직 에러는 여기에 error로 담겨 온다.
	// 위의 future.Error()와 층이 다르므로 따로 확인해야 한다.
	res := future.Response()
	if err, ok := res.(error); ok {
		return nil, err
	}

	return res, nil
}

// Read는 Raft를 거치지 않고 로컬 로그에서 바로 읽는다.
// 빠른 대신 강한 일관성이 없다. 팔로워는 아직 복제가 안 닿아 예전 데이터를 줄 수 있다.
func (l *DistributedLog) Read(offset uint64) (*api.Record, error) {
	return l.log.Read(offset)
}

// Join은 노드를 클러스터에 투표권자로 추가한다. discovery.Handler 구현이라
// Serf가 새 멤버를 발견하면 호출된다. 리더가 아닌 노드에서는 ErrNotLeader로 실패하는데,
// 이건 정상이라 호출 측(Membership.logError)에서 Debug로 낮춰 처리한다.
func (l *DistributedLog) Join(id, addr string) error {
	configFuture := l.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}

	serverID := raft.ServerID(id)
	serverAddr := raft.ServerAddress(addr)

	// Serf 이벤트는 중복 전달될 수 있고, 노드가 재시작하며 주소가 바뀔 수도 있다.
	// 그래서 이미 있는 노드인지 먼저 확인한다.
	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == serverID || srv.Address == serverAddr {
			if srv.ID == serverID && srv.Address == serverAddr {
				// ID와 주소가 모두 같으면 이미 원하는 상태. 아무것도 하지 않는다(멱등).
				return nil
			}

			// 둘 중 하나만 같다면 주소가 바뀐 것이므로 지우고 아래에서 다시 추가한다.
			removeFuture := l.raft.RemoveServer(serverID, 0, 0)
			if err := removeFuture.Error(); err != nil {
				return err
			}
		}
	}
	// Voter로 추가하면 이 노드가 선거와 과반 계산에 참여한다.
	addFuture := l.raft.AddVoter(serverID, serverAddr, 0, 0)
	if err := addFuture.Error(); err != nil {
		return err
	}

	return nil
}

// Leave는 노드를 클러스터 구성에서 제거한다. 남은 노드로 과반이 다시 계산된다.
func (l *DistributedLog) Leave(id string) error {
	removeFuture := l.raft.RemoveServer(raft.ServerID(id), 0, 0)
	return removeFuture.Error()
}

// WaitForLeader는 리더가 뽑힐 때까지 기다린다.
// 부트스트랩 직후 바로 쓰기를 시도하면 ErrNotLeader가 나기 때문에 필요하다.
func (l *DistributedLog) WaitForLeader(timeout time.Duration) error {
	timeoutc := time.After(timeout)
	ticker := time.NewTicker(time.Second)

	for {
		select {
		case <-timeoutc:
			return fmt.Errorf("timed out")
		case <-ticker.C:
			if l := l.raft.Leader(); l != "" {
				return nil
			}
		}
	}
}

func (l *DistributedLog) Close() error {
	// Raft를 먼저 내려야 종료 중에 FSM이 로그에 쓰는 일을 막을 수 있다.
	f := l.raft.Shutdown()
	if err := f.Error(); err != nil {
		return err
	}

	if err := l.raftLog.Close(); err != nil {
		return err
	}

	return l.log.Close()
}

// 컴파일 타임에 인터페이스 구현 여부를 검사한다. 메서드 시그니처가 어긋나면 여기서 바로 잡힌다.
var _ raft.FSM = (*fsm)(nil)

// fsm은 Raft가 커밋한 커맨드를 로컬 로그에 반영하는 상태 기계다.
// 모든 노드가 같은 커맨드를 같은 순서로 적용하므로 상태가 일치하게 된다.
type fsm struct {
	log *Log
}

type RequestType uint8

const (
	AppendRequestType RequestType = 0
)

// Apply는 엔트리가 커밋된 뒤 각 노드에서 호출된다.
// 반환값은 그 커맨드를 제출한 노드(리더)의 Apply future로만 전달된다.
func (f *fsm) Apply(record *raft.Log) interface{} {
	buf := record.Data
	// apply()에서 앞에 붙여 둔 종류 바이트를 꺼내 분기한다.
	reqType := RequestType(buf[0])
	switch reqType {
	case AppendRequestType:
		return f.applyAppend(buf[1:])
	}
	return nil
}

func (f *fsm) applyAppend(b []byte) interface{} {
	var req api.ProduceRequest
	err := proto.Unmarshal(b, &req)
	if err != nil {
		return nil
	}

	offset, err := f.log.Append(req.Record)
	if err != nil {
		return nil
	}

	return &api.ProduceResponse{Offset: offset}

}

// Snapshot은 현재 상태의 스냅샷을 뜬다. 스냅샷이 있으면 오래된 Raft 로그를 지울 수 있고,
// 새 노드가 합류할 때 로그를 처음부터 재생하지 않아도 된다.
// Raft가 FSM을 잠그는 시간을 줄이려고 여기서는 Reader만 잡고 실제 쓰기는 Persist에서 한다.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	r := f.log.Reader()
	return &snapshot{reader: r}, nil
}

var _ raft.FSMSnapshot = (*snapshot)(nil)

type snapshot struct {
	reader io.Reader
}

// Persist는 스냅샷 내용을 sink에 쓴다. 실패하면 Cancel로 반쯤 쓰인 스냅샷을 버려야
// 나중에 깨진 스냅샷으로 복구하는 일이 없다.
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := io.Copy(sink, s.reader); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *snapshot) Release() {}

// Restore는 스냅샷으로 상태를 되돌린다. Persist가 쓴 형식(길이 8바이트 + 레코드)을 그대로 읽는다.
func (f *fsm) Restore(r io.ReadCloser) error {
	b := make([]byte, lenWidth)
	var buf bytes.Buffer
	for i := 0; ; i++ {
		_, err := io.ReadFull(r, b)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		size := int64(enc.Uint64(b))
		if _, err = io.CopyN(&buf, r, size); err != nil {
			return err
		}
		record := &api.Record{}
		if err = proto.Unmarshal(buf.Bytes(), record); err != nil {
			return err
		}

		// 첫 레코드에서 기존 로그를 버리고 스냅샷의 시작 오프셋으로 새로 연다.
		// 스냅샷이 0이 아닌 오프셋에서 시작할 수 있으므로 오프셋을 맞춰야 한다.
		if i == 0 {
			f.log.Config.Segment.InitialOffset = record.Offset
			if err := f.log.Reset(); err != nil {
				return err
			}
		}

		if _, err = f.log.Append(record); err != nil {
			return err
		}

		buf.Reset()
	}

	return nil
}

var _ raft.LogStore = (*logStore)(nil)

// logStore는 우리가 만든 Log를 Raft의 LogStore 인터페이스에 맞춰주는 어댑터다.
// 새 저장소를 만드는 대신 이미 있는 세그먼트 로그를 재사용한다.
type logStore struct {
	*Log
}

func newLogStore(dir string, c Config) (*logStore, error) {
	log, err := NewLog(dir, c)
	if err != nil {
		return nil, err
	}

	return &logStore{log}, nil
}

// Raft의 "인덱스"가 우리 로그의 "오프셋"에 그대로 대응된다.
func (l *logStore) FirstIndex() (uint64, error) {
	return l.LowestOffset()
}

func (l *logStore) LastIndex() (uint64, error) {
	off, err := l.HighestOffset()
	return off, err
}

// GetLog는 우리 Record를 raft.Log로 옮겨 담는다.
// Term과 Type은 Raft가 쓰는 필드라 log.proto에 따로 추가해 둔 것이다.
func (l *logStore) GetLog(index uint64, out *raft.Log) error {
	in, err := l.Read(index)
	if err != nil {
		return err
	}

	out.Data = in.Value
	out.Index = in.Offset
	out.Type = raft.LogType(in.Type)
	out.Term = in.Term
	return nil
}

func (l *logStore) StoreLog(record *raft.Log) error {
	return l.StoreLogs([]*raft.Log{record})
}

func (l *logStore) StoreLogs(records []*raft.Log) error {
	for _, record := range records {
		if _, err := l.Append(&api.Record{
			Value: record.Data,
			Term:  record.Term,
			Type:  uint32(record.Type),
		}); err != nil {
			return err
		}
	}
	return nil
}

// DeleteRange는 스냅샷 이후 필요 없어진 오래된 엔트리를 지울 때 Raft가 호출한다.
// 우리 로그는 앞에서부터 지우는 것만 지원하므로 min은 무시하고 max까지 잘라낸다.
func (l *logStore) DeleteRange(min, max uint64) error {
	return l.Truncate(max)
}

var _ raft.StreamLayer = (*StreamLayer)(nil)

// StreamLayer는 Raft 노드 간 연결을 만들고 받는다.
// 포트 하나를 gRPC와 나눠 쓰기 위해 연결 앞에 1바이트 식별자를 붙이고, TLS도 여기서 건다.
type StreamLayer struct {
	ln net.Listener
	// serverTLSConfig는 들어오는 연결을, peerTLSConfig는 나가는 연결을 감쌀 때 쓴다.
	serverTLSConfig *tls.Config
	peerTLSConfig   *tls.Config
}

func NewStreamLayer(
	ln net.Listener,
	serverTLSConfig,
	peerTLSConfig *tls.Config,
) *StreamLayer {
	return &StreamLayer{
		ln:              ln,
		serverTLSConfig: serverTLSConfig,
		peerTLSConfig:   peerTLSConfig,
	}
}

// RaftRPC는 이 연결이 Raft용임을 알리는 식별 바이트.
// agent의 cmux 매처가 같은 값으로 연결을 골라낸다.
const RaftRPC = 1

func (s *StreamLayer) Dial(
	addr raft.ServerAddress,
	timeout time.Duration,
) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	var conn, err = dialer.Dial("tcp", string(addr))
	if err != nil {
		return nil, err
	}

	// TLS를 걸기 전에 평문으로 식별 바이트를 먼저 보낸다.
	// 상대방 cmux가 이 바이트를 읽어야 어느 리스너로 보낼지 판단할 수 있기 때문이다.
	_, err = conn.Write([]byte{byte(RaftRPC)})
	if err != nil {
		return nil, err
	}

	if s.peerTLSConfig != nil {
		conn = tls.Client(conn, s.peerTLSConfig)
	}

	return conn, err

}

func (s *StreamLayer) Accept() (net.Conn, error) {
	conn, err := s.ln.Accept()
	if err != nil {
		return nil, err
	}

	// cmux가 매칭하며 읽은 바이트를 되돌려주므로 여기서 다시 읽을 수 있다.
	// 이 바이트를 소비해야 뒤이은 TLS 핸드셰이크가 올바른 위치에서 시작한다.
	b := make([]byte, 1)
	_, err = conn.Read(b)
	if err != nil {
		return nil, err
	}

	if bytes.Compare([]byte{byte(RaftRPC)}, b) != 0 {
		return nil, fmt.Errorf("not a raft rpc")
	}

	if s.serverTLSConfig != nil {
		return tls.Server(conn, s.serverTLSConfig), nil
	}

	return conn, nil
}

func (s *StreamLayer) Close() error {
	return s.ln.Close()
}

// Addr은 이 노드의 Raft 주소. 부트스트랩할 때 자기 자신의 주소로 쓰인다.
func (s *StreamLayer) Addr() net.Addr {
	return s.ln.Addr()
}
