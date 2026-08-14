package server

import (
	"context"
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	api "github.com/yeongseoksong/proglog/api/v1"
	"github.com/yeongseoksong/proglog/internal/auth"
	"github.com/yeongseoksong/proglog/internal/config"
	"github.com/yeongseoksong/proglog/internal/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

// testScenario is a reusable signature for table-driven server tests.
type testScenario func(t *testing.T, rootClient api.LogClient, nobodyClient api.LogClient, config *Config)

// TestServer는 테이블 주도(table-driven) 방식으로 각 시나리오를 서브테스트로 실행한다.
// 시나리오마다 setupTest을 새로 호출하므로 서버/로그가 매번 초기화되어
// 테스트 간 상태 공유로 인한 순서 의존성이 생기지 않는다.
func TestServer(t *testing.T) {

	for senario, fn := range map[string]testScenario{
		"produce/consume amessage to from the log succeeds": testProduceConsume,
		"produce/consume stream succeeds":                   testProduceConsumeStream,
		"consume past log boundary fails":                   testConsumePastBoundary,
		"unauthorized fails":                                testUnauthorized,
	} {
		t.Run(senario, func(t *testing.T) {
			rootClient, nobodyClient, config, teardown := setupTest(t, nil)
			// 서브테스트가 끝나면 서버 종료 + 임시 디렉터리 삭제
			defer teardown()
			fn(t, rootClient, nobodyClient, config)
		})
	}

}

// setupTest은 실제 TCP 위에서 동작하는 gRPC 서버와 그에 연결된 클라이언트를 준비한다.
// 목(mock)이 아닌 진짜 네트워크 스택을 쓰므로 직렬화·스트리밍·상태 코드 전파까지 함께 검증된다.
//
// fn은 서버를 띄우기 직전 Config를 수정할 수 있는 훅이다.
// (이후 인증/ACL 챕터에서 테스트별로 다른 설정을 주입하는 데 쓰인다.)
func setupTest(t *testing.T, fn func(*Config)) (rootClient api.LogClient, nobodyClient api.LogClient, cfg *Config, teardown func()) {

	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	newClient := func(crtPath, keyPath string) (*grpc.ClientConn, api.LogClient, []grpc.DialOption) {
		tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
			CertFile: crtPath,
			KeyFile:  keyPath,
			CAFile:   config.CAFile,
			Server:   false,
		})
		require.NoError(t, err)
		tlsCreds := credentials.NewTLS(tlsConfig)
		opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
		conn, err := grpc.NewClient(
			l.Addr().String(),
			opts...,
		)
		require.NoError(t, err)
		client := api.NewLogClient(conn)
		return conn, client, opts

	}

	var rootConn *grpc.ClientConn
	rootConn, rootClient, _ = newClient(
		config.RootClientCertFile,
		config.RootClientKeyFile,
	)

	var nobodyConn *grpc.ClientConn
	nobodyConn, nobodyClient, _ = newClient(
		config.NobodyClientCertFile,
		config.NobodyClientKeyFile,
	)

	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: l.Addr().String(),
		Server:        true,
	})

	require.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTLSConfig)

	dir, err := os.MkdirTemp("", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	authorizer := auth.New(config.ACLModelFile, config.ACLPolicyFile)

	cfg = &Config{CommitLog: clog, Authorizer: authorizer}
	if fn != nil {
		fn(cfg)
	}

	server, err := NewGRPCSever(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)

	go func() {
		server.Serve(l)
	}()

	return rootClient, nobodyClient, cfg, func() {
		server.Stop()
		rootConn.Close()
		nobodyConn.Close()
		l.Close()
		clog.Remove()
		os.RemoveAll(dir)
	}
}

// testProduceConsume은 단건 쓰기/읽기 왕복(round-trip)을 검증한다.
// Produce가 돌려준 오프셋으로 Consume했을 때 같은 레코드가 나와야 한다.
func testProduceConsume(t *testing.T, client, _ api.LogClient, config *Config) {
	ctx := context.Background()
	want := &api.Record{Value: []byte("hello world")}

	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: want})

	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset})

	require.NoError(t, err)
	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, consume.Record.Offset)

}

// testConsumePastBoundary는 로그 끝을 넘어선 오프셋을 읽을 때
// 서버가 의도한 gRPC 상태 코드를 돌려주는지 확인한다.
//
// 도메인 에러(ErrOffsetOutOfRange)가 네트워크 경계를 넘어 클라이언트까지
// 코드로 보존되는지가 핵심이다. 에러 타입에 GRPCStatus() 메서드가 없으면
// gRPC가 codes.Unknown으로 뭉개버리므로 이 테스트가 그 회귀를 잡아준다.
func testConsumePastBoundary(t *testing.T, client, _ api.LogClient, config *Config) {
	ctx := context.Background()

	produce, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("hello world"),
		},
	})
	require.NoError(t, err)

	// 방금 쓴 오프셋 +1 = 아직 존재하지 않는 위치
	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset + 1})
	if consume != nil {
		t.Fatal("consume not nil")
	}
	got := status.Code(err)

	want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	if got != want {
		t.Fatalf("got err: %v, want: %v", got, want)
	}

}

// testProduceConsumeStream은 양방향/서버 스트리밍 RPC를 검증한다.
// 블록 스코프({})로 쓰기 구간과 읽기 구간을 나눠 stream 변수를 격리했다.
func testProduceConsumeStream(t *testing.T, client, _ api.LogClient, config *Config) {
	ctx := context.Background()

	records := []*api.Record{{
		Value:  []byte("first message"),
		Offset: 0,
	}, {
		Value:  []byte("second message"),
		Offset: 1,
	}}
	{
		// ProduceStream: 양방향 스트리밍.
		// 여기서는 Send 직후 Recv해 응답을 하나씩 확인하지만,
		// 실제로는 여러 건을 몰아 보낸 뒤 나중에 받아도 된다.
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)

		for offset, record := range records {
			err = stream.Send(&api.ProduceRequest{Record: record})
			require.NoError(t, err)
			res, err := stream.Recv()
			require.NoError(t, err)

			// 오프셋은 0부터 순차 증가해야 한다.
			if res.Offset != uint64(offset) {
				t.Fatalf(
					"got offset: %d, want: %d",
					res.Offset,
					offset,
				)
			}
		}
	}
	{
		// ConsumeStream: 서버 스트리밍.
		// 오프셋 0부터 시작해 서버가 레코드를 순서대로 밀어준다.
		stream, err := client.ConsumeStream(
			ctx, &api.ConsumeRequest{Offset: 0},
		)
		require.NoError(t, err)

		for i, record := range records {
			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, res.Record, &api.Record{
				Value:  record.Value,
				Offset: uint64(i),
			})
		}
	}
}

func testUnauthorized(t *testing.T, _ api.LogClient, client api.LogClient, config *Config) {
	ctx := context.Background()
	want := &api.Record{Value: []byte("hello world")}

	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: want})
	if produce != nil {
		t.Fatal("produce not nil")
	}

	gotCode, wantCode := status.Code(err), codes.PermissionDenied
	if gotCode != wantCode {
		t.Fatalf("got code: %d, want: %d", gotCode, wantCode)
	}

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: 0})
	if consume != nil {
		t.Fatal("consume not nil")
	}

	gotCode, wantCode = status.Code(err), codes.PermissionDenied
	if gotCode != wantCode {
		t.Fatalf("got code: %d, want: %d", gotCode, wantCode)
	}
}
