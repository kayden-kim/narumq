# Go coding conventions

NaruMQ의 Go 이름은 domain과 type의 역할을 함께 드러낸다. 공통 구현 성격은 `AWS*`, `Default*`, `WAL*` 같은 Prefix로 구분하고, 책임은 `*Repository`, `*Service`, `*Controller`, `*DTO` 같은 Postfix로 통일한다. 다만 Go에는 class가 없으므로 이 규칙은 struct, interface와 named type에 적용한다.

| 항목 | 값 |
| --- | --- |
| 상태 | Active |
| 적용 대상 | 새로 작성하거나 변경하는 human-written Go source와 test |
| 기준 Go | [ADR-0001](../decisions/ADR-0001-core-implementation-language.md)의 Go 1.26 계열 |
| 관련 Architecture | [Project structure](../architectures/project-structure.md), [Broker core architecture](../architectures/broker-core.md) |
| 적용 시작일 | 2026-08-16 |
| Enforcement | Code review 즉시 적용, source tree 생성 뒤 CI 자동화 |

Generated code와 외부 protocol의 wire field는 생성 원본과 contract를 우선한다. Human-written wrapper와 domain type에서 이 convention으로 변환한다.

## 1. 기본 원칙

1. `gofmt` 결과가 formatting 기준이다.
2. Package는 framework가 아니라 MQTT, session, delivery 같은 책임을 드러낸다.
3. 이름은 domain 의미와 역할을 보여준다.
4. Source dependency는 domain을 향한다.
5. Interface는 이를 사용하는 client package가 소유한다.
6. Error, goroutine과 resource의 owner를 숨기지 않는다.
7. 짧음보다 명확성을 우선하되 package 이름과 type 이름을 불필요하게 반복하지 않는다.

Go identifier, package·file 이름, source comment와 error string은 영어를 사용한다. 제품 문서는 [상위 문서 규칙](../AGENTS.md)에 따라 한국어를 사용한다. MQTT packet·property, AWS product와 API field는 공식 표기를 유지한다.

## 2. Package 이름

Package 이름은 소문자 한 단어를 기본으로 한다. Underscore와 mixed caps를 사용하지 않는다.

```text
좋음: mqtt, session, routing, delivery, tenancy, storage, raft
나쁨: mqtt_service, sessionStore, common, utils, helpers, models
```

Package qualifier와 identifier를 함께 읽었을 때 자연스러워야 한다.

```go
session.Owner
mqtt.Packet
routing.Filter
storage.WALSessionRepository
```

Domain package 안에서는 이름을 반복하지 않는다. `session.SessionOwner`보다 `session.Owner`를 사용한다. 반면 `application/ports`나 adapter처럼 여러 domain 역할을 함께 다루는 package에서는 `SessionRepository`처럼 대상과 역할을 모두 표시한다.

다음 포괄 package는 만들지 않는다.

- `common`, `shared`, `utils`, `helpers`
- `models`, `types`, `interfaces`
- 모든 기능을 모은 `services`, `repositories`, `controllers`

## 3. Prefix와 Postfix

### 3.1 조합 규칙

구현 type은 다음 순서로 읽히게 이름을 짓는다.

`[구현 성격 Prefix] + [대상·행위] + [역할 Postfix]`

```text
WAL + Session + Repository  → WALSessionRepository
Default + Drain + Service   → DefaultDrainService
EKS + Node + Discovery      → EKSNodeDiscovery
JWKS + Authentication + Provider → JWKSAuthenticationProvider
```

Prefix나 Postfix를 붙이는 것 자체가 목적은 아니다. 이름의 각 부분이 implementation 선택, domain 대상 또는 책임을 하나씩 설명해야 한다.

### 3.2 허용 Prefix

| Prefix | 사용 조건 | 예 |
| --- | --- | --- |
| `AWS` | AWS 공통 API나 identity에 종속된 구현 | `AWSCredentialProvider` |
| `EKS` | EKS·Kubernetes integration에 특화 | `EKSNodeDiscovery` |
| `NLB` | NLB target·health lifecycle에 특화 | `NLBTargetController` |
| `EBS` | EBS snapshot·volume API에 특화 | `EBSSnapshotRepository` |
| `Default` | 같은 port에 여러 구현이 있고 제품 기본 구현을 명시해야 함 | `DefaultDrainService` |
| `WAL`, `Memory`, `Static`, `File` | 저장 방식이나 data source가 구현 차이를 결정 | `WALSessionRepository`, `MemorySessionRepository`, `FileAuditPublisher` |
| `Raft`, `TCP`, `HTTP`, `JWKS`, `MTLS` | Protocol이나 mechanism이 구현 성격을 결정 | `RaftReplicationService`, `TCPPeerTransport` |
| `Cached`, `Retrying` | 다른 구현을 감싸는 명시적 decorator | `CachedAuthenticationProvider` |
| `Fake`, `Stub`, `Spy` | Test double의 동작 특성을 구분 | `FakeClock`, `StubSessionRepository`, `SpyAuditPublisher` |

`Default`는 구현이 하나뿐인데 미래 확장 가능성만으로 붙이지 않는다. `AWSService`, `DefaultManager`, `CommonRepository`처럼 대상과 책임을 알 수 없는 이름은 금지한다.

### 3.3 역할 Postfix

| Postfix | 의미 | 사용 기준 |
| --- | --- | --- |
| `Repository` | Domain aggregate나 복구 가능한 상태의 persistence abstraction | Session, retained state처럼 domain 의미를 보존하는 read·write 경계 |
| `Store` | WAL record, byte, cache 같은 낮은 수준의 저장 primitive | Domain aggregate 정책을 소유하지 않는 storage port |
| `Service` | 여러 domain operation을 묶는 응집된 application orchestration | 책임을 한 문장으로 설명할 수 있을 때만 사용 |
| `UseCase` | 하나의 명시적 application command·query | `StartDrainUseCase`, `TakeoverSessionUseCase` |
| `Controller` | Inbound request·event를 application input으로 변환 | `MQTTConnectionController`, `NLBTargetController` |
| `Client` | 외부 HTTP·RPC API를 호출하는 protocol client | `JWKSClient`, `KubernetesClient` |
| `Provider` | Credential, clock, configuration 같은 값을 제공 | `AuthenticationProvider`, `CredentialProvider` |
| `Publisher` | Event나 Metric을 외부 sink로 발행 | `AuditPublisher` |
| `Validator` | 입력이나 invariant를 검증하고 결과를 반환 | `PacketValidator` |
| `Encoder`, `Decoder` | 명시한 wire·storage 표현을 변환 | `MQTTPacketDecoder`, `SnapshotEncoder` |
| `DTO` | Application 또는 adapter 경계를 넘는 단순 data transfer object | `ConnectRequestDTO`, `ClientResponseDTO` |
| `Factory` | 생성 과정 자체가 별도 policy일 때만 사용 | 단순 constructor를 감싸는 용도로 사용하지 않음 |

`Service`는 편의성 이름이 아니다. `SessionService`가 ownership, storage, routing, delivery를 모두 처리하면 책임을 분리한다. 단일 동작이면 `*UseCase`, 저장 경계면 `*Repository`, 외부 호출이면 `*Client`처럼 더 정확한 이름을 우선한다.

### 3.4 DTO 이름

직접 작성한 data transfer object에는 반드시 `DTO` Postfix를 붙인다. `Dto` 표기는 사용하지 않는다. 이름은 다음 순서로 읽히게 짓는다.

`[대상·행위] + [Request|Response|Command|Result|Event] + DTO`

| 용도 | 이름 예 |
| --- | --- |
| Inbound request | `ConnectRequestDTO` |
| Outbound response | `ClientResponseDTO` |
| Application command | `StartDrainCommandDTO` |
| Query result | `DrainStatusResultDTO` |
| Integration event | `AuditEventDTO` |

- Application 경계의 request, response, command, query result와 integration event에 적용한다.
- DTO는 data 전달에 필요한 field만 가지며 domain business logic, repository, framework object를 포함하지 않는다.
- Domain entity, value object와 MQTT packet처럼 자체 domain 의미가 있는 type에는 `DTO`를 붙이지 않는다.
- OpenAPI나 protocol 도구가 생성한 wire type은 생성기 이름을 유지한다. Adapter가 이를 project의 `*DTO` 또는 domain value로 변환한다.
- `DataDTO`, `CommonDTO`처럼 대상과 용도를 알 수 없는 이름은 사용하지 않는다.

### 3.5 Port와 구현체 이름

Port는 구현 기술을 이름에 넣지 않는다. Concrete implementation은 Prefix로 차이를 드러낸다.

```go
// package application/ports/sessionstore
type SessionRepository interface {
	Load(ctx context.Context, key session.Key) (session.State, error)
	Save(ctx context.Context, state session.State, epoch session.Epoch) error
}
```

```go
// package adapters/storage
type WALSessionRepository struct {
	// implementation fields
}

func NewWALSessionRepository(cfg WALSessionRepositoryConfig) (*WALSessionRepository, error) {
	// validate and construct
	return &WALSessionRepository{}, nil
}
```

같은 port의 test·local 구현은 다음처럼 대응한다.

```text
SessionRepository
├── WALSessionRepository
├── MemorySessionRepository
└── StubSessionRepository
```

`SessionRepositoryImpl`, `ISessionRepository`, `BaseSessionRepository`는 사용하지 않는다.

## 4. Initialism과 대소문자

Initialism은 한 단어처럼 대소문자를 일관되게 유지한다.

| 사용 | 금지 |
| --- | --- |
| `AWSClient` | `AwsClient` |
| `ClientID`, `clientID` | `ClientId`, `clientId` |
| `MQTTPacket` | `MqttPacket` |
| `HTTPServer` | `HttpServer` |
| `TLSConfig` | `TlsConfig` |
| `JWKSClient` | `JwksClient` |
| `EBSVolumeID` | `EbsVolumeId` |
| `SessionDTO` | `SessionDto` |

Human-written Go identifier에는 `AWS`, `API`, `DTO`, `EBS`, `EKS`, `HTTP`, `ID`, `JWT`, `JWKS`, `MQTT`, `NLB`, `RPC`, `TLS`, `URL`, `WAL` 표기를 사용한다.

Constant도 `MaxPacketSize`, `defaultTimeout`처럼 MixedCaps를 사용한다. Human-written identifier에 `MAX_PACKET_SIZE` 같은 screaming snake case를 사용하지 않는다.

Wire contract가 `clientId`를 요구하면 Go field는 `ClientID`로 쓰고 tag에서 외부 이름을 유지한다.

```go
type ClientResponseDTO struct {
	ClientID string `json:"clientId"`
	NodeID   string `json:"nodeId"`
}
```

## 5. Interface

- Interface는 이를 사용하는 application package에 둔다.
- 구현 package에서 mock을 만들기 위해 interface를 먼저 만들지 않는다.
- 실제 use case가 요구하는 최소 method만 포함한다.
- 한 method interface는 가능하면 `Reader`, `Writer`, `Clock`, `Publisher`처럼 capability로 이름 짓는다.
- 여러 persistence operation을 하나의 domain 경계로 묶을 때는 `*Repository`를 사용할 수 있다.
- `IRepository`, `RepositoryInterface` 같은 표식은 사용하지 않는다.
- Embedding으로 관련 없는 capability를 큰 interface 하나에 합치지 않는다.

구현 package constructor는 concrete type을 반환한다. Composition root가 이를 application port에 주입한다.

```go
repo, err := storage.NewWALSessionRepository(cfg.Storage)
if err != nil {
	return fmt.Errorf("create session repository: %w", err)
}

service := drain.NewDefaultDrainService(repo, replication, clock)
```

## 6. Constructor와 configuration

- 기본 constructor는 `NewType`을 사용한다.
- Prefix가 type의 핵심 차이면 constructor에도 type 전체를 유지한다.
- Constructor는 필수 dependency와 configuration을 검증한다.
- 실패 가능한 초기화는 `(*Type, error)`를 반환한다.
- Constructor에서 goroutine을 암묵적으로 시작하지 않는다. `Run` 또는 `Start`가 lifecycle을 드러내게 한다.
- Package import만으로 side effect가 생기는 `init` 사용은 피한다.
- Option pattern은 선택 항목이 많고 backward-compatible 확장이 실제로 필요할 때만 사용한다.

```go
func NewDefaultDrainService(
	repo SessionRepository,
	replication ReplicationService,
	clock Clock,
) (*DefaultDrainService, error)
```

## 7. Function, method와 variable

- Function과 method는 동사로 시작하고 수행 결과를 드러낸다.
- Getter에는 `Get`을 붙이지 않는다. `Owner()`와 `SetOwner()`를 사용한다.
- Boolean은 `is`, `has`, `can`, `should` 의미가 드러나게 한다.
- `context.Context`는 필요한 function의 첫 parameter `ctx`로 전달한다.
- Context를 struct field에 저장하거나 custom context interface로 감싸지 않는다.
- Receiver 이름은 짧고 type 전체 method에서 일관되게 유지한다.
- Named result와 naked return은 의미를 명확하게 만들 때만 사용한다.

```go
func (s *DefaultDrainService) Start(ctx context.Context, nodeID cluster.NodeID) error

func (o Owner) IsCurrent(epoch Epoch) bool

func (r *WALSessionRepository) Load(ctx context.Context, key session.Key) (session.State, error)
```

`data`, `info`, `item`, `obj`, `manager`처럼 문맥 없이 넓은 이름은 피한다. 짧은 범위에서는 `ctx`, `err`, `cfg`, `req`, `resp`, `msg`, `pkt`를 사용할 수 있다.

## 8. File 이름과 구성

Human-written Go file은 소문자 snake case를 사용한다.

```text
default_drain_service.go
wal_session_repository.go
aws_credential_provider.go
client_response_dto.go
session_repository_test.go
packet_fuzz_test.go
```

- Test file은 `_test.go`, platform suffix는 Go build convention을 따른다.
- Human-written unit·acceptance test의 기본 이름은 대상 production file의 snake case prefix를 따른다. 예를 들어 `framing.go`는 `framing_test.go`, `decoder.go`는 `decoder_test.go`를 사용한다.
- Fuzz와 benchmark는 같은 prefix 뒤에 검증 종류를 붙인다: `decoder_fuzz_test.go`, `encoder_benchmark_test.go`.
- 여러 production component를 함께 검증하는 경우에는 `packet`처럼 domain aggregate prefix를 사용한다: `packet_edge_test.go`, `packet_fuzz_test.go`. `coverage_test.go`, `misc_test.go`처럼 측정 방법이나 모호한 목적을 파일 이름으로 사용하지 않는다.
- 공통 test helper도 가장 가까운 production component prefix와 `_helpers_test.go` suffix를 결합한다: `packet_helpers_test.go`. 여러 component에 걸쳐 재사용되는 경우에만 `test_helpers_test.go`를 사용한다. Helper 파일은 production behavior를 검증하는 파일과 분리한다.
- 파일 prefix는 해당 파일의 주된 production 책임을 설명하면 충분하다. 모든 test function이 하나의 source file에 일대일로 대응할 필요는 없다.
- Generated file은 generator가 정한 이름과 `// Code generated ... DO NOT EDIT.` header를 사용한다.
- 한 file에 하나의 class를 강제하지 않는다. 함께 변경되는 작은 type과 function은 같은 file에 둘 수 있다.
- File이 여러 독립 책임을 가지면 domain 의미를 기준으로 나눈다.
- Package 설명은 한 file의 package comment에 둔다. 복잡한 package는 `doc.go`를 사용할 수 있다.

## 9. Comment와 documentation

- 모든 exported package, type, function, method, constant와 variable에 doc comment를 작성한다.
- Doc comment의 첫 문장은 대상 identifier로 시작하는 완전한 문장으로 쓴다.
- 구현을 그대로 읽어주는 comment보다 invariant, ownership, unit과 failure 의미를 설명한다.
- TODO는 owner 또는 issue와 제거 조건을 연결한다.
- Protocol 표준 조항이 동작을 결정하면 MQTT normative statement ID를 test나 comment에서 추적한다.

```go
// DefaultDrainService coordinates session transfer before a broker stops.
type DefaultDrainService struct {
	// ...
}
```

## 10. Error와 logging

- 정상 실패에는 `error`를 사용한다. `panic`을 error handling으로 사용하지 않는다.
- Error를 무시하지 않는다. 처리하거나 context를 추가해 반환한다.
- Wrapping은 `%w`를 사용해 `errors.Is`와 `errors.As`가 동작하게 한다.
- Error string은 고유명사를 제외하고 소문자로 시작하고 마침표로 끝내지 않는다.
- Sentinel error는 `ErrNotFound`, 구체 error type은 `StaleEpochError`처럼 이름 짓는다.
- 같은 error를 여러 계층에서 반복 logging하지 않는다. 처리 책임이 끝나는 process boundary에서 한 번 기록한다.
- Log와 error에 token, password, private key, payload와 제한된 identity를 넣지 않는다.

```go
if err := repo.Save(ctx, state, epoch); err != nil {
	return fmt.Errorf("persist session state: %w", err)
}
```

## 11. Concurrency와 resource ownership

- Goroutine을 시작한 component가 종료 조건과 wait 책임을 문서화한다.
- Background loop는 `context.Context` 취소 또는 명시적 `Close`로 종료할 수 있어야 한다.
- Channel을 생성한 owner가 close 책임을 가진다.
- Queue와 channel은 bounded capacity를 기본으로 한다.
- 공유 mutable state는 owner goroutine 또는 명시적 synchronization 경계 하나를 선택한다.
- Lock을 잡은 채 network, disk 또는 callback을 호출하지 않는다.
- Goroutine leak, blocked send와 shutdown race를 test한다.

`Default*Service`가 goroutine lifecycle까지 소유하면 `Run`, `Close`와 error propagation을 public contract에 드러낸다.

## 12. Test convention

- Unit test와 fuzz test는 production package 옆에 둔다.
- Test 이름은 대상, 동작과 조건을 드러낸다.
- 여러 입력 조합은 table-driven test와 subtest를 사용한다.
- Test double Prefix는 동작을 구분한다.

| Prefix | 의미 |
| --- | --- |
| `Fake` | 단순하지만 동작 가능한 in-memory 구현 |
| `Stub` | 준비한 값을 반환하는 수동 응답 구현 |
| `Spy` | 호출과 인자를 기록하는 구현 |
| `Mock` | 기대 호출 검증이 중심인 generated 또는 명시적 mock |

```go
func TestDefaultDrainService_Start_RejectsUnknownBarrier(t *testing.T) {
	t.Parallel()

	// arrange, act, assert
}
```
- Coverage·conformance command가 `-run` 정규식을 사용하면 파일 이름이 아니라 test function 이름을 기준으로 선택한다. 파일을 rename하거나 분리해도 대상·동작을 드러내는 기존 test 이름과 requirement metadata를 유지하고, gate 정규식은 package 책임에 맞게 관리한다.

- Failure message에는 입력, 기대값과 실제값을 포함한다.
- 시간, random, network, storage와 consensus는 port와 fake로 제어한다.
- Concurrency test가 순서나 sleep에 우연히 의존하지 않게 한다.
- Requirement ID와 MQTT statement가 있는 acceptance·conformance test는 이름이나 metadata로 추적한다.

## 13. Formatting과 CI

Repository verification의 표준 진입점은 다음 command다.

```text
go run ./tools/verify local
go run ./tools/verify ci --base <merge-base>
go run ./tools/verify mutation
```

`local`과 `ci`는 gofmt 검사 뒤 각각 12개 command를 실행한다. `local`은 package
coverage 병렬도를 8로 두고, CI는 유효 `GOMAXPROCS`를 상한으로 쓴다. 독립 command는
병렬 실행하며 하나가 실패하면 같은 단계의 나머지를 취소한다. Coverage policy는 전체
test command가 만든 profile을 기다린 뒤 실행한다. Mutation은 이 두 gate에 포함되지 않는다.

각 package 안에서 `t.Parallel()`의 상한은 local에서 8, CI에서 유효 `GOMAXPROCS`다.
Local과 CI의 다른 실행 조건은 [성능 및 검증 §8](../specs/performance-and-verification.md#8-coverage와-test-quality-gate)을 따른다.

```text
공통:
→ go vet ./...
→ traceability
→ scenarios 연결 검사
→ architecture
→ atomic coverage test와 go-test JSON 저장 (local `-parallel=8`; CI `-parallel=GOMAXPROCS`; `-count=1`)
→ go test -race ./cmd/... ./internal/... -count=1
→ 5 bounded fuzz campaigns (각 -parallel=1)
→ scenarios 실행 검사 (`--go-test-json`·`--require-executed`)
→ coverage policy

CI 전용:
→ coverage test `-parallel=GOMAXPROCS`; diff coverage에만 `--base` 전달

수동 mutation 전용:
→ `go run ./tools/verify mutation`
→ Delivery, MQTT, NodeDrain, Security, Session package를 Gremlins `workers=1`,
  `test-cpu=1`로 CPU 예산에 맞춘 batch 실행
```

Mutation command는 local·CI에서 실행하지 않는다. 필요할 때 별도 명령으로 실행하고
각 package의 efficacy와 mutant coverage 80% 기준을 확인한다.

다섯 fuzz campaign은 각각 별도 process와 single worker로 실행한다.
개별 fuzz failure는 다음 exact command로 재현할 수 있다.

```text
go test ./internal/domain/mqtt -run=^$ -fuzz=^FuzzDecoder$ -fuzztime=10000x -parallel=1
go test ./internal/domain/mqtt -run=^$ -fuzz=^FuzzCodecRoundTrip$ -fuzztime=10000x -parallel=1
go test ./internal/domain/routing -run=^$ -fuzz=^FuzzTopicMatching$ -fuzztime=10000x -parallel=1
go test ./internal/domain/session -run=^$ -fuzz=^FuzzLifecycleTransitions$ -fuzztime=10000x -parallel=1
go test ./internal/adapters/tcp -run=^$ -fuzz=^FuzzConnectionInput$ -fuzztime=10000x -parallel=1
```


`go run ./tools/verify mutation`은 다섯 package를 실행하고 각 결과를 JSON path에
보존한다. 실패 원인을 좁히려면 해당 package 하나에 아래처럼 Gremlins를 직접 실행한다.

```text
go tool gremlins unleash --config=tests/verification/gremlins.yaml --output=.artifacts/verification/mutation-mqtt.json internal/domain/mqtt
```

Fuzz가 생성한 입력의 발견 순서는 deterministic하지 않다. 고정된 `10000x` quota와
보존된 `testdata/fuzz/<Target>/` failure corpus가 반복 실행 계약이며, 입력 순서를
재현 가능한 것으로 가정하지 않는다. Coverage 원본과 요약은
`.artifacts/verification/coverage.out` 및
`.artifacts/verification/coverage-summary.json`에 기록한다.

- `gofmt` 검사 출력이 있으면 CI를 실패시킨다.
- Import 정리는 version을 고정한 `goimports`를 사용할 수 있다.
- Import는 standard library, third-party, project package group 순으로 나눈다. Dot import는 사용하지 않고 blank import는 `main` 또는 필요한 test registration으로 제한한다.
- 고정된 line length 제한은 두지 않는다. 의미 단위로 나누고 긴 이름·parameter list가 책임 과다의 신호인지 먼저 확인한다.
- Fuzz, coverage, mutation, compatibility와 benchmark gate는 [Performance와 verification 스펙](../specs/performance-and-verification.md)을 따른다.
- Architecture test는 domain·application·adapter의 금지 import와 cycle을 검사한다.
- Linter는 version과 configuration을 repository에 고정한 뒤 필수 gate로 승격한다.
- Generated source는 별도 검증하되 human-written lint 위반을 숨기는 제외 pattern을 넓게 적용하지 않는다.

## 14. Review checklist

1. Package 이름이 domain 책임을 드러내는가?
2. Type 이름이 Prefix·대상·Postfix로 구현 성격과 역할을 보여주는가?
3. Initialism이 `AWS`, `MQTT`, `ID`처럼 일관되는가?
4. 직접 작성한 경계 객체가 `*DTO`를 사용하고 domain·framework type을 노출하지 않는가?
5. `Default`, `Service`, `Manager`가 불필요하게 사용되지 않았는가?
6. Interface가 consumer 측에 있고 실제 필요 method만 포함하는가?
7. Constructor가 concrete type과 validation error를 반환하는가?
8. Context, goroutine, channel, buffer와 close owner가 명확한가?
9. Error가 wrapping되고 secret 없이 한 경계에서 기록되는가?
10. Unit test가 정상·오류·경계 조건을 검증하는가?
11. `gofmt`, test, race와 architecture gate를 통과하는가?

## 15. 이름 짓기

상위 [컨벤션 작성 규칙](AGENTS.md)의 이름 짓기 기본 규칙을 Go에 적용한다. §2~§4가 이름의 형태를 정한다. 이 절은 package qualification과 현재 코드의 suffix 목록을 기록한다. 조사 기준은 `internal/domain`, `internal/application`, `internal/adapters`, `cmd/narumq`, `tools`의 test와 생성 코드(`*.pb.go`)를 제외한 type 선언 813개다.

### 15.1 규칙 우선순위와 적용

규칙은 컴파일러·Go 명세, `gofmt`, 공식 Go 코딩 관례(Effective Go, Code Review Comments, package names), 이 컨벤션, 개별 package 규칙 순서로 적용한다. 따라서 상위 규칙의 도메인 prefix는 Go 관례에 맞춰 다음처럼 읽는다.

- Go에서는 package qualifier가 이름의 맥락을 제공한다. `session.Owner`를 `session.SessionOwner`처럼 쓰지 않는다. 이는 공식 Go 관례의 stutter 방지 규칙이며, 업무 용어의 prefix를 정하는 규칙과 구분한다.
- 한 package가 여러 도메인 대상을 다루는 `application/ports/*`, `adapters/*`에서는 §2대로 `AuditPublisher`처럼 대상을 type 이름에 둔다.
- 용어집(`GLOSSARY.md`)이 아직 없다. 아래 표는 현재 package 위치와 책임을 찾는 코드 목록이다. 용어집이나 업무 prefix 목록을 대신하지 않는다. 새 업무 용어의 코드 영어 이름을 확정할 때는 [용어집 유지보수](../AGENTS.md#용어집-유지보수)에 따라 용어집에 먼저 기록한다.

| 도메인 package | 현재 책임 |
| --- | --- |
| `internal/domain/connrebalance` | 연결 재균형과 로컬 shedding 규칙 |
| `internal/domain/delivery` | MQTT QoS 1·2 inflight 정책과 상태 전이 |
| `internal/domain/mqtt` | 네트워크와 독립된 MQTT packet codec |
| `internal/domain/nodedrain` | 노드 drain 생애주기와 safe-to-stop 판정 |
| `internal/domain/release` | broker release 버전과 feature token |
| `internal/domain/rollout` | StatefulSet 순차 rollout 정책 |
| `internal/domain/routing` | topic·filter 검증과 구독 매칭 |
| `internal/domain/security` | 인증된 principal과 tenant ACL policy |
| `internal/domain/session` | MQTT Session Expiry 생애주기 |
| `internal/domain/tenancy` | tenant 범위 identity·topic·filter 값 |

Application package(`audit`, `connect`, `management`, `membership`, `observability`, `publish`, `rollout`, `subscribe`, `takeover`, `tenantlimit`)에도 같은 package qualification 규칙을 적용한다.

### 15.2 Suffix 목록

Type 유형은 아래 suffix 중 하나로 나타낸다. 구현 성격 Prefix(`WAL`, `Default` 등)는 §3.2를 따른다. 층별 수는 조사 기준 type 선언 수이며 unexported type을 포함한다. 예는 `package 경로.type` 형식이고 모두 소스에서 확인했다.

역할 suffix:

| Suffix | 층별 사용 | 역할 | 예 | 추가 이유 |
| --- | --- | --- | --- | --- |
| `DTO` | application 101, ports 70, adapters 30 | 경계를 넘는 단순 자료 | `application/audit.QueryCommandDTO`, `application/ports/audit.AuditEventDTO` | §3.4에서 정의됨 |
| `Service` | application 12, ports 1 | 응집된 application 흐름 | `application/membership.BlocklistService` | §3.3에서 정의됨 |
| `UseCase` | application 3, ports 1, adapters 8 | 명시적 application 명령·조회. adapter가 필요한 use case만 골라 선언한 interface에도 쓴다 | `application/connect.UseCase`, `application/ports/management.NodeConfigurationUseCase`, `adapters/http/management.BlocklistUseCase` | §3.3에서 정의됨 |
| `Repository` | application 1, ports 1, adapters 2 | 복구 가능한 상태의 저장 경계 | `application/ports/retained.Repository`, `adapters/memory/sessionstore.MemorySessionRepository` | §3.3에서 정의됨 |
| `Controller` | cmd 7 | 외부 이벤트를 application 입력으로 변환하는 composition root 구성요소 | `cmd/narumq.clusterDrainController` | §3.3에서 정의됨 |
| `Handler` | adapters 17 | HTTP 요청 등 adapter의 처리 진입점 | `adapters/http/management.HealthHandler` | 기존 코드 관례. Go 표준 `http.Handler`가 공식 관례(우선순위 3)이고, `Controller`는 cmd에서 쓰므로 adapter 진입점은 `Handler`로 구분 |
| `Config` | application 4, adapters 14, cmd 16 | 구성 요소를 만드는 설정 값 묶음 | `application/audit.ServiceConfig` | 기존 코드 관례. 값을 제공하는 `Provider`로 표현할 수 없음 |
| `Provider` | ports 4, adapters 3, cmd 1 | 값이나 조회 결과를 제공 | `application/ports/authentication.AuthenticationProvider`, `application/ports/management.StateProvider` | §3.3에서 정의됨 |
| `Publisher` | application 2, ports 1, adapters 1 | event를 외부 sink로 발행 | `application/ports/audit.AuditPublisher`, `adapters/auditlog.FileAuditPublisher` | §3.3에서 정의됨 |
| `Reader` | application 1, ports 2, adapters 5 | 읽기 전용 capability | `application/ports/audit.AuditRecordReader` | 기존 코드 관례. §5의 capability 이름 |
| `Recorder` | application 1, ports 5 | 결과·event를 기록 | `application/ports/audit.DataPlaneDenialRecorder` | 기존 코드 관례. 외부 sink로 발행하는 `Publisher`와 달리 프로세스 내 기록 |
| `Reporter` | application 1, cmd 3 | 거절·상태를 로그나 metric으로 보고 | `application/tenantlimit.Reporter` | 기존 코드 관례. 값을 돌려주지 않는 알림이라 `Provider`·`Publisher`와 다름 |
| `Checker` | application 1, ports 1, cmd 1 | 단일 조건을 판정해 `bool`과 `error`를 반환 | `application/connect.CredentialRevocationChecker` | 기존 코드 관례. 입력 검증인 `Validator`와 달리 외부 상태를 조회 |
| `Counter` | application 1, ports 1, adapters 1 | 개수 조회 | `application/connect.TenantSessionCounter` | 기존 코드 관례 |
| `Scheduler` | application 1, adapters 2 | 지연 callback 실행 | `application/connect.Scheduler` | 기존 코드 관례 |
| `Source` | ports 4, cmd 1 | snapshot이나 committed 값의 조회 원천 | `application/ports/management.ClusterSessionSource` | 기존 코드 관례 |
| `Snapshotter` | ports 3 | 현재 상태의 snapshot 제공 | `application/ports/management.SessionSnapshotter` | 기존 코드 관례 |
| `Committer` | application 3, ports 2 | commit된 결과(`ApplyResultDTO`)를 반환 | `application/ports/deliverystate.PublishCommitter` | 기존 코드 관례. `Repository`는 상태 저장 경계이고 이 interface는 commit 결과 계약을 가짐 |
| `Coordinator` | application 4 | 여러 단계의 ownership·offline 처리를 조정 | `application/connect.OwnershipCoordinator` | 기존 코드 관례 |
| `Visitor` | ports 1, adapters 2 | 순회하며 metric family·sample을 수신 | `application/ports/metrics.MetricVisitor` | 기존 코드 관례. Visitor 패턴의 공식 이름 |
| `Limiter` | application 1, adapters 2 | 속도·횟수 제한 | `adapters/http/management.requestRateLimiter` | 기존 코드 관례 |
| `Error` | domain 1, application 1, ports 1, adapters 2, tools 1 | 구체 error type | `domain/mqtt.UnsupportedProtocolVersionError` | §10에서 정의됨 |

값 유형 suffix(주로 domain·application 값 type):

| Suffix | 층별 사용 | 역할 | 예 | 추가 이유 |
| --- | --- | --- | --- | --- |
| `Key` | domain 2, application 3, adapters 9, cmd 4 | 값 식별자 | `domain/tenancy.SessionKey` | 기존 코드 관례. `ID`는 단일 scalar 식별자에 사용 |
| `Action` | domain 2, application 12, ports 1, adapters 1, cmd 1 | 수행할 동작의 종류 | `application/membership.CredentialRevocationAction` | 기존 코드 관례 |
| `State` | domain 1, application 2, ports 2, adapters 7, cmd 1 | 시점의 상태 값 | `domain/connrebalance.ShedState` | 기존 코드 관례 |
| `Phase` | domain 4, application 3, ports 1 | 생애주기의 단계 | `domain/nodedrain.Phase`, `application/membership.MemberPhase` | 기존 코드 관례 |
| `Stage` | domain 1, adapters 3, cmd 1 | 한 처리의 로컬 실행 진행 | `domain/connrebalance.ShedStage` | 기존 코드 관례. 생애주기 단계인 `Phase`와 구분 |
| `Kind` | domain 1, ports 1, adapters 2, cmd 2 | 하위 종류 구분 | `domain/security.BlockKind` | 기존 코드 관례 |
| `Status` | ports 2, adapters 2, tools 1 | 관측·조회에 노출하는 상태 | `application/ports/management.HealthStatus` | 기존 코드 관례 |
| `Outcome` | application 3, adapters 1, tools 1 | 적용·처리 결과 분류 | `application/membership.ApplyOutcome` | 기존 코드 관례 |
| `Result` | domain 1, ports 2, adapters 1, cmd 1, tools 1 | 한 연산의 반환 묶음 | `domain/session.DisconnectResult` | 기존 코드 관례 |
| `Filter` | domain 1, ports 6 | 조회 조건·topic filter | `domain/tenancy.TopicFilter`, `application/ports/management.ClientFilter` | 기존 코드 관례 |
| `Entry` | domain 1, adapters 3, cmd 1 | 목록의 한 항목 | `domain/security.BlockEntry` | 기존 코드 관례 |
| `Record` | domain 1, adapters 3 | 저장·열거되는 한 건의 기록 | `domain/routing.SubscriptionRecord` | 기존 코드 관례 |

Domain entity와 값 type(`session.Lifecycle`, `session.Ownership`, `mqtt.Will` 등)은 suffix를 붙이지 않고 package 이름과 명사로 의미를 나타낸다. MQTT packet 이름(`PubAck`, `ConnAck`)은 표준 용어를 따른다.

목록에 없는 단발 이름은 2번 이하로 쓰인다. `Client`(`adapters/rollout.ManagementClient`), `Store`(`application/rollout.StatusStore`), `Encoder`와 `Decoder`(`domain/mqtt.Encoder`, `domain/mqtt.Decoder`)는 §3.3에 정의되어 있다. `Verifier`(`adapters/authentication.JWTVerifier`), `Transport`(`adapters/tcp.TCPPeerTransport`), `Machine`(`application/membership.StateMachine`)은 아직 2번뿐이라 목록에 올리지 않았다. 셋째 사용처가 생길 때 이유와 함께 추가한다. `Option`(`application/connect.Option`)은 package마다 하나인 functional option type 이름이다.

새 suffix는 위 suffix와 §3.3으로 표현할 수 없을 때만 추가하고, 표에 층·역할·추가 이유를 함께 적는다.

### 15.3 이름 길이와 file 이름

이름은 5단어 이상으로 짓지 않기를 권장한다. 단어는 대문자 경계로 세고 `DTO`, `MQTT`, `QoS` 같은 약어와 `WebSocket`은 한 단어로 센다.

```text
좋음: membership.BlocklistService (Blocklist + Service, qualifier 제외 2단어)
나쁨: ports/management.TenantQuotaProfileConfigurationDTO (Tenant + Quota + Profile + Configuration + DTO, 5단어)
개선: ports/management.QuotaProfileDTO (Quota + Profile + DTO, 3단어)
```

File 이름은 §8의 Go 규칙을 따른다. 소문자 snake case를 쓰고 test는 `_test.go`로 끝낸다. 대문자와 `-`는 쓰지 않으며 생성 file은 생성기 이름을 유지한다. 현재 5단어 이상인 file 이름은 없다.

### 15.4 현재 목록에서 벗어난 이름

코드는 이름을 바꾸지 않고 기록만 한다. 조사 기준은 15.2와 같다.

| 구분 | 수 | 예 | 벗어난 규칙 |
| --- | --- | --- | --- |
| 5단어 이름 | 17 (6단어 이상 없음) | `application/membership.UpdateClusterPolicyRequestDTO`, `application/ports/management.TenantQuotaLimitsConfigurationDTO`, `cmd/narumq.clusterTenantQuotaProfileConfig` | 5단어 권고(§15.3) |
| 도메인 prefix 반복 | 6 | `application/membership.MembershipAction`, `application/membership.MembershipRecordDTO`, `application/membership.MembershipOperationPhase`, `application/membership.MembershipOperationDTO`, `application/rollout.RolloutDTO`, `application/connect.ConnectRequestDTO` | 공식 관례의 stutter 금지(§2, §15.1). `application/ports/*` package의 대상 반복은 §2가 허용 |
| 목록 밖 `Port` suffix | 6 | `application/membership.BlocklistPort`, `application/membership.NodeDrainPort` | 목록에 없음. `application/ports` package와 의미가 겹치고, §5는 interface를 소비 package가 소유하며 capability 또는 `UseCase`·`Repository` 같은 역할로 이름 짓도록 함 |
| 목록 밖 `Wire` suffix | 4 | `adapters/auditlog.auditEventWire`, `adapters/auditlog.producerWire` | 목록에 없음. 직접 작성한 전송 자료는 §3.4에 따라 `DTO` |
| HTTP JSON 자료의 `Request`·`Response` | 53 | `adapters/http/management.clusterPolicyRequest`, `adapters/http/management.auditRecordResponse`, `adapters/http/management.ErrorResponse` | 직접 작성한 전송 자료에는 `DTO` suffix가 필요(§3.4). 현재는 `DTO` 앞의 중간 단어로만 허용 |
| 모호한 `Data` | 1 | `tools/verify.coverageData` | 목록에 없음. §7이 피하라는 문맥 없는 넓은 이름 |

새로 작성하거나 변경하는 type은 위 이름을 따라 하지 않는다. 기존 이름은 해당 type을 바꾸는 변경에서 함께 정리한다.

## 참고 자료

- [Effective Go](https://go.dev/doc/effective_go)
- [Go Code Review Comments](https://go.dev/wiki/CodeReviewComments)
- [Go Doc Comments](https://go.dev/doc/comment)
- [Go Blog: Package names](https://go.dev/blog/package-names)
- [Go Programming Language Specification](https://go.dev/ref/spec)
