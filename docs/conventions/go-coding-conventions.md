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
```

`local`은 gofmt 검사 뒤 12개 command, `ci`는 같은 검사와 mutation을 더해 17개
command를 실행한다. gofmt 검사는 source를 수정하지 않으며 불일치가 있으면 gate를
중단한다. 독립 command는 동시에 실행하고, 하나가 실패하면 나머지를 취소한다.
Coverage policy 판정은 전체 test command가 만든 profile을 기다린 뒤 실행한다.

검증 게이트는 `go test ./... -parallel=2`로 test parallel limit을 2로 설정한다.
기본 gate의 격리된 cluster process acceptance test는 `t.Parallel()`로 이 상한을
함께 사용한다. 시험별 데이터와 포트는 공유하지 않는다. 빌드 공유와 opt-in 시험의
범위는 [성능 및 검증 §8](../specs/performance-and-verification.md#8-coverage와-test-quality-gate)을 따른다.

```text
공통:
→ go vet ./...
→ traceability
→ architecture
→ go test ./... -parallel=2 -count=1 -covermode=atomic -coverpkg=./cmd/...,./internal/... -coverprofile=.artifacts/verification/coverage.out
→ go test -race ./cmd/... ./internal/... -count=1
→ 5 bounded fuzz campaigns (각 -parallel=1)
→ coverage policy

CI 전용:
→ delivery mutation
→ mqtt mutation
→ nodedrain mutation
→ security mutation
→ session mutation
```

Mutation command는 CI gate에서만 자동 실행한다. Local 개발 gate는 mutation을 제외하고
전체 package suite, race, fuzz와 coverage 검증을 유지한다.

다섯 fuzz campaign은 각각 별도 process와 single worker로 실행한다.
개별 fuzz failure는 다음 exact command로 재현할 수 있다.

```text
go test ./internal/domain/mqtt -run=^$ -fuzz=^FuzzDecoder$ -fuzztime=10000x -parallel=1
go test ./internal/domain/mqtt -run=^$ -fuzz=^FuzzCodecRoundTrip$ -fuzztime=10000x -parallel=1
go test ./internal/domain/routing -run=^$ -fuzz=^FuzzTopicMatching$ -fuzztime=10000x -parallel=1
go test ./internal/domain/session -run=^$ -fuzz=^FuzzLifecycleTransitions$ -fuzztime=10000x -parallel=1
go test ./internal/adapters/tcp -run=^$ -fuzz=^FuzzConnectionInput$ -fuzztime=10000x -parallel=1
```

Package별 mutation failure는 다음 exact command로 재현하며, 결과는 각 JSON path에
보존한다.

```text
go tool gremlins unleash --config=tests/verification/gremlins.yaml --output=.artifacts/verification/mutation-delivery.json internal/domain/delivery
go tool gremlins unleash --config=tests/verification/gremlins.yaml --output=.artifacts/verification/mutation-mqtt.json internal/domain/mqtt
go tool gremlins unleash --config=tests/verification/gremlins.yaml --output=.artifacts/verification/mutation-nodedrain.json internal/domain/nodedrain
go tool gremlins unleash --config=tests/verification/gremlins.yaml --output=.artifacts/verification/mutation-security.json internal/domain/security
go tool gremlins unleash --config=tests/verification/gremlins.yaml --output=.artifacts/verification/mutation-session.json internal/domain/session
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

## 참고 자료

- [Effective Go](https://go.dev/doc/effective_go)
- [Go Code Review Comments](https://go.dev/wiki/CodeReviewComments)
- [Go Doc Comments](https://go.dev/doc/comment)
- [Go Blog: Package names](https://go.dev/blog/package-names)
- [Go Programming Language Specification](https://go.dev/ref/spec)
