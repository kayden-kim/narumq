# NaruMQ 로컬 개발 환경을 구성하고 검증하는 방법

이 절차는 NaruMQ 브로커 코어 및 어댑터 코드를 개발·수정하고, 클린 아키텍처 계층 규칙과 코딩 컨벤션을 준수하며, 단위 테스트, 동시성 레이스 검출, 퍼즈 테스트, 아키텍처 규칙 검사, 규격 추적성 검사, 커버리지 정책 및 뮤테이션 테스트를 포함한 종합 검증 도구 체인(`tools/verify`)을 실행하여 변경 사항을 안전하게 검증하는 방법을 안내한다.

| 항목 | 값 |
| --- | --- |
| 대상 독자 | NaruMQ 브로커 코어와 어댑터를 개발하고 수정·기여하려는 백엔드 소프트웨어 엔지니어 |
| 검증 상태 | Draft; Windows/amd64, Go 1.26.5에서 `GOMAXPROCS=4 go run ./tools/verify local` 통과. 수동 로컬 broker smoke는 미실행 |
| 검증 환경·버전 | Windows/amd64, Go 1.26.5 |
| 최종 검증일 | 2026-09-27 |
| 관련 기준 문서 | [Go 코딩 컨벤션](../conventions/go-coding-conventions.md), [Project structure의 코드 지도](../architectures/project-structure.md), [Broker core architecture](../architectures/broker-core.md), [Performance와 verification 스펙](../specs/performance-and-verification.md) |

## 1. 대상과 영향

- 대상: NaruMQ 전체 소스 코드 저장소(`cmd/`, `internal/`, `tests/`, `tools/`).
- 개발 범위:
  - **도메인 계층 (`internal/domain/`)**: 순수 MQTT 비즈니스 불변식과 상태 머신 (외부 프레임워크 및 I/O import 엄격 금지).
  - **애플리케이션 계층 (`internal/application/`)**: 유스케이스 흐름 조율 및 애플리케이션 포트(인터페이스), 경계 DTO.
  - **어댑터 계층 (`internal/adapters/`)**: TCP/TLS 소켓, 관리 HTTP, Raft 합의, 인메모리/디스크 스토리지 등의 구체 I/O 구현체.
  - **조립 루트 (`cmd/narumq/`)**: CLI 옵션 파싱, 어댑터 생성 및 유스케이스 의존성 주입(Composition Root).
  - **검증 도구 (`tools/verify/`)**: 아키텍처, 커버리지, 규격 추적성, 뮤테이션 등을 검증하는 독립 실행 도구.
- **현재 구현 상태 및 검증 경계:**
  - 현재 소스 트리는 **Phase 1 단일 노드 보안 및 테넌트 격리 슬라이스**가 구현·검증되어 있으며, 분산 클러스터 및 세션 복제는 **Phase 2 진행 중(In-progress)** 상태다.
  - 개발용 standalone 리스너(`cmd/narumq/main.go`)는 인메모리 세션/큐/retained 저장소를 사용하며 `qos0_only`로 동작한다. 영속적 세션 복구나 분산 HA 보장은 standalone 단일 프로세스의 범위가 아니다.
- 개발 환경의 안전성: 개발 및 검증 작업은 로컬 워크스페이스에서 격리되어 수행되므로 운영 시스템에 직접적인 영향을 주지 않는다.

## 2. 전제 조건

- [ ] Go 1.26.5 이상의 Go 공식 툴체인이 설치되어 있다. (`go.mod` 파일의 요구 버전 기준)
- [ ] Git 클라이언트가 설치되어 저장소 최신 브랜치를 체크아웃했다.
- [ ] 코드 작성 전 [Go 코딩 컨벤션](../conventions/go-coding-conventions.md)과 [Broker core architecture](../architectures/broker-core.md)를 읽고 숙지했다.
- [ ] 신규 기능이나 동작 변경 시, 관련 [시스템 스펙](../specs/)과 [경계 계약](../contracts/)을 단일 기준(SSOT)으로 삼는다.
- [ ] 비밀 값(개인키, API 토큰 등)을 테스트 파일이나 커밋에 포함하지 않는다.

## 3. 절차

모든 작업은 저장소 루트 디렉터리(`narumq`)에서 실행한다.

### 1단계: 개발 환경 설정 및 모듈 의존성 검증

저장소를 준비하고 정의된 Go 모듈 의존성을 동기화한다.

```sh
# 의존성 모듈 다운로드
go mod download

# 다운로드된 모듈의 해시 무결성 검증
go mod verify
```

기대 결과: `all modules verified`가 출력되며 오류 없이 완료된다.

### 2단계: 아키텍처 계층 원칙 이해 및 코드 작성

NaruMQ는 **클린 아키텍처(Clean Architecture / Ports and Adapters)** 패턴을 철저히 준수한다. 코드를 수정하거나 추가할 때 다음 의존성 규칙을 반드시 지켜야 한다.

```text
[Composition Root: cmd/narumq]
        │                │
        ▼                ▼
[internal/adapters] ──> [internal/application] ──> [internal/domain]
```

1. **`internal/domain` (도메인 정책)**
   - MQTT 패킷 검증, 세션 상태 전이, 토픽 라우팅 매칭, 테넌트 불변식을 소유한다.
   - **절대 규칙:** 표준 라이브러리(`net`, `os` 제외, 순수 로직 관련 라이브러리 위주) 외에 외부 라이브러리나 상위 패키지(`application`, `adapters`)를 일절 import하지 않는다.
2. **`internal/application` (유스케이스 및 포트)**
   - `ConnectContext`, `Publish`, `Subscribe` 등 비즈니스 흐름을 조율한다.
   - 외부 I/O가 필요한 경우 인터페이스(포트, 예: `SessionRepository`, `Clock`)를 직접 정의하여 소유한다.
   - **절대 규칙:** 구체 어댑터(`internal/adapters`)를 import하지 않는다.
3. **`internal/adapters` (인프라 어댑터)**
   - 네트워크 통신, HTTP 핸들러, etcd/raft 연동, 메모리 캐시 등 I/O를 수행하며 애플리케이션의 포트를 구현한다.
   - 외부 데이터 타입을 도메인/애플리케이션 DTO로 변환하여 전달한다.
4. **`cmd/narumq` (Composition Root)**
   - 설정을 읽고 구체 어댑터를 생성하여 유스케이스 포트에 주입(Dependency Injection)하는 유일한 장소다.

#### 주요 코딩 컨벤션 요약
- **식별자 명명:** 역할에 맞는 접두사와 접미사를 붙인다.
  - 저장소 포트: `*Repository` (예: `SessionRepository`)
  - 구체 구현체: `WALSessionRepository`, `MemorySessionRepository`
  - 데이터 전송 객체: 반드시 `*DTO` 접미사를 사용한다 (예: `ConnectRequestDTO`). `*Dto`는 금지된다.
  - 약어 대소문자: `ClientID`, `JWKSClient`, `HTTPServer`, `TLSConfig`처럼 전체 대문자를 유지한다.
- **문서화 주석:** 외부로 노출(exported)되는 모든 패키지, 타입, 함수, 메서드에는 대상 이름으로 시작하는 완전한 영문 doc comment를 작성한다.

### 3단계: 개별 단위 테스트 및 레이스 조건 검사

코드를 수정한 후 해당 패키지의 단위 테스트를 먼저 실행하여 빠른 피드백을 얻는다.

1. 특정 패키지 단위 테스트 실행:
   ```sh
   go test ./internal/domain/mqtt/... -count=1
   ```
   기대 결과: `ok   narumq/internal/domain/mqtt ...` 출력.

2. 전체 패키지 단위 테스트 실행:
   ```sh
   go test ./... -count=1
   ```
   기대 결과: 전체 패키지 테스트 통과.

3. 동시성 레이스 조건(Race Detector) 검사:
   ```sh
   go test -race ./... -count=1
   ```
   기대 결과: 데이터 레이스 경고 없이 전체 통과.

### 4단계: 바운디드 퍼즈 캠페인(Bounded Fuzz Campaigns) 실행

NaruMQ는 중요한 상태 머신과 파서의 불변식을 보장하기 위해 5개의 핵심 퍼즈 타깃에 대해 고정 작업량(`-fuzztime=10000x`, `-parallel=1`)의 퍼즈 테스트를 필수 관문으로 운영한다.

```sh
# 1. MQTT 디코더 퍼즈
go test ./internal/domain/mqtt -run=^$ -fuzz=^FuzzDecoder$ -fuzztime=10000x -parallel=1

# 2. MQTT 코덱 왕복(Round-trip) 퍼즈
go test ./internal/domain/mqtt -run=^$ -fuzz=^FuzzCodecRoundTrip$ -fuzztime=10000x -parallel=1

# 3. 토픽 매칭 및 와일드카드 퍼즈
go test ./internal/domain/routing -run=^$ -fuzz=^FuzzTopicMatching$ -fuzztime=10000x -parallel=1

# 4. 세션 라이프사이클 상태 전이 퍼즈
go test ./internal/domain/session -run=^$ -fuzz=^FuzzLifecycleTransitions$ -fuzztime=10000x -parallel=1

# 5. TCP 연결 입력과 자원 회수 퍼즈
go test ./internal/adapters/tcp -run=^$ -fuzz=^FuzzConnectionInput$ -fuzztime=10000x -parallel=1
```

기대 결과: 각 명령이 실패 입력(crash corpus) 없이 정상 종료된다.

### 5단계: 로컬 종합 검증 도구 체인(`tools/verify local`) 실행

저장소의 로컬 검증 gate를 실행한다. CI는 이 gate에 PR diff coverage와 다섯 Gremlins mutation 검사를 추가하므로, local PASS만으로 CI 검증 전체를 대체하지 않는다.

```sh
go run ./tools/verify local
```

이 명령은 먼저 `gofmt`를 검사한 뒤, 아래 9개 검증 command를 병렬 실행한다. 모두 통과하면 coverage policy 검사를 실행한다. `local`은 mutation 검사를 실행하지 않는다.

1. **Preflight (3개 command):** `go vet ./...`, `go run ./tools/verify traceability`, `go run ./tools/verify architecture`.
2. **전체 package test와 atomic coverage profile (병렬):** 전체 테스트를 실행해 `.artifacts/verification/coverage.out`을 생성한다.
3. **Race suite (병렬):** `go test -race ./cmd/... ./internal/... -count=1`.
4. **4대 fuzz campaigns (병렬):** decoder, codec round-trip, topic matching, session lifecycle를 각각 고정 실행 횟수로 검사한다.
5. **Coverage policy:** 병렬 검사가 성공한 뒤 line 85% 및 function 90% floor/ratchet를 검사한다.

로컬 실행은 총 10개 비-mutation command(병렬 9개와 coverage policy 1개)이며, CI 실행은 병렬 단계에 Delivery·MQTT·NodeDrain·Security·Session의 다섯 Gremlins mutation command를 더한다.

기대 결과: 마지막에 `local: PASS` 메시지가 출력되고 종료 코드 0으로 완료된다.

### Markdown 링크를 수정할 때

저장소 루트에서 로컬 파일과 제목 앵커를 검사한다.

```sh
go run ./tools/verify markdown-links
```

검사 범위는 루트 `AGENTS.md`, `README.md`, `docs/**/*.md`, `.agents/skills/**/*.md`다. 검사기는 링크 대상 파일·폴더와 Markdown 제목 앵커를 확인한다. 외부 URL과 코드 펜스는 검사하지 않는다. 제목 앵커는 Markdown 제목에서 기본 slug를 만들거나 HTML `id`·`name`, `{#id}` 값을 확인한다. 인라인 링크만 검사하며 reference-style 링크는 범위 밖이다. 코드 펜스는 제외하지만 inline code 안의 링크 문법은 제외하지 않는다. 오류가 있으면 실패한 링크를 출력하고 종료 코드 1을 반환한다.

### 6단계: 로컬 브로커 기동 및 수동 확인

실제 네트워크 상에서 동작을 시험해보고 싶다면 로컬 개발 모드로 브로커를 기동한다.

```sh
go run ./cmd/narumq -tcp-listen 127.0.0.1:1883 -http-listen 127.0.0.1:8080 -allow-insecure-anonymous-mqtt
```

- **구현 경계 안내:** 단일 프로세스 standalone 리스너는 개발 및 단일 노드 검증용으로 인메모리 세션/큐/retained 저장소를 사용하며 `qos0_only`로 동작한다.
- 별도 터미널에서 `curl http://127.0.0.1:8080/health/ready`로 준비 상태를 확인한다.
- 상세한 실행 옵션과 보안 설정 적용은 [설치 가이드](installation-guide.md)를 참조한다.

## 4. 검증

개발자가 수행한 작업의 완료 여부는 다음 기준표로 판정한다. 2026-10-09, Windows/amd64, Go 1.26.5에서 새 80% overall branch 기준으로 Run C를 실행해 `local: PASS`를 확인했다. Branch coverage는 schema 2 report의 `measured`로 기록한다. Overall·core branch ratchet에는 `branchTolerance=0.20`을 적용한다. Core branch는 90%, 변경 코드 line/function/branch diff coverage는 각각 90%다. Diff 기준 변경은 [ADR-0044](../decisions/ADR-0044-diff-coverage-floor-90.md)를 따른다. Run C의 `requirementShortfalls`는 비었다. Run C 당시 P0 gate는 58/81 요구사항 연결과 예외 23개를 보고했으며, 예외가 남아 TEST-10은 미검증이다. CI 필수 status와 TEST-07 diff CI 증거도 남아 있다. 측정값과 경계 시험은 [성능 스펙 §8.4](../specs/performance-and-verification.md#84-전체-branch-floor-수용-증거)에 있다. Run C에서는 mutation과 수동 로컬 broker smoke를 실행하지 않았다. 이후 5개 package mutation과 로컬 CI runner를 실행해 통과했다. 이 실행 증거는 [성능 스펙 §8.7](../specs/performance-and-verification.md#87-mutation-gate-범위-확장-수용-증거)에 있다. 원격 필수 CI status를 확인한 결과는 아니다.

| 관찰 대상 | 성공 조건 | 확인 방법 | 실제 결과 | 실행 증거 위치 |
| --- | --- | --- | --- | --- |
| 단위 테스트 및 레이스 | 전체 테스트 PASS 및 Data Race 0건 | `go run ./tools/verify local` | Run A·B·C 통과; 전체 package test와 `go test -race ./cmd/... ./internal/... -count=1` 단계 완료. 앞선 실패 시도는 성능 스펙에 기록 | 콘솔 출력과 성능 스펙 §8.2 |
| 아키텍처 규칙 | 계층 간 허용되지 않은 import 0건 | `go run ./tools/verify architecture` | 통과; `architecture: PASS` | 콘솔 출력 |
| 규격 추적성 | PRD의 모든 P0를 test 함수 또는 유효한 예외에 연결하고 stale 예외 0건 | `go run ./tools/verify traceability` | 최신 확인(2026-10-09): gate PASS, P0 66/81 연결, 예외 15개. 예외와 필수 CI 증거가 남아 TEST-10은 미검증. Accepted ADR-0042의 결정 기록에 적힌 59/81·22개는 그대로 유지 | [성능 스펙 §8.7](../specs/performance-and-verification.md#87-mutation-gate-범위-확장-수용-증거), [ADR-0042](../decisions/ADR-0042-p0-requirement-traceability-gate.md) |
| Markdown 링크 | 지정 범위의 로컬 링크 대상과 제목 앵커가 모두 유효함 | `go run ./tools/verify markdown-links` | 2026-10-10 현재 작업 트리(ADR-0051·Drain spec·PRD·ADR 목록·TODO 변경 포함), Windows/amd64, Go 1.26.5: `markdown-links: PASS (files 149, local links 1996)`; 오류 0건 | 이 행의 실행 기록 |
| 코드 커버리지 | Line/function 기준, overall branch 80%, core branch 90%, ratchet 허용폭, line/function/branch diff 90% 충족 | `go run ./tools/verify local`; `ci --base <merge-base>`는 별도 CI 실행 | Run C 82.84%, `requirementShortfalls` 비었음; 당시 overall ratchet 80.03%와 core/diff 기준 유지. +0.21%p 경계 거절과 +0.19%p 경계 통과도 확인. 현재 diff 기준 변경과 실행 결과는 성능 스펙 §8.5 참조 | `.artifacts/verification/coverage-summary-floor-c.json`, `coverage-boundary-plus-*.json`, 성능 스펙 §8.4·§8.5 |
| 뮤테이션 테스트 | Delivery·MQTT·NodeDrain·Security·Session 각각 efficacy·mutant coverage 80% 이상 | `go run ./tools/verify ci --base <merge-base>` | 2026-10-09 로컬 순차 측정과 CI runner 실행 모두 5개 package 통과. LIVED·timeout 0개. `local` gate는 mutation을 포함하지 않음. 원격 필수 CI 증거가 없어 TEST-12는 미검증 | [성능 스펙 §8.7](../specs/performance-and-verification.md#87-mutation-gate-범위-확장-수용-증거), `.artifacts/verification/mutation-scope-*.json`, `mutation-*.json` |
| 종합 로컬 검증 | 로컬 검증 gate 전체 완료 및 종료 코드 0 | `go run ./tools/verify local` | 새 80% policy의 Run C에서 `coverage: PASS`, `local: PASS`. 추가 실행(2026-10-10, 현재 작업 트리, Windows/amd64, Go 1.26.5): `GOMAXPROCS=4 go run ./tools/verify local`가 184.56초 뒤 `coverage: PASS`, `local: PASS`를 출력했다. Traceability는 P0 69/81 연결, 예외 12개를 보고했다. CI 필수 status 통과를 뜻하지 않음 | 성능 스펙 §8.4 (Run C 기록); 이 행(2026-10-10 추가 실행) |

## 5. 중단 조건 및 빈발 문제 해결

검증 도구 실행 중 다음 오류가 발생하면 즉시 수정을 진행한다.

### 1. `architecture: FAIL` (아키텍처 규칙 위반)
- **원인:** `internal/domain` 패키지가 외부 라이브러리나 `adapters` 패키지를 import했거나, `application` 계층이 구체 `adapters`를 직접 참조한 경우.
- **해결:** 의존성 방향을 역전시킨다. `application/ports`에 인터페이스를 정의하고, 어댑터가 이를 구현하도록 변경한 후 `cmd/narumq`에서 주입받는다.

### 2. `traceability: FAIL` (규격 추적성 불일치)
- **원인:** MQTT normative statement와 matrix의 연결이 다르거나, Traceability marker의 PRD ID가 없거나, P0 연결과 예외 목록의 상태가 맞지 않는 경우.
- **해결:** MQTT statement 오류는 `tests/conformance/mqtt-compliance-v1.json`과 test comment를 맞춘다. `P0 requirement <ID> has no Traceability test and no exception`이면 관련 test 함수에 `Traceability:`를 추가하거나 이유와 저장소 내 경로를 적어 예외 파일에 추가한다. `P0 requirement <ID> is mapped by ... but still listed`이면 예외 파일에서 해당 ID를 제거한다. `unknown PRD requirement <ID>`이면 marker의 ID를 PRD와 대조해 고친다.

### 3. `coverage: FAIL` (커버리지 기준 미달)
- **원인:** 새로 작성한 비즈니스 로직에 단위 테스트가 충분하지 않아 라인 85% 또는 함수 90% 기준 밑으로 떨어진 경우.
- **해결:** 정상 경로뿐만 아니라 경계 조건(Boundary condition), 예외 처리, 에러 반환 경로를 커버하는 단위 테스트 케이스를 `*_test.go`에 추가한다.

### 4. `gofmt check failed` (포맷팅 불일치)
- **원인:** Go 표준 포맷팅 룰에 맞지 않는 파일이 존재하는 경우.
- **해결:** `gofmt -w .`를 실행하여 포맷팅을 자동으로 정돈한다.

## 6. 이전 상태로 되돌리는 방법

작업 중 발생한 변경 사항을 되돌릴 때는 **저장소 전체를 파괴하는 명령(`git restore .`, `git clean -fd`)을 사용하지 말고**, 변경된 대상을 먼저 확인한 후 안전하게 선택 복구한다.

1. **현재 변경 및 미추적 파일 상태를 먼저 확인한다:**
   ```sh
   git status
   git diff
   ```
2. **자신이 작업한 특정 파일만 선택하여 원래 상태로 복원한다:**
   ```sh
   git restore <수정한_파일_경로>
   ```
3. **테스트 목적으로 직접 생성한 미추적 파일만 지정하여 삭제한다:**
   ```sh
   rm <직접_생성한_임시_파일_경로>
   ```
4. **빌드 및 로컬 검증 중 생성된 산출물은 해당 전용 파일만 정리한다:**
   저장소에 이미 존재하는 `.artifacts/verification/` 디렉터리 전체를 삭제하지 말고, 이번 작업으로 새로 생성된 특정 파일만 개별 정리한다.
   ```sh
   # 빌드 바이너리 삭제
   rm -f bin/narumq bin/narumq.exe

   # 검증 시 새로 생성된 임시 커버리지 파일만 개별 삭제 (기존 베이스라인 증거 보존)
   rm -f .artifacts/verification/coverage.out .artifacts/verification/coverage-summary.json
   ```

## 7. 문제 전달

검증 단계에서 해결하기 어려운 아키텍처 충돌이나 모듈 간 의존성 사이클이 발생할 경우 다음 규칙에 따라 전달한다.

- 전달 대상: 백엔드 테크 리드 및 플랫폼 아키텍트.
- 보존할 증거: `go run ./tools/verify local` 실패 로그 전문, `git diff` 내용, 관련 spec/contract 문서 링크.
- 비밀 값 제외 규칙: 토큰, 패스워드, 개인키 등 보안 자격 증명은 보고서나 이슈 본문에 절대 기입하지 않는다.
