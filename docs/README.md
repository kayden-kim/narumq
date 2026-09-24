# NaruMQ 문서

이 디렉터리는 프로젝트의 기준 문서를 질문의 종류에 따라 분리한다.

[Raft failure-bound 증거 이후 잔여 작업](tasks.md)은 현재 cluster spike로 닫히지 않은 검증·구현 작업을 기준 문서에 연결해 정리한다.

| 디렉터리 | 답하는 질문 | 시작 템플릿 |
| --- | --- | --- |
| [PRD(제품 요구사항 정의서)](requirements/README.md) | 무엇이 필요하며 왜 필요한가? | `PRD.template.md` |
| [스펙(동작 명세)](specs/README.md) | 시스템은 외부에서 어떻게 동작해야 하는가? | `system-spec.template.md` |
| [인터페이스 계약(Contract)](contracts/README.md) | 경계에서 무엇을 어떤 형식으로 주고받는가? | `contract.template.md` |
| [아키텍처(구성요소의 책임과 연결 관계)](architectures/README.md) | 구성요소와 경계는 어떻게 연결되는가? | `architecture-view.template.md` |
| [ADR(기술 결정 기록)](decisions/README.md) | 왜 이 선택을 했는가? | `ADR-0000-template.md` |
| [해설](explainers/README.md) | 핵심 기술과 원리는 어떻게 작동하는가? | `explainer.template.md` |
| [시각 자료](artifacts/README.md) | 구현과 스펙을 시각 자료로 어떻게 이해하는가? | 요청별 HTML artifact |
| [코딩 컨벤션](conventions/README.md) | 소스와 저장소 산출물을 어떤 규칙으로 작성하는가? | `convention.template.md` |
| [실행 절차](how-to/README.md) | 특정 작업을 안전하게 어떻게 수행하는가? | `how-to.template.md` |

## 프로젝트 작업 안내

작업 전 아래 표의 기준 문서를 확인한다. 명령과 규칙의 본문은 해당 문서 한 곳에서 관리하고 여기에는 복제하지 않는다.

| 확인할 내용 | 프로젝트의 기준 문서 |
| --- | --- |
| 변경 대상의 구조와 코드 위치 | [프로젝트 구조](architectures/project-structure.md), [브로커 코어 아키텍처](architectures/broker-core.md) |
| 적용할 코드 작성 규칙 | [Go 코딩 컨벤션](conventions/go-coding-conventions.md) |
| 개발 환경 준비와 실행 방법 | [저장소 README](../README.md) (Go 1.26.5 기준) |
| 변경 범위별 테스트와 전체 완료 검증 | [성능 및 검증](specs/performance-and-verification.md) (`go run ./tools/verify local`, `ci`) |

검증 절차에는 실행 위치, 필요한 도구·환경·버전, 변경 범위별 명령, 전체 완료에 필요한 명령과 성공 조건(`local`, `ci`)을 기록한다.

작업 순서는 루트 [작업 지침](../AGENTS.md#작업-순서와-필수-참조), 문서 채택 여부와 충돌 판단은 [기준 문서 적용과 충돌 처리](AGENTS.md#기준-문서-적용과-충돌-처리)를 따른다.

## 변경에 따른 문서 선택

모든 변경에 모든 유형의 문서를 만들지 않는다. 기존 문서로 다룰 수 있으면 그 문서를 갱신하고, 독립적으로 관리할 질문이 생겼을 때만 필요한 문서를 추가한다. 여러 조건에 해당하면 관련 유형을 함께 확인한다.

| 변경 상황 | 우선 확인하고 필요한 경우 갱신할 문서 |
| --- | --- |
| 사용자에게 보이는 동작을 추가하거나 변경 | 요구사항·동작 명세 |
| 기존 요구사항과 다른 동작을 고치는 버그 수정 | 기존 동작 명세와 해당 시나리오의 시험·증거. 요구사항 자체가 달라지지 않으면 새로 만들지 않는다. |
| API·메시지·파일의 교환 형식이나 호환성 변경 | 경계 계약과 관련 동작 명세 |
| 구성요소의 책임·의존 관계·배포 구조 변경 | 구조 문서 |
| 중요한 대안을 비교하고 설계 방향 선택 | 결정 기록과 결정이 반영될 기준 문서 |
| 이름 짓기·서식·코드 작성 방식 변경 | 작성 규칙 |
| 반복할 개발·검증·운영 작업 정리 | 실행 절차 |
| 이해하기 어려운 원리나 구현 설명 | 해설 |

작은 기능은 관련 요구사항과 동작 명세부터 시작하고, 교환 형식이나 중요한 설계 선택이 생기면 해당 문서를 연결한다. 영향이 없는 문서 유형은 빈 문서를 만들어 채우지 않는다.

## 문서 추가 절차

1. 독자가 답을 얻으려는 주된 질문을 하나 정한다.
2. 위 선택 기준과 표에서 디렉터리를 선택하고, 같은 질문을 다루는 기존 문서가 있는지 먼저 확인한다.
3. 해당 템플릿을 복제하고 자리표시자를 채운다.
4. 요구사항 ID와 관련 기준 문서를 상대 링크로 연결한다.
5. 디렉터리의 `README.md` 문서 목록을 갱신한다.
6. 링크, 채택 상태와 적용 범위, 검증 증거와 미결정 사항을 점검한다. 시험 추적의 기준은 해당 동작 명세의 수용 시험 표로 두고, 다른 문서에서는 같은 결과를 복제하지 않고 참조한다.

## Fixed-voter Raft peer-only 실행

`-raft-peer-config`는 고정 voter `{1,2,3}` 사이에서 TLS 1.3 mTLS peer transport와 Raft core만 실행한다. MQTT·관리 HTTP listener, application proposal API, `READY` 상태 또는 MQTT durable ACK를 제공하지 않는다. 이 mode에서는 `-raft-peer-config` 외 다른 CLI flag와 positional argument를 함께 쓸 수 없다.

각 node config는 strict JSON이다. 아래 예시는 한 host의 loopback 주소를 사용하므로 세 node를 같은 machine에서 실행한다. 다른 host에서는 `listenAddress`와 peer `address`를 reachable 주소로 바꾸고 인증서 SAN과 `dnsName`을 실제 DNS 이름에 맞춘다. `directory`, certificate와 key 경로는 process의 current working directory 기준이다.

```json
{
  "clusterId": "sample-cluster",
  "nodeId": 1,
  "directory": "data/node1",
  "listenAddress": "127.0.0.1:19001",
  "localDnsName": "node1.example",
  "certFile": "certs/node1.crt",
  "keyFile": "certs/node1.key",
  "caFile": "certs/ca.crt",
  "peers": [
    {"id": 2, "address": "127.0.0.1:19002", "dnsName": "node2.example"},
    {"id": 3, "address": "127.0.0.1:19003", "dnsName": "node3.example"}
  ]
}
```

```json
{
  "clusterId": "sample-cluster",
  "nodeId": 2,
  "directory": "data/node2",
  "listenAddress": "127.0.0.1:19002",
  "localDnsName": "node2.example",
  "certFile": "certs/node2.crt",
  "keyFile": "certs/node2.key",
  "caFile": "certs/ca.crt",
  "peers": [
    {"id": 1, "address": "127.0.0.1:19001", "dnsName": "node1.example"},
    {"id": 3, "address": "127.0.0.1:19003", "dnsName": "node3.example"}
  ]
}
```

```json
{
  "clusterId": "sample-cluster",
  "nodeId": 3,
  "directory": "data/node3",
  "listenAddress": "127.0.0.1:19003",
  "localDnsName": "node3.example",
  "certFile": "certs/node3.crt",
  "keyFile": "certs/node3.key",
  "caFile": "certs/ca.crt",
  "peers": [
    {"id": 1, "address": "127.0.0.1:19001", "dnsName": "node1.example"},
    {"id": 2, "address": "127.0.0.1:19002", "dnsName": "node2.example"}
  ]
}
```

세 인증서는 모두 `ca.crt`가 신뢰하는 CA에서 발급해야 한다. 각 인증서에는 대응하는 `localDnsName`을 DNS SAN으로 넣고, server와 client 역할에 모두 사용할 수 있는 EKU(`serverAuth`, `clientAuth`)를 포함한다. Private key는 해당 node만 읽을 수 있도록 보호한다.

세 개의 terminal에서 각 설정을 실행한다.

```sh
go run ./cmd/narumq -raft-peer-config node1.json
go run ./cmd/narumq -raft-peer-config node2.json
go run ./cmd/narumq -raft-peer-config node3.json
```

로그에서 `role: leader`로 바뀐 node를 확인하고, 중지할 때는 각 terminal에서 `Ctrl+C`를 누른다. 종료 시 peer socket, listener와 local WAL을 닫는다. 이는 고정 3-node transport와 Raft election의 실행 예이며 MQTT traffic, replicated application command 또는 production readiness를 검증하는 방법은 아니다.

