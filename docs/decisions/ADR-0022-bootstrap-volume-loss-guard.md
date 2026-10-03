# ADR-0022: 빈 bootstrap directory는 peer 확인 뒤에만 시작한다

| 항목 | 내용 |
| --- | --- |
| 상태 | Accepted (empty-directory bootstrap guard와 local process acceptance; production EBS 제외) |
| 결정일 | 2026-10-03 |
| 결정자 | NaruMQ 프로젝트 — 사용자 선택 "Peer 확인 후 거절", "함께 시작한 peer 허용" |
| 관련 요구사항 | [PRD](../requirements/PRD.md) CLU-02, CLU-07, CLU-14 |
| 관련 스펙·데이터 계약 | [Cluster §1·§7·§11.20](../specs/cluster-session-and-delivery.md#1120-voter-volume-손실-보호와-재시드-수용-매핑), [Membership v2 §1.5](../contracts/raft-membership-v2.md#empty-directory-bootstrap-probe) |
| 관련 아키텍처 | [Cluster architecture](../architectures/cluster-architecture.md) |
| 관련 ADR | [ADR-0010](ADR-0010-dynamic-cluster-membership.md) |
| 이 결정이 대체하는 이전 결정 기록(ADR) | 없음 |
| 이 결정을 대체한 새 결정 기록(ADR) | 없음 |

## 1. 결정

- 데이터 디렉터리가 없거나 비어 있는 bootstrap voter는 다른 bootstrap peer 모두의 `NMQBOOT2` 응답을 확인한 뒤에만 Raft를 연다.
- 응답 node의 Raft `Term > 1` 또는 `CommitIndex > 3`이면 진행한 cluster로 판정한다. 세 voter의 `RawNode.Bootstrap`은 term 1·index 3에서 끝난다. 하나라도 `PROGRESSED`이면 `cluster bootstrap refused` 오류로 종료한다.
- 초기 시작 확인에서 `EMPTY`로 본 peer ID는 process memory에 함께 시작한 peer(co-bootstrap peer)로 기억한다. 이 peer의 probe에는 진행한 cluster에서도 `READY`로 응답한다. 해당 peer의 Raft frame을 한 번이라도 받으면 허용을 지운다. Forwarding frame은 해제하지 않는다.

ADR-0010의 결론을 바꾸지 않고, 기록을 잃은 voter의 bootstrap 금지 규칙만 추가한다.

## 2. 배경

Volume을 잃은 voter는 같은 ID와 config로 빈 directory에서 시작할 수 있었다. 투표와 ACK 기록이 사라진 node를 새로운 cluster처럼 bootstrap하면 기존 합의 안전성을 지킬 수 없다. 단순 진행 여부 확인은 초기 배포에서 늦게 시작한 node까지 거절하므로 함께 시작한 peer를 구분한다.

## 3. 결정 요인

1. 투표·ACK 기록을 잃은 voter의 재참여를 막는다.
2. 초기 동시 배포의 시작 순서 차이를 허용한다.
3. 새 영속 형식과 운영 flag 비용을 최소화한다.

## 4. 선택 근거

손실 voter가 허용되려면 모든 peer가 그 voter를 함께 시작한 peer로 기억해야 하고, 그 voter에게서 Raft frame을 한 번도 받지 않았어야 한다. 그런데 투표·ACK가 효력을 가지려면 상대가 그 frame을 받아야 한다. 따라서 허용되는 voter는 cluster 결정에 영향을 준 적이 없다. 허용은 process memory에만 두며 restart에서 사라지므로 기존 cluster의 기록 손실을 새로운 최초 배포로 승인하지 않는다.

## 5. 대안

| 대안 | 장점 | 단점 | 제외 이유 |
| --- | --- | --- | --- |
| 보호 없음 + 절차만 | 초기 배포와 구현이 단순하다. | 손실 voter를 코드로 막지 못한다. | 손실 voter 안전성을 충족하지 못한다. |
| Commit index 확인만 | 새 영속 형식 없이 진행 여부를 확인한다. | 초기 동시 시작 경합으로 늦은 node도 거절한다. | 초기 배포 성공 가능성이 낮다. |
| Peer local metadata에 참여 기록 영속화 | Peer restart 뒤에도 참여 여부를 기억한다. | 형식 변경·sync·복구 검증 비용이 생긴다. | Process memory와 frame 수신 경계로 안전성을 지킬 수 있다. |
| Operator 최초 bootstrap flag | 시작 의도를 명시할 수 있다. | 손실 volume에 같은 flag를 재사용하면 보호를 우회한다. | 운영 실수에 대한 안전성이 낮고 설정 비용이 늘어난다. |

## 6. 결과

### 긍정적 결과

- 손실 voter의 같은 ID bootstrap을 거절하며 기존 새 ID stage·join·planned removal로 교체한다.
- 함께 시작했지만 아직 Raft에 영향을 주지 않은 peer는 늦게 시작할 수 있다.

### 부정적 결과와 비용

- 빈 directory bootstrap은 세 peer가 모두 probe를 지원하고 동시에 시작해야 끝난다. Kubernetes StatefulSet은 `podManagementPolicy: Parallel`이 필요하다.
- Probe 중에는 management·MQTT listener가 없다.
- Probe를 모르는 이전 binary peer는 응답하지 않아 대기한다.
- Joiner voter의 빈 volume은 기존 seed admission이 막는다. `cmd/narumq/cluster_runtime.go`의 `admitPeer`는 후보를 뺀 incumbent voter 3개를 요구한다. 이 경우는 process 미검증이다.

### 후속 작업

- Production Linux/EBS volume 손실 검증은 미실행이며 CLU-14 production gate를 유지한다.

## 7. 검증

| 가설·결정 요인 | 검증 방법 | 합격 기준 | 증거 |
| --- | --- | --- | --- |
| 손실 voter 거절·새 ID 재시드 | 실제 process에서 volume 삭제·재시작·교체 | Exit 1, 빈 directory 유지, retained·offline·session 복구 | [Cluster §11.20 실행 증거](../specs/cluster-session-and-delivery.md#1120-voter-volume-손실-보호와-재시드-수용-매핑) |
| 늦은 초기 peer 허용 | Probe 지연 failpoint process | 다른 peer 진행 뒤에도 세 node READY | 같은 수용 매핑 |
| Wire·TLS·frame 경계 | 실제 TLS socket 단위 시험 | Configured peer만 허용, Raft frame 수신 뒤 허용 해제 | 같은 수용 매핑 |

## 8. 재검토 조건

- Bootstrap voter 수나 `RawNode.Bootstrap`의 초기 term/index가 바뀐다.
- Raft frame을 받지 않은 peer가 합의 결과에 영향을 줄 수 있는 새로운 경로가 생긴다.
- 배포 환경이 세 peer의 동시 시작을 지원하지 못한다.
