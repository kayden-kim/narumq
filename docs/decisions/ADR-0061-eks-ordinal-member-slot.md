# ADR-0061: EKS StatefulSet은 ordinal slot이 가리키는 member ID로 broker를 시작한다

| 항목 | 내용 |
| --- | --- |
| 상태 | `Accepted` |
| 결정일 | 2026-10-16 |
| 결정자 | 사용자 선택("ordinal→node ID 매핑 설계 후 실행", 2026-10-16) |
| 관련 요구사항 | [PRD](../requirements/PRD.md#92-클러스터와-세션) CLU-14, DEP-16, DEP-21 |
| 관련 스펙·계약 | [Cluster §11.20](../specs/cluster-session-and-delivery.md#1120-voter-volume-손실-보호와-재시드-수용-매핑), Draft [Raft membership v2](../contracts/raft-membership-v2.md), [동적 멤버십 절차](../how-to/dynamic-cluster-membership.md) |
| 관련 결정 | [ADR-0010](ADR-0010-dynamic-cluster-membership.md), [ADR-0022](ADR-0022-bootstrap-volume-loss-guard.md), [ADR-0024](ADR-0024-joiner-voter-volume-loss-admission.md), [ADR-0007](ADR-0007-aws-production-deployment-platform.md) |
| 이 ADR이 대체하는 이전 ADR | 없음 |
| 이 ADR을 대체한 새 ADR | 없음 |

## 1. 결정

`deploy/kubernetes/eks-ec2-ha`의 StatefulSet Pod는 자기 ordinal의 slot 파일에서 member ID를 읽는다. Slot은 StatefulSet Pod 자리(ordinal)와 같은 이름·DNS·PVC를 쓰는 broker 자리다.

- ConfigMap `narumq-cluster-configs`에 ordinal마다 key `slot-<ordinal>`을 둔다. 값은 그 slot에서 실행할 member ID 하나다. 양의 10진수이며 앞자리 0이 없다. 초기 값은 `slot-0=1`, `slot-1=2`, `slot-2=3`이다.
- Main container는 `/configs/slot-${NODE_INDEX}`의 ID로 `/configs/node-<ID>.json`을 실행한다. preStop drain도 같은 파일의 ID로 `/api/v1/nodes/<ID>/drain`을 호출한다. 값이 없거나 형식이 틀리면 broker를 시작하지 않고 exit 1로 끝낸다. preStop도 drain을 요청하지 않고 exit 1로 끝낸다.
- Process 설정 파일 이름(`node-<ID>.json`)과 그 안의 `nodeId`는 member ID를 따른다. 두 값이 다르면 배포 계약 시험이 실패한다.
- Peer TLS Secret의 `node-<ordinal>.crt`·`node-<ordinal>.key`는 slot의 인증서다. 이름은 바꾸지 않는다. DNS SAN은 slot의 DNS 이름(`narumq-<ordinal>.narumq-headless.narumq.svc.cluster.local`)이다.

Volume을 잃은 slot은 다음 순서로 새 ID에 넘긴다. 세부 명령은 [동적 멤버십 절차의 EKS slot 교체](../how-to/dynamic-cluster-membership.md#eks-slot의-volume-손실-교체)를 따른다.

1. 남은 두 voter로 quorum이 있는지 확인한다.
2. 새 ID용 key와 인증서를 만든다. DNS SAN은 slot DNS 이름이다.
3. `POST /api/v1/cluster/members`로 새 ID를 stage한다. `address`는 slot DNS 이름과 peer port, `certificateSha256`은 새 인증서의 SHA-256이다.
4. ConfigMap에 새 ID의 joiner 설정 `node-<새 ID>.json`을 추가하고 `slot-<ordinal>`을 새 ID로 바꾼다. Secret의 slot 인증서와 key를 새 것으로 바꾼다.
5. 잃은 PVC와 slot Pod를 삭제한다. StatefulSet이 빈 PVC로 Pod를 다시 만들고, broker는 joiner로 seed admission을 시작한다.
6. 새 ID가 voter로 승격된 것을 확인한 뒤 `DELETE /api/v1/cluster/members/<이전 ID>`로 이전 ID를 제거한다.
7. 이전 ID의 `node-<이전 ID>.json`을 ConfigMap에서 지운다.

새 ID는 이전 ID와 같은 주소와 DNS 이름을 이어받는다. Membership은 member ID 중복만 거절하고 주소 중복은 검사하지 않는다(`internal/application/membership/machine.go`의 `stageJoin`). Peer TLS는 member마다 commit된 인증서 SHA-256과 DNS SAN을 함께 확인한다. 그래서 이전 ID의 인증서로는 새 ID로 인증할 수 없고, 새 ID의 인증서로도 이전 ID로 인증할 수 없다.

이전 ID가 제거되기 전에는 남은 노드가 이전 ID용 dialer와 새 ID용 dialer를 모두 같은 주소로 연결한다. 이전에는 dialer가 상대 인증서를 자신의 hello를 보낸 뒤에야 확인했다. 그래서 이전 ID용 dialer의 hello를 새 ID가 정상 연결로 받아 같은 노드의 기존 연결을 교체하고 닫았다. Hello에 대상 멤버 ID가 없어 수락 측은 이를 구분할 수 없다. 로컬 process 시험이 64회 중 4회 실패한 원인이 이것이다([Cluster §11.20](../specs/cluster-session-and-delivery.md#1120-voter-volume-손실-보호와-재시드-수용-매핑)). 이제 `dialPeer`는 TLS handshake 직후, hello를 보내기 전에 기대한 멤버의 인증서 SHA-256과 DNS SAN을 확인하고 다르면 연결을 닫는다([Raft 멤버십 v2 §1.5](../contracts/raft-membership-v2.md#인증된-raft-hello)). Hello 바이트와 프로토콜 `2.0`은 바꾸지 않았다.

## 2. 배경

이전 manifest는 ordinal N을 member ID N+1에 고정했다. 새 ID 교체([ADR-0022](ADR-0022-bootstrap-volume-loss-guard.md) §1, [ADR-0024](ADR-0024-joiner-voter-volume-loss-admission.md))는 volume을 잃은 ID를 같은 ID로 다시 bootstrap하지 않고 새 ID를 join시킨다. 하지만 같은 Pod 자리에서 다른 ID를 실행할 방법이 없었다. Ordinal을 하나 늘리면 새 ID는 실행할 수 있다. 그러나 StatefulSet은 중간 ordinal을 지울 수 없어 잃은 slot의 Pod가 거절 상태로 계속 남는다. 그래서 EKS에서 CLU-14의 volume loss/reseed를 운영 절차로 실행할 수 없었다.

## 3. 결정 요인

1. 잃은 volume의 slot을 새 member ID로 다시 채우고, Pod 수와 이름을 3개로 유지한다.
2. Main container와 preStop이 같은 출처의 ID를 쓴다. 둘이 다른 ID를 쓰면 다른 node를 drain할 수 있기 때문이다.
3. 잘못된 slot·인증서·설정 조합은 시작하지 않거나 peer 인증에서 거절되어야 한다.
4. 제품 binary, wire 형식과 관리 API는 바꾸지 않는다.
5. 기존 Secret key 이름, restore Job과 overlay의 변경을 최소화한다.

## 4. 대안

| 대안 | 장점 | 제외 이유 |
| --- | --- | --- |
| Ordinal을 4로 늘려 새 ID 실행 | Manifest 변경이 없다. | 잃은 ordinal의 Pod를 지울 수 없어 crash loop가 영구히 남는다. Scale down은 가장 큰 ordinal인 새 member를 지운다. |
| Slot마다 전체 설정 파일(`slot-<ordinal>.json`) | 파일 하나로 slot을 설명한다. | preStop이 JSON에서 ID를 꺼내야 한다. Restore Job의 `node-<ID>.json` 경로도 바뀐다. |
| Secret key를 member ID 기준으로 변경 | 인증서 이름이 ID와 맞는다. | Restore Job·overlay·시험을 모두 바꿔야 한다. Slot 인증서는 DNS SAN이 slot에 묶여 있어 ordinal 이름이 더 정확하다. |
| 관리 API로 local member ID 조회 | 파일 없이 ID를 안다. | 그런 endpoint가 없다(`/api/v1/nodes/{nodeId}/config`는 ID를 미리 알아야 한다). 제품 API 변경이 필요하다. |
| Member마다 별도 StatefulSet | ID와 Pod를 분리한다. | 배포 구조·PDB·topology 규칙을 크게 바꾼다. |

## 5. 결과와 제약

- 초기 배포의 동작은 이전과 같다. Slot 값이 1~3이므로 같은 설정 파일과 인증서를 사용한다.
- 교체 중에는 ConfigMap·Secret과 cluster membership이 잠시 다르다. 4단계 전에 Pod가 다시 시작하면 이전 ID로 시작해 bootstrap 보호에 거절되거나, 같은 volume으로 정상 복구한다. 4단계 뒤 5단계 전에 이전 volume이 남은 Pod가 새 ID로 시작하면 joiner는 빈 directory가 아니어서 seed admission에 성공할 수 없다 [INFERENCE]. 절차는 손실 volume을 확인한 뒤에만 4단계를 시작한다.
- `deploy/kubernetes/eks-ec2-ha-s3`의 restore seed Job은 ID 1~3의 `node-<ID>.json`과 slot 인증서 `node-0..2`를 고정해 사용한다. 재시드로 voter ID가 바뀐 cluster의 S3 restore 절차는 이 결정의 범위 밖이며 검증하지 않았다.
- Membership v2 계약과 동적 멤버십 절차는 아직 Draft다. 이 ADR은 EKS manifest의 ID 선택 방법만 채택한다. Production 운영 승인을 뜻하지 않는다.

## 6. 검증

- `tests/integration.TestEKSDeployment`: slot 기본값, main container와 preStop이 slot 파일로 ID를 읽는 것, 설정 파일 이름과 `nodeId`의 일치를 확인한다.
- `internal/adapters/tcp`의 `TestDynamicDialVerifiesPeerCertificateBeforeSendingHello`와 `TestDynamicSameAddressStaleDialerDoesNotEvictLiveConnection`: 기대한 멤버와 다른 인증서의 listener에는 hello를 보내지 않고, 같은 주소의 정상 연결을 교체하지 않는지 확인한다. 수정 전에는 두 시험이 모두 실패했다.
- `tests/integration`의 같은 주소 새 ID 재시드 process 시험: 잃은 voter의 주소·DNS 이름을 새 ID가 이어받아 voter가 되고 이전 ID가 RETIRED가 되는지 확인한다. 수정 뒤 40회 모두 통과했다. 수정 전에는 64회 중 4회 실패했다.
- 격리 EKS 실행(2026-10-16, 1회): 실제 EBS 손실 뒤 bootstrap 보호 거절, 같은 slot의 새 ID 재시드, durable ACK 메시지 보존을 확인했다. 판정 13개 항목이 모두 통과했다. 실행 중 1 MiB 초과 snapshot으로 leader가 실패하는 결함을 발견해 고쳤다([Cluster §7](../specs/cluster-session-and-delivery.md#7-failure-recovery) 7항). 이 결함은 slot 방식과 관계없다. 직렬화한 snapshot이 1 MiB를 넘는 경우는 [ADR-0062](ADR-0062-raft-snapshot-stream.md)의 `snapshot-stream`을 장애 전에 활성화해야 한다. 활성화 전에는 이 절차로 새 ID를 join할 수 없다. EKS 결과는 [Cluster §11.20 검증 기록](../verification/cluster-session-and-delivery.md#2026-10-16-eks-slot-재시드-실행-결과)에 기록한다. Stream의 local process 검증은 [§11.37](../specs/cluster-session-and-delivery.md#1137-raft-snapshot-stream-수용-매핑)과 구분한다.
