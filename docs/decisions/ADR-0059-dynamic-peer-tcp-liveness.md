# ADR-0059: Dynamic peer 연결은 TCP 생존 확인으로 응답 없는 상대를 끊는다

| 항목 | 내용 |
| --- | --- |
| 상태 | `Accepted` |
| 결정일 | 2026-10-15 |
| 결정자 | 사용자 요청("peer 재연결 지연을 로컬에서 해결", 2026-10-15)에 따른 선택 |
| 관련 요구사항 | [PRD](../requirements/PRD.md#92-클러스터와-세션) CLU-02, CLU-07, [PRD §4.2](../requirements/PRD.md#42-초기-성공-지표)의 비정상 노드 장애 RTO |
| 관련 스펙·계약 | [Cluster §11.34](../specs/cluster-session-and-delivery.md#1134-dynamic-peer-tcp-생존-확인-수용-매핑), Draft [Raft membership v2](../contracts/raft-membership-v2.md#raft-메시지-프레임과-크기-한도) |
| 관련 결정 | [ADR-0008](ADR-0008-raft-peer-transport.md)(연결 방향·5초 I/O timeout), [ADR-0010](ADR-0010-dynamic-cluster-membership.md), [ADR-0014](ADR-0014-cross-node-live-qos0-forwarding.md) |
| 이 ADR이 대체하는 이전 ADR | 없음 |
| 이 ADR을 대체한 새 ADR | 없음 |

## 1. 결정

`DynamicTCPPeerTransport`(v2)는 수립된 Raft·forwarding stream의 생존 확인 시간을 짧게 제한한다.

- Raft·forwarding stream을 dial한 socket과, listener가 수락한 뒤 stream magic(`NMQRAFT2`, `NMQFWRD2`)을 확인한 socket에 TCP keepalive를 설정한다. 유휴 5초 뒤 5초 간격으로 2회 probe한다.
- Linux에서는 같은 socket에 `TCP_USER_TIMEOUT`을 10초로 설정한다. 보낸 data가 10초 동안 ACK를 받지 못하면 kernel이 연결을 닫는다. 이 10초는 ACK 대기 data가 생긴 시점부터 센다. 상대 host 장애 시점부터 10초를 보장하지 않는다.
- Linux가 아닌 OS에서는 keepalive만 적용한다. 지원 배포 profile(`EKS_EC2_HA`)은 Linux다.
- 연결이 닫히면 기존 dial loop가 100ms 뒤 commit된 주소로 다시 dial한다. 주소의 DNS 이름은 dial마다 다시 해석된다.

Wire 형식, cluster protocol `2.0`, 길이 prefix의 유휴 대기 규칙, 5초 frame I/O timeout, 대기열 한도와 연결 방향 규칙은 바꾸지 않는다. 가입(`NMQJOIN2`), bootstrap probe, 관측 RPC처럼 한 번 주고받고 닫는 socket은 기존 설정과 deadline을 유지한다. 고정 voter v1 transport(`TCPPeerTransport`)는 이 결정의 범위가 아니다.

## 2. 배경

v2 연결 방향은 member ID가 낮은 node가 dial하고 높은 node가 accept하는 것이다. 높은 ID의 node가 새 Pod와 새 IP로 다시 시작해도 먼저 연결을 걸 수 없다. 낮은 ID 쪽이 이전 연결을 끝내야 새 주소로 dial한다.

이전 구현의 stream socket은 Go 기본 TCP keepalive(유휴 15초, 간격 15초)만 사용했다. 감지 시간을 제한하는 별도 설정은 없었다. Linux keepalive는 ACK 대기 data가 없을 때만 probe한다. Leader는 상대가 사라져도 heartbeat를 계속 보내므로 이 경우 keepalive가 동작하지 않고 kernel 재전송 한도까지 연결이 남는다고 본다 [INFERENCE]. 길이 prefix를 기다리는 읽기는 제한 시간이 없다. 관찰한 log는 응용 송신 대기열(32개)이 찬 뒤 `peer write failed`(5초 write deadline)와 `connected=false`가 이어진 순서다. Kernel 송신 buffer가 찼다는 설명과 `write failed` 뒤 5초 간격의 원인은 확인하지 않았다 [INFERENCE].

2026-10-15 EKS 재시험에서 leader는 종료된 Pod의 이전 연결을 약 190초 유지했다. 그동안 대체 node 3은 leader의 메시지를 받지 못했다([재시험 기록](../verification/drain-and-rolling-update.md#기존-pod-해제의-eks-재시험-2026-10-15)). 로컬 Linux Docker에서 같은 DNS 이름의 node를 새 IP로 바꾸고 이전 IP를 응답 없이 버리자 같은 순서가 재현됐다. 결과는 [Cluster §11.34](../specs/cluster-session-and-delivery.md#1134-dynamic-peer-tcp-생존-확인-수용-매핑)에 둔다.

## 3. 결정 요인

1. 응답 없는 상대를 송신량과 무관한 시간 안에 감지한다.
2. Cluster protocol `2.0`의 wire bytes와 mixed-release 호환성을 유지한다.
3. 정상 상태에서 유휴인 follower 사이 연결을 불필요하게 끊지 않는다.
4. Raft와 forwarding lane에 같은 규칙을 적용한다.

## 4. 값의 근거

Raft heartbeat는 100ms, election timeout은 1초다. Leader는 상대가 사라져도 heartbeat를 계속 보내므로 ACK 없는 data가 바로 생긴다. 10초는 cross-AZ 지연과 짧은 network 흔들림을 견디면서 [PRD §4.2](../requirements/PRD.md#42-초기-성공-지표)의 장애 RTO p99 30초 안에 들어간다. 유휴 연결은 keepalive가 5초 뒤 probe를 시작하므로 약 15초 안에 끊긴다. 잘못 끊긴 연결은 100ms 재시도로 다시 수립되고 Raft가 재전송한다.

## 5. 대안

| 대안 | 장점 | 제외 이유 |
| --- | --- | --- |
| 응용 heartbeat frame 추가 | OS와 무관하게 응답 없는 peer를 감지한다. | Raft frame bytes가 바뀌어 cluster protocol `2.1`이 필요하다. |
| 길이 prefix 대기에 읽기 유휴 timeout 추가 | 구현이 단순하다. | 유휴인 follower 사이 연결이 반복해서 끊긴다. 유휴 대기에 제한이 없다는 v2 계약을 바꾼다. |
| Leader가 응답 없는 follower 연결을 `ResetPeer`로 끊음 | OS와 무관하다. | Raft lane의 leader 쪽만 다룬다. Forwarding stream과 follower 쪽 연결은 남고, Raft 상태와 transport 수명이 결합된다. |
| 송신 대기열 포화 때 연결 재설정 | 기존 v1 규칙과 같다. | 대기열이 찰 때까지 감지하지 못하는 문제가 그대로다. v2 계약은 포화 때 연결 보존을 요구한다. |

## 6. 결과와 제한

- 응답 없는 상대의 감지가 응용 대기열이나 write deadline 실패를 기다리지 않는다.
- 변경 전후 시험은 keepalive 값과 `TCP_USER_TIMEOUT`을 함께 바꿨다. 두 설정 각각의 기여는 측정하지 않았다.
- Linux가 아닌 개발 환경에서는 송신 중인 연결의 감지가 kernel 재전송 한도에 남는다.
- Network가 10초 넘게 멈추면 연결이 끊긴다. 이후 재연결과 Raft 재전송이 처리한다.
- 재연결은 새 Pod 주소가 DNS에 게시되어야 성공한다. EKS manifest의 headless Service는 `publishNotReadyAddresses: true`로 준비 전 Pod 주소도 게시한다([service-headless.yaml](../../deploy/kubernetes/eks-ec2-ha/service-headless.yaml)). 이 결정은 DNS 게시 정책을 바꾸지 않는다.
- 2026-10-16 EKS에서 follower EC2를 OS 종료 없이 두 번 잃었다. 두 번 모두 leader가 확인받지 못한 송신 데이터가 생긴 뒤 약 10.3초 안에 이전 IP 연결을 닫았다. 대체 node는 새 IP에서 시작한 직후 연결됐다([Cluster §11.34 EKS 재측정](../verification/cluster-session-and-delivery.md#eks-무응답-종료-재측정-2026-10-16)). 정확한 RTO와 다른 장애 유형은 측정하지 않았다.

## 7. 검증

[Cluster §11.34](../specs/cluster-session-and-delivery.md#1134-dynamic-peer-tcp-생존-확인-수용-매핑)에 다음을 연결한다.

1. 같은 DNS 이름의 peer를 새 IP로 교체하고 이전 IP를 응답 없이 버리는 Linux Docker chaos 시나리오가 변경 전에 실패하고 변경 뒤 통과한다.
2. 기존 peer transport 시험과 local 검증이 통과한다.
3. EKS에서 OS 종료 없이 follower EC2를 잃었을 때 leader가 이전 IP 연결을 약 10초 안에 닫고 새 IP에 연결한다.

## 8. 재검토 조건

- 실제 EKS 측정에서 감지 시간이 PRD 장애 RTO와 맞지 않는다.
- 비 Linux OS를 지원 배포 profile에 추가한다.
- Cluster protocol을 올리는 다른 변경이 있어 응용 heartbeat를 함께 도입할 수 있다.
