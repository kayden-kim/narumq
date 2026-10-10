# 고정 voter Raft peer-only 프로세스를 실행하는 방법

이 절차는 개발 환경에서 고정 voter `{1,2,3}`(Raft 합의에 투표하는 node ID 세 개)의 peer-only process를 실행해 TLS 1.3 mutual TLS(mTLS, 양쪽 node가 서로의 인증서를 확인하는 방식) peer transport와 Raft leader election(Raft 작업을 주도할 node 선출)을 확인한다. 이 mode는 MQTT broker나 전체 production cluster가 아니며 MQTT traffic, application command, readiness 또는 durable ACK를 검증하지 않는다.

| 항목 | 값 |
| --- | --- |
| 대상 독자 | Raft peer transport 개발자와 제한된 peer-only smoke를 수행하는 검증 담당자 |
| 검증 상태 | Draft; 제한된 CLI acceptance test는 통과했고 수동 3-process 절차는 미실행 |
| 검증 환경·버전 | Windows/amd64, Go 1.26.5; `cmd/narumq` targeted acceptance test |
| 최종 검증일 | 2026-09-26 (targeted test; 수동 절차 미실행) |
| 관련 기준 문서 | [Raft peer transport v1 Contract](../contracts/raft-peer-transport-v1.md), [Cluster §11.3 acceptance mapping](../specs/cluster-session-and-delivery.md), [Cluster architecture](../architectures/cluster-architecture.md) |

## 1. 대상과 영향

- 대상: 고정 voter ID `1`, `2`, `3`을 사용하는 Raft peer-only process 세 개.
- 각 process는 TLS peer listener, Raft node와 local WAL(Write-Ahead Log, Raft 상태 복구에 쓰이는 로그 파일)을 실행한다. MQTT/management listener, application proposal API, cluster readiness 및 MQTT durable ACK는 제공하지 않는다.
- 이 절차는 개발·검증용이다. Production cluster 구성, membership 변경 또는 application state replication 승인으로 해석하지 않는다.
- 각 process는 지정한 TCP port를 열고 `directory`에 local Raft data를 기록한다.

## 2. 전제 조건

- [ ] 저장소 root에서 실행할 수 있고 Go 1.26.5 toolchain을 준비했다.
- [ ] 서로 다른 terminal 세 개와 loopback port `19001`~`19003`을 사용할 수 있다. 다른 host에 실행할 때는 peer 주소가 상호 접근 가능해야 한다.
- [ ] 같은 신뢰 CA가 서명한 node별 certificate와 각 certificate에 대응하는 private key가 있다. 각 certificate의 DNS SAN(Subject Alternative Name의 DNS 항목)은 해당 `localDnsName`과 일치하고, TLS server/client 용도로 쓸 수 있는 EKU(Extended Key Usage, 인증서의 허용 용도)에 `serverAuth`, `clientAuth`를 모두 포함한다.
- [ ] 각 config의 `clusterId`는 같고 `nodeId`는 `1`, `2`, `3` 중 하나다. `peers`에는 자기 자신을 제외한 다른 두 voter를 모두 정확히 한 번씩 적는다.
- [ ] node마다 별도인 테스트용 `directory`를 지정했다. 기존 운영 데이터 디렉터리를 사용하지 않는다.
- [ ] Certificate, private key, CA와 config 경로는 각 process의 current working directory 기준으로 접근 가능하다. Private key를 저장소에 넣거나 로그·보고서에 복사하지 않는다.
- [ ] 이 mode에는 `-raft-peer-config <path>` 외의 다른 CLI flag나 positional argument를 전달하지 않는다.

Certificate 발급·배포와 방화벽 변경은 이 절차의 범위가 아니다. TLS identity 또는 peer 연결이 맞지 않으면 인증 검증을 끄지 말고 중단한다.

## 3. 절차

실행 위치는 저장소 root다. 세 process가 같은 host에서 실행되는 예시이므로 peer address에는 loopback을 사용한다. JSON field, 자료형, 필수 조건과 검증 규칙은 [Process configuration v1 contract §1.5](../contracts/process-configuration-v1.md)에서 정한다.

1. **Node 1 설정을 `node1.json`으로 저장한다.**

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

2. **Node 2 설정을 `node2.json`으로 저장한다.**

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

3. **Node 3 설정을 `node3.json`으로 저장한다.**

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

   각 `localDnsName`과 peer `dnsName`은 실제 해당 certificate SAN과 일치해야 한다. 다른 host에서는 `listenAddress`와 peer `address`를 reachable 주소로 바꾸고 인증서 SAN도 실제 DNS 이름에 맞춘다. `directory`, certificate와 key 경로는 실행 directory 기준이다.

4. **각 node를 별도 terminal에서 시작한다.** 세 terminal 모두 저장소 root에서 실행한다.

   ```sh
   go run ./cmd/narumq -raft-peer-config node1.json
   ```

   ```sh
   go run ./cmd/narumq -raft-peer-config node2.json
   ```

   ```sh
   go run ./cmd/narumq -raft-peer-config node3.json
   ```

## 4. 검증

| 관찰 대상 | 성공 조건 | 확인 방법 | 실제 결과 | 실행 증거 위치 |
| --- | --- | --- | --- | --- |
| Process 시작·종료 | 세 process가 각기 다른 node ID와 peer address로 시작하고 취소 시 종료한다. | 세 terminal의 structured log를 관찰하고 각 process에 `Ctrl+C`를 보낸다. | 수동 절차 미실행. | 이 문서의 절차 |
| Raft election | Leader 역할을 나타내는 structured log record가 관찰된다. | 로그에서 `"role":"leader"`를 확인한다. | 수동 절차 미실행. | `TestRaftPeerOnlyStartsThreePeersAndStops`의 제한된 acceptance mapping은 [Cluster §11.3](../specs/cluster-session-and-delivery.md)에 있다. |
| Peer-only 경계 | MQTT `READY`, MQTT listener 또는 application proposal API를 기대하지 않는다. | 실행 mode와 log를 확인한다. 이 항목은 broker readiness 검증이 아니다. | 수동 절차 미실행. | [Cluster architecture](../architectures/cluster-architecture.md) |
| Local WAL | 각 node의 설정한 data directory에 `wal.bin`이 생성된다. | 세 `directory`를 확인한다. | 수동 절차 미실행. | `TestRaftPeerOnlyStartsThreePeersAndStops` |
| CLI peer-only acceptance | Leader 관찰, local WAL, `READY` 미출력과 cancellation cleanup을 확인한다. | 아래의 targeted `go test` 명령 | PASS: `go test` 종료 코드 0. Test fixture를 사용하며 manual JSON config나 terminal 세 개의 실행은 검증하지 않는다. | [raft_peer_config_test.go](../../cmd/narumq/raft_peer_config_test.go) |

제한된 실행 acceptance를 별도로 재현하려면 다음 시험을 실행한다. 이 시험은 manual config file, 실제 terminal 세 개 또는 production storage를 대신 검증하지 않는다.

```sh
go test ./cmd/narumq -run '^(TestRaftPeerOnlyRejectsOtherFlags|TestRaftPeerOnlyStartsThreePeersAndStops)$' -count=1
```

## 5. 중단 조건

- Strict JSON decode, fixed voter ID, peer list 또는 address 검증이 실패하면 진행하지 않는다.
- TLS certificate chain, DNS SAN, EKU 또는 mTLS 인증에 실패하면 진행하지 않는다. TLS 검증을 비활성화하거나 voter ID를 바꿔 우회하지 않는다.
- Port가 사용 중이거나 leader log가 나타나지 않으면 error와 terminal log를 보존하고 원인을 확인한다. 기존 data directory를 지우거나 다른 node의 WAL을 복사하지 않는다.

## 6. 이전 상태로 되돌리는 방법

이 절차는 개발 환경에서 process 세 개와 지정한 local data directory만 사용한다. 각 terminal에서 `Ctrl+C`를 눌러 process를 종료한다. WAL과 로그는 진단에 필요할 수 있으므로 자동 삭제하지 않는다. 정리가 필요하면 경로가 이 smoke 전용 데이터인지 확인한 뒤 저장소의 삭제 정책을 따른다.

## 7. 문제 전달

- 보존할 증거: 비밀을 제거한 각 node ID, listener/peer address, 오류와 election 관련 structured log.
- CA private key, node private key, 인증서 비밀 값 또는 private key 내용을 전달하지 않는다.
- Wire identity·TLS 규칙은 [Raft peer transport v1 Contract](../contracts/raft-peer-transport-v1.md), 실행 제한과 acceptance 범위는 [Cluster §11.3](../specs/cluster-session-and-delivery.md)을 따른다.
