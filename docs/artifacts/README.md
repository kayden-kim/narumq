# 문서 시각화 산출물

| 파일 | 대상 스펙 ID | 설명 | 생성일 |
| --- | --- | --- | --- |
| [`raft-consensus-wal-snapshot-explanation.html`](raft-consensus-wal-snapshot-explanation.html) | `CLU-01`, `CLU-02`, `CLU-06`, `CLU-07`, `CLU-09`, `CLU-10`, ADR-0001 gate 4, ADR-0002, cluster session과 durable delivery spec | `go.etcd.io/raft/v3 v3.7.0` 기반 Phase 0 gate 4 deterministic 3-node consensus, experimental `NMQWAL01` WAL과 immutable snapshot recovery를 실제 source·test evidence와 연결해 설명한다. Production transport, durable MQTT ACK와 membership 완료를 주장하지 않는다. | 2026-09-19 |
| [`mqtt-protocol-spec-code-explanation.html`](mqtt-protocol-spec-code-explanation.html) | `MQTT-01`, `MQTT-02`, `MQTT-03`, `MQTT-06`, `MQTT-11`, `MQTT-12`, `MQTT-13`, MQTT protocol/session spec, OASIS MQTT 3.1.1·5.0 | `7117790`, `332606f`, `79b4663`, `aebafa9`의 변경을 따라 `internal/domain/mqtt` wire codec, topic routing·bounded delivery queue, `internal/domain/delivery` QoS 상태와 `internal/domain/session` Session Expiry lifecycle을 실제 호출 흐름과 현재 TCP 실행 범위에 연결해 설명한다. | 2026-09-19 |
| [`verification-gate-structure-explanation.html`](verification-gate-structure-explanation.html) | `TEST-12`, `TEST-13`, `docs/specs/performance-and-verification.md` §7–§10 | `tools/verify`의 local·CI 진입점, 13개 command graph, 첫 실패 중단, bounded fuzz, package별 mutation, coverage policy, traceability·architecture 및 현재 Phase 0 범위를 실제 코드·설정·검증 artifact와 연결해 설명한다. | 2026-09-20 |
