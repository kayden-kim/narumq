# How-to Guides

특정 운영·개발 작업을 안전하게 완료하기 위한 절차를 관리한다.

## 문서 목록

| 가이드 | 상태 | 설명 |
| --- | --- | --- |
| [NaruMQ 소스 빌드 및 실행](installation-guide.md) | Draft | 소스 빌드, 로컬 개발·보안 단일 노드 실행과 health 확인 절차 |
| [로컬 개발 환경 구성 및 검증](development-guide.md) | Draft | 개발 환경 준비, 단위·퍼즈·mutation 시험과 `tools/verify` 실행 절차. 현재 전체 gate 결과는 검증 기록에서 확인한다. |
| [고정 voter Raft peer-only 프로세스 실행](fixed-voter-raft-peer-only.md) | Draft | MQTT listener가 없는 고정 3-voter peer transport/Raft election의 개발용 실행 절차 |
| [동적 클러스터 멤버십 변경](dynamic-cluster-membership.md) | Draft | 3-node cluster에서 node를 추가·교체·제거하는 운영 절차 |
| [Cluster backup 복원](cluster-backup-restore.md) | Draft | 3-voter identity를 유지하며 backup archive를 오프라인 restore하고 readiness/resync를 확인한다. |
| [Cluster feature activation 승인](cluster-feature-activation.md) | Verified (Windows local 절차 범위) | 운영 체크리스트·외부 승인 기록을 만든 뒤 PowerShell로 feature 하나를 활성화하고 commit·rollback 경계를 확인한다. Production 승인이 아니다. |
| [Credential 회전·폐기](credential-rotation-and-revocation.md) | Draft (저장소 내 3-node process acceptance 범위; production 미검증) | Security config 배포, 새 credential 전환, Dynamic cluster 폐기와 결과 확인 |
| [NaruMQ Operations Dashboard 구성](operations-dashboard.md) | Verified (local Docker acceptance 범위; production 미검증) | Prometheus scrape, Grafana provisioning, ReadOnly API key와 Dashboard 확인 절차 |
