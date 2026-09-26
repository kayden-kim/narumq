# How-to Guides

특정 운영·개발 작업을 안전하게 완료하기 위한 절차를 관리한다.

## 문서 목록

| 가이드 | 상태 | 설명 |
| --- | --- | --- |
| [NaruMQ 소스 빌드 및 실행](installation-guide.md) | Draft | 소스 코드 빌드, 로컬 개발 모드 및 보안 단일 노드 모드 실행과 헬스체크 확인 절차 (Windows 익명 스모크 검증 완료, TLS/클러스터 절차 미실행) |
| [로컬 개발 환경 구성 및 검증](development-guide.md) | Draft | 클린 아키텍처 규칙 준수, 단위·퍼즈 테스트 및 `tools/verify`를 통한 코드 검증 절차 (아키텍처·추적성 검증 완료, 종합 13단계 파이프라인 미실행) |
| [고정 voter Raft peer-only 프로세스 실행](fixed-voter-raft-peer-only.md) | Draft | MQTT listener가 없는 고정 3-voter peer transport/Raft election의 개발용 실행 절차 |
| [동적 클러스터 멤버십 변경](dynamic-cluster-membership.md) | Draft | 3-node cluster에서 node를 추가·교체·제거하는 운영 절차 |
| [Cluster backup 복원](cluster-backup-restore.md) | Draft | 3-voter identity를 유지하며 backup archive를 오프라인 restore하고 readiness/resync를 확인한다. |
