# 실행 안내 문서 작성 규칙

[상위 규칙](../AGENTS.md)을 따르며 다음을 추가한다.

- 목적, 대상과 영향, 전제 조건, 절차, 검증, 중단 조건, rollback과 escalation을 다룬다.
- 한 단계에는 하나의 행동과 판정 가능한 결과만 둔다.
- 명령 전에 정확한 대상과 현재 상태를 확인한다.
- 파괴적 단계 바로 앞에 영향과 복구 가능 여부를 표시한다.
- shell, 실행 위치, placeholder, timeout과 기대 출력을 명시한다.
- 무한 대기나 secret을 command line에 노출하는 예시를 쓰지 않는다.
- 실제 환경에서 확인하지 않은 명령은 검증된 절차처럼 표시하지 않는다.
- 관련 spec, contract, ADR와 구현 version을 연결한다.

## 추가 작성 지침

- **필수 절차 구조:** 목적 → 대상과 영향 → 전제 조건 → 절차(단계별 번호) → 검증 → 중단 조건 → 복구 또는 rollback → Escalation 순서로 구성한다.
- **안전 점검:** 권한, backup, quorum, N-1 용량, maintenance window를 사전 확인한다. 여러 node에 영향을 주는 작업은 동시 허용 중단 수를 명시한다. Secret, token, private key를 command line이나 로그에 절대 노출하지 않는다.
