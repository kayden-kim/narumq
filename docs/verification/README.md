# 검증 기록 목록

검증 기록은 스펙 시나리오의 과거 실행 기록을 보존한다. EKS·kind 실측, 수동 운영 검증, 실패 기록이 해당한다. 테스트 연결과 현재 결과는 [테스트 컨벤션](../conventions/test-conventions.md)의 시나리오 보고서(`go run ./tools/verify scenarios`)로 확인한다. 이 폴더는 프로젝트 전용 폴더이며, 두는 이유는 [AGENTS.md](AGENTS.md)에 있다.

| 문서 | 대상 스펙 | 시나리오 ID |
| --- | --- | --- |
| [Cluster session과 delivery 검증 기록](cluster-session-and-delivery.md) | [Cluster session과 delivery](../specs/cluster-session-and-delivery.md) | `CLU-SCN-*` |
| [Configuration과 runtime option 검증 기록](configuration-and-runtime-options.md) | [Configuration과 runtime option](../specs/configuration-and-runtime-options.md) | `CFG-SCN-*` |
| [Drain과 rolling update 검증 기록](drain-and-rolling-update.md) | [Drain과 rolling update](../specs/drain-and-rolling-update.md) | `DRN-SCN-*` |
| [Management API와 observability 검증 기록](management-api-and-observability.md) | [Management API와 observability](../specs/management-api-and-observability.md) | `MGT-SCN-*` |
| [MQTT protocol과 session 검증 기록](mqtt-protocol-and-session.md) | [MQTT protocol과 session](../specs/mqtt-protocol-and-session.md) | `MQTT-SCN-*`, `MQTT-10-SCN-*` |
| [Performance와 verification 검증 기록](performance-and-verification.md) | [Performance와 verification](../specs/performance-and-verification.md) | `PRF-SCN-*` |
| [Tenancy와 security 검증 기록](tenancy-and-security.md) | [Tenancy와 security](../specs/tenancy-and-security.md) | `SEC-SCN-*` |

작성 규칙은 [AGENTS.md](AGENTS.md)를 따른다.
