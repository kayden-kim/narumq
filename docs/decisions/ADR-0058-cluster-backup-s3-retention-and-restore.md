# ADR-0058: 클러스터 백업의 S3 보존과 오프라인 복원

| 항목 | 내용 |
| --- | --- |
| 상태 | `Accepted` |
| 결정일 | 2026-10-14 |
| 결정자 | 사용자 위임에 따른 선택 (2026-10-14 요청); 백업 정책 목표는 사용자 승인 |
| 관련 요구사항 | [TODO §1.1 SPEC-TBD-09](../TODO.md#11-production-storage와-disaster-recovery) |
| 관련 스펙·계약 | [Cluster session and delivery §10](../specs/cluster-session-and-delivery.md#10-snapshot-backup과-restore) |
| 관련 아키텍처 문서 | [Cluster architecture](../architectures/cluster-architecture.md) |
| 이 ADR이 대체하는 이전 ADR | 없음 |
| 이 ADR을 대체한 새 ADR | 없음 |

## 1. 결정

사용자가 승인한 백업 정책은 매일 백업, 30일 보존, 재해 복구 RPO 24시간, 전체 클러스터 RTO 1시간이다. 남은 SPEC-TBD-09 설계 결정은 사용자에게 위임받았다. 이에 따라 새 백업의 보존을 관장하는 기준 저장소로 S3를 선택하고, 기존 PVC는 임시 보관용으로만 사용한다. 버킷은 클러스터와 같은 리전의 비공개 버킷으로 하며 퍼블릭 접근을 차단하고 TLS가 아닌 접근을 거부한다. 암호화에는 SSE-S3를 사용한다. 백업 객체는 30일 후 만료시키고 완료되지 않은 multipart upload는 1일 후 중단한다.

예약 백업 작업은 새 아카이브와 크기를 제한한 JSON 업로드 요청 기록을 영속 PVC에 기록한다. 요청 기록에는 UTC 시각, commit index, 파일 전체의 SHA-256, 안전한 staged filename을 담는다. 업로더는 영속 요청 기록이 함께 있는 아카이브만 처리한다. 기록된 파일 전체 checksum을 `PutObject`에 전달하고 S3가 반환한 checksum과 대조한다. 두 값이 일치한 뒤에만 아카이브와 요청 기록을 삭제한다. 실패하면 다음 Job이 재시도할 수 있도록 두 파일을 보존한다. 새 object key에는 UTC 시각, commit index, 파일 전체 SHA-256을 넣는다. 재시도는 같은 content-addressed key에만 덮어쓸 수 있으며 `If-None-Match`는 사용하지 않는다.

digest가 고정된 공개 AWS CLI 이미지 `amazon/aws-cli:2.37.9@sha256:92de75724b6a746951f0e8b915d86bbccd7cb55aff96cd0cb4f7017272160780`에는 `s3_uploader.py`와 `restore-from-s3.py`를 실행할 Python 환경이 포함되어 있다. 별도 이미지를 만들거나 실행 중 패키지를 설치할 필요가 없다. 기본 `deploy/kubernetes/eks-ec2-ha` Kustomize 패키지에서는 백업을 로컬 예약 작업으로 수행하며 백업 helper `ConfigMap`에는 `backup.sh`만 넣는다. 기본 패키지는 선택형 S3 `ServiceAccount`나 Terraform 설정 `ConfigMap`을 참조하지 않는다. 명시적으로 선택하는 `deploy/kubernetes/eks-ec2-ha-s3` overlay는 S3 보조 스크립트를 추가하고 uploader sidecar와 설정 참조를 연결한다. 공유 운영 Terraform root `deploy/terraform`는 `backup_s3_enabled`(기본값 `false`)로 S3 리소스 생성을 제어한다. 별도 `deploy/terraform-acceptance-s3` root에는 이 변수가 없고 S3 module을 직접 호출하므로, 운영자가 해당 root에 명시적으로 `apply`할 때만 fixture 리소스를 만든다. 선택한 Terraform root에서 리소스를 설치한 뒤에만 Kubernetes S3 overlay를 적용한다. 목록 조회와 voter seed에 필요할 때 사용하는 Job 매니페스트는 같은 디렉터리에 있지만 Kustomize 렌더링 및 적용 대상에는 포함하지 않는다. EKS identity webhook의 `skip-containers` annotation은 web-identity 자격 증명을 전송 container에만 제공하고, `automountServiceAccountToken: false`는 기본 Kubernetes token을 비활성화한다. S3 복원에는 `deploy/kubernetes/eks-ec2-ha-s3-restore-mode`를 사용한다. 이 overlay는 복원 전에 StatefulSet을 정지하고 S3 backup CronJob을 `suspend: true`로 설정하며 새 빈 target PVC를 준비한다. 별도 `eks-ec2-ha-restore-mode`는 로컬 복원 전용 overlay로 유지한다.

업로드 요청 기록이 없는 기존 PVC 아카이브는 이행하지 않고 형식도 파싱하지 않는다. 이 아카이브는 기존 `mtime +30` 정리 정책에 따라 로컬에서 관리한다. S3 보존 및 관련 수용 검증 증거는 새로 생성해 S3에 업로드한 아카이브부터 시작한다. 스모크 시험에는 폐기 가능한 fixture에서 새로 만든 아카이브를 사용하며 공유 `narumq-eks` 클러스터는 사용하지 않는다.

업로드 역할의 IRSA 신뢰 조건은 `system:serviceaccount:narumq:narumq-backup`과 정확히 일치해야 한다. 이 역할은 설정된 cluster prefix 아래에서 `s3:PutObject`만 수행할 수 있으며 객체 목록 조회·읽기·삭제 권한은 없다. 별도 복원 역할의 IRSA 신뢰 조건은 `system:serviceaccount:narumq:narumq-backup-restore`와 정확히 일치해야 한다. 복원 역할은 해당 prefix만 `s3:ListBucket`으로 조회하고 그 prefix 아래 객체만 `s3:GetObject`로 읽을 수 있다. 복원용 web-identity token은 전송 container에만 제공한다. 복원은 오프라인으로 수행한다. 복원 도구는 선택한 정확한 object key를 내려받아 S3 checksum과 파일 전체 SHA-256을 검증한다. 그 뒤 기존 `-cluster-restore` 아카이브 검사와 cluster identity 검사를 거쳐 비어 있는 voter PVC 하나를 seed한다. 복원 대상은 새로 마련한 격리 환경이어야 하며 원본 클러스터나 이미 데이터가 있는 fixture PVC에는 적용하지 않는다.

## 2. 남은 수용 기준

- 격리 fixture에서 비공개 S3 버킷의 퍼블릭 접근 차단, TLS 전용 정책, SSE-S3, 30일 만료, 1일 후 미완료 multipart upload 중단, 동일 리전 배치를 확인한다.
- 업로드 역할이 새 아카이브를 올릴 수 있지만 객체를 나열·읽기·삭제할 수 없는지 확인한다. 복원 역할이 설정된 prefix만 나열하고 해당 prefix 안의 객체만 읽을 수 있는지도 확인한다.
- EKS identity webhook이 `skip-containers`를 따르는지 확인한다. IRSA web-identity token이 업로드·다운로드 container에만 제공되고 broker 및 복원/대상 준비 container에는 제공되지 않는지 검증한다.
- Seed Job은 새로 만든 격리 복구 대상에서만 실행한다. 기존 원본/fixture PVC는 빈 복원 대상이 아니므로 재사용하지 않는다.
- 새 fixture 아카이브를 업로드하고 응답 checksum 확인, 정확한 object key 조회·다운로드, SHA-256 검증, 오프라인 복원까지 수행한다. 업로드 실패를 주입한 뒤 아카이브와 요청 기록이 다음 Job을 위해 모두 보존되는지 확인한다. checksum이 일치하는 재시도에 성공하면 두 파일이 모두 제거되는지도 확인한다.
- Fixture 리소스는 별도 acceptance Terraform root/state로만 관리하고 `narumq-eks`는 변경하지 않는다. 증거 수집 뒤 fixture에서 새로 만든 모든 리소스를 제거한다.
- 한 번의 스모크 시험으로 실제 30일 경과 보존이나 매일 실행에서 지속되는 `RPO ≤ 24h`를 입증할 수 없다. 단일 실행으로 확인할 수 있는 것은 설정된 lifecycle 정책과 백업 한 회의 성공뿐이다.
- 아카이브 seed 시간만으로 전체 클러스터 1시간 `RTO`를 입증할 수 없다. 새 fixture에서 복원 시작부터 세 voter의 기동, readiness, quorum 달성 및 서비스 복구 동작까지 측정한다.
