# 인터페이스 계약(Contract) 작성 규칙

[상위 규칙](../AGENTS.md)을 따르며 다음을 추가한다.

## 시작 템플릿

[계약 템플릿](../templates/contract.template.md)을 복제해 실제 경계 계약을 작성한다.

- 스키마가 있으면 machine-readable 파일을 기준으로 삼고, Markdown에는 스키마 전체를 복제하지 않고 목적과 호환성 규칙만 적는다.
- 호환성을 깨는 변경(Breaking change)은 새 major 버전, 마이그레이션 및 롤백 계획을 함께 수립한다.
- 만드는 쪽(Producer)과 사용하는 쪽(Consumer) 양쪽의 계약 테스트(Contract test)를 연결한다.
- 비밀 값이나 실제 인증 정보를 예시에 넣지 않는다.

