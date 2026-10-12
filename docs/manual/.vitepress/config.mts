import { defineConfig } from 'vitepress'

export default defineConfig({
  title: 'NaruMQ 사용자 매뉴얼',
  description: '분산 MQTT 브로커 NaruMQ 클라이언트 개발자 및 운영자 가이드',
  lang: 'ko-KR',
  base: process.env.BASE_PATH || '/narumq/',

  themeConfig: {
    siteTitle: 'NaruMQ Manual',
    nav: [
      { text: '시작하기', link: '/getting-started/overview' },
      { text: '클라이언트 가이드', link: '/client-guide/connection-and-auth' },
      { text: '운영자 가이드', link: '/operator-guide/installation' },
      { text: '클러스터 프리뷰', link: '/cluster-preview/cluster-preview' },
      { text: '문제 해결', link: '/troubleshooting/troubleshooting' }
    ],

    sidebar: [
      {
        text: '시작하기 (Getting Started)',
        items: [
          { text: 'NaruMQ 개요 및 지원 범위', link: '/getting-started/overview' },
          { text: '5분 빠른 시작', link: '/getting-started/quick-start' }
        ]
      },
      {
        text: '클라이언트 개발자 가이드',
        items: [
          { text: '연결 및 인증', link: '/client-guide/connection-and-auth' },
          { text: '메시지 발행 및 구독', link: '/client-guide/pub-sub' },
          { text: '언어별 클라이언트 예제', link: '/client-guide/client-examples' }
        ]
      },
      {
        text: '브로커 운영자 가이드',
        items: [
          { text: '시스템 요구사항 및 빌드', link: '/operator-guide/installation' },
          { text: '프로세스 및 CLI 설정', link: '/operator-guide/configuration' },
          { text: 'TLS 및 보안 정책 설정', link: '/operator-guide/security' },
          { text: '관리 REST API', link: '/operator-guide/management-api' },
          { text: '메트릭 및 헬스 프로브', link: '/operator-guide/observability' }
        ]
      },
      {
        text: '클러스터 런타임 (Preview)',
        items: [
          { text: 'Raft 클러스터 평가 가이드', link: '/cluster-preview/cluster-preview' }
        ]
      },
      {
        text: '문제 해결 및 지원',
        items: [
          { text: '자주 묻는 질문 및 오류 해결', link: '/troubleshooting/troubleshooting' }
        ]
      }
    ],

    search: {
      provider: 'local',
      options: {
        locales: {
          root: {
            translations: {
              button: {
                buttonText: '검색',
                buttonAriaLabel: '검색'
              },
              modal: {
                noResultsText: '결과를 찾을 수 없습니다',
                resetButtonTitle: '검색어 지우기',
                footer: {
                  selectText: '선택',
                  navigateText: '이동',
                  closeText: '닫기'
                }
              }
            }
          }
        }
      }
    },

    docFooter: {
      prev: '이전 페이지',
      next: '다음 페이지'
    },

    outline: {
      level: [2, 3],
      label: '목차'
    }
  }
})
