# C:\githome\14week_AWS\lg7pilot\README.md - 웹 크롤링 프로젝트

## 📋 프로젝트 개요
이 프로젝트는 네이버뉴스와 GeekNews를 크롤링하여 데이터베이스에 저장하는 파이썬 기반 웹 크롤링 시스템입니다. Playwright와 BeautifulSoup을 활용하여 안정적인 크롤링을 수행하며, MariaDB를 통해 데이터를 체계적으로 관리합니다.

## 🗂️ 파일 구성

### 📰 네이버뉴스 크롤링 모듈
- **`lg7_scrap_naver_news_list.py`** - 네이버뉴스 목록페이지에서 기사 URL을 수집
- **`lg7_scrap_naver_news_item.py`** - 수집된 URL에서 실제 뉴스 내용을 크롤링

### 🔧 GeekNews 크롤링 모듈
- **`geeknews_crawling.py`** - GeekNews 통합 크롤링 시스템 (최신글, 댓글페이지 URL 수집 및 콘텐츠 수집)
- **`lg7_scrap_gn_list.py`** - GeekNews 목록페이지에서 토픽 URL 수집
- **`lg7_scrap_gn_item.py`** - GeekNews 토픽의 상세 내용과 댓글 수집

### 🗄️ 데이터베이스 스키마
- **`lg7_naver.sql`** - 네이버뉴스 크롤링을 위한 데이터베이스 테이블 생성
- **`geeknews_crawling_table.sql`** - GeekNews 크롤링을 위한 데이터베이스 테이블 생성

### 🚇 지하철 데이터
- **`tb_subway.sql`** - 서울지하철 승차 데이터 테이블 생성 및 CSV 데이터 임포트
- **`tb_subway_Quiz.sql`** - 지하철 데이터 분석을 위한 퀴즈/예제 쿼리
- **`import_data.csv`** - 서울지하철 승차 현황 데이터

## ⚙️ 주요 기능

### 🌐 웹 크롤링 기능
- **로봇 배제 표준 준수**: 페이지 로딩 시 3-10초 대기로 서버 부하 방지
- **오류 처리**: 타임아웃 발생 시 브라우저 재시작 및 재시도 로직
- **중복 방지**: DB 조회를 통한 중복 URL 수집 방지
- **단계별 처리**: URL 수집 → 콘텐츠 크롤링 2단계 분리 처리

### 📊 데이터 관리
- **상태 관리**: 크롤링 단계별 상태 추적 (처리전 → 수집중 → 완료/실패)
- **재시도 메커니즘**: 실패한 항목에 대한 재시도 카운트 관리
- **데이터 무결성**: 외래키 제약조건과 유니크 인덱스로 데이터 품질 보장

## 🛠️ 기술 스택
- **Python**: 메인 프로그래밍 언어
- **Playwright**: 동적 웹페이지 렌더링 및 브라우저 자동화
- **BeautifulSoup**: HTML 파싱 및 데이터 추출
- **MariaDB/MySQL**: 데이터 저장 및 관리
- **PyMySQL/MariaDB Connector**: 데이터베이스 연결

## 🗄️ 데이터베이스 구조

### 네이버뉴스
- `news_scrap_ready`: 크롤링 대기 URL 관리
- `news_master`: 수집된 뉴스 기사 저장

### GeekNews
- `gn_scrap_ready`: GeekNews 토픽 URL 대기열
- `gn_master`: GeekNews 토픽 및 댓글 데이터 저장

### 서울지하철
- `tb_subway`: 노선별/월별 승차 승객수 데이터

## 🚀 실행 방법

1. **데이터베이스 설정**
   ```sql
   -- 네이버뉴스용
   source lg7_naver.sql
   
   -- GeekNews용  
   source geeknews_crawling_table.sql
   
   -- 지하철 데이터용
   source tb_subway.sql
   ```

2. **크롤링 실행**
   ```bash
   # GeekNews 통합 크롤링 (URL수집 + 콘텐츠수집)
   python geeknews_crawling.py
   
   # 네이버뉴스 크롤링
   python lg7_scrap_naver_news_list.py    # URL 수집
   python lg7_scrap_naver_news_item.py    # 콘텐츠 수집
   ```

## 📈 데이터 활용
수집된 데이터는 다음과 같은 분석에 활용할 수 있습니다:
- 뉴스 트렌드 분석
- 기술 토픽 인기도 추이
- 댓글 감정 분석
- 서울지하철 이용 패턴 분석

## ⚠️ 주의사항
- 크롤링 시 로봇 배제 표준을 준수합니다
- 서버 부하 방지를 위해 적절한 딜레이를 유지합니다  
- 개인정보나 민감한 정보는 수집하지 않습니다
- 사이트 이용약관을 준수하여 사용하시기 바랍니다

---
**📝 작성일**: 2025년 8월 11일  
**🔧 개발환경**: Python 3.x, MariaDB/MySQL, Windows
