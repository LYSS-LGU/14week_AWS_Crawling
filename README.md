# 📚 데이터 엔지니어링 학습 프로젝트

> 기상청 API와 소상공인 데이터를 활용한 **ETL 파이프라인** 구축 실습

## 🎯 학습 목표

이 프로젝트를 통해 **데이터 엔지니어링의 핵심 개념**을 실습으로 학습합니다:

- **ETL (Extract, Transform, Load)** 파이프라인 구축
- **데이터웨어하우스** 아키텍처 이해
- **ODS → FACT → MART** 데이터 플로우
- **SQL 데이터 변환** 및 **집계 분석**
- **Python 데이터 처리** 자동화

---

## 🏗️ 데이터 아키텍처

```mermaid
graph LR
    A[원본 데이터<br/>CSV/API] --> B[ODS<br/>Operational Data Store]
    B --> C[FACT<br/>정제된 데이터]
    C --> D[MART<br/>분석용 데이터]
    D --> E[사용자/대시보드]

    style A fill:#ff9999
    style B fill:#99ccff
    style C fill:#99ff99
    style D fill:#ffcc99
    style E fill:#ff99ff
```

---

## 📊 ETL 프로세스 상세

### **1. Extract (추출) 🗂️**

**원본 데이터를 시스템으로 가져오기**

#### 🌤️ 기상청 데이터 (API)

```python
# 기상청 API 호출로 실시간 데이터 수집
kma_aws_collector.py → tb_weather_aws1 테이블
```

#### 🏪 소상공인 데이터 (CSV)

```python
# 대용량 CSV 파일 일괄 처리
sbiz_load_smb_data.py → tb_smb_ods 테이블 (270만건)
```

### **2. Transform (변환) 🔧**

**비즈니스 요구사항에 맞게 데이터 정제 및 변환**

#### 데이터 정제

- **NULL 처리**: 빈 값을 기본값으로 치환
- **타입 변환**: 문자열 → 숫자, 날짜 형변환
- **데이터 검증**: 유효하지 않은 레코드 필터링
- **중복 제거**: 기본키 기준 중복 데이터 처리

#### 스키마 매핑

```sql
-- ODS → FACT 테이블 매핑 예시
col1 (상가업소번호) → smb_id CHAR(20)
col2 (상호명) → smb_name TEXT
col13 (시도명) → addr1 VARCHAR(255)
col38 (경도) → lon DECIMAL(16,12)
```

### **3. Load (적재) 💾**

**분석 가능한 형태로 최종 저장**

#### MART 테이블 생성

```sql
-- 비즈니스 로직 적용: 행정동별 최다 업종 분석
WITH category_count AS (
    -- 행정동별 + 업종별 집계
    SELECT addr3, cate3_nm, COUNT(*) as store_count
    FROM fact_smb_master
    GROUP BY addr3, cate3_nm
),
ranked_category AS (
    -- 각 행정동별 업종 랭킹
    ROW_NUMBER() OVER (PARTITION BY addr3 ORDER BY store_count DESC)
)
-- 각 동별 1위 업종만 추출
SELECT * FROM ranked_category WHERE rn = 1
```

---

## 🔄 데이터 플로우 실습

### **Phase 1: 기상청 실시간 데이터** ⛅

```
기상청 API → Python 스크립트 → MariaDB → 대시보드
```

**핵심 학습 포인트:**

- REST API 호출 및 JSON 파싱
- 실시간 데이터 수집 스케줄링
- 시계열 데이터 저장 패턴

### **Phase 2: 소상공인 대용량 데이터** 🏪

```
CSV (270만건) → ODS → FACT → MART → 분석 결과
```

**핵심 학습 포인트:**

- 대용량 데이터 배치 처리
- 데이터웨어하우스 레이어 구조
- 집계 및 랭킹 분석

---

## 🎓 핵심 데이터 개념

### **ODS (Operational Data Store)**

> **"원본 데이터 그대로 보관하는 저장소"**

- **목적**: 원본 데이터 무결성 보장
- **특징**: 최소한의 변환, col1~col39 같은 원시 컬럼명
- **예시**: `tb_smb_ods` - CSV 원본 그대로 TEXT 타입

### **FACT (팩트 테이블)**

> **"분석을 위해 정제된 핵심 데이터"**

- **목적**: 분석 가능한 형태로 데이터 정제
- **특징**: 의미있는 컬럼명, 적절한 데이터 타입
- **예시**: `fact_smb_master` - smb_id, addr1, cate3_nm 등

### **MART (데이터 마트)**

> **"특정 목적을 위해 집계된 서비스용 데이터"**

- **목적**: 최종 사용자/서비스가 직접 사용
- **특징**: 비즈니스 로직 적용, 집계/랭킹 결과
- **예시**: `mart_addr3_top_category` - 행정동별 최다 업종

---

## 💡 데이터 엔지니어링 베스트 프랙티스

### **1. 데이터 품질 관리**

```sql
-- 데이터 검증 쿼리 예시
SELECT
    '전체 건수' as metric,
    COUNT(*) as value
FROM fact_smb_master
UNION ALL
SELECT
    '유효 좌표 비율',
    CONCAT(ROUND(COUNT(CASE WHEN lon != 0 THEN 1 END) * 100.0 / COUNT(*), 2), '%')
FROM fact_smb_master;
```

### **2. 성능 최적화**

```sql
-- 적절한 인덱스 설계
CREATE INDEX idx_addr_category ON fact_smb_master(addr1, addr2, addr3, cate3_cd);
CREATE INDEX idx_coordinates ON fact_smb_master(lon, lat);
```

### **3. 모니터링 및 로깅**

```python
# Python에서 처리 상황 로깅
print(f"✅ 처리 완료: {inserted_count:,}건")
print(f"⏭️ 중복 스킵: {skipped_count:,}건")
print(f"❌ 오류 발생: {error_count}건")
```

---

## 📁 프로젝트 구조

### **📂 data/기상청AWS/**

**🌤️ 기상청 데이터 파일들**

- `기상청AWS매분자료조회.md` - AWS 매분자료 API 가이드
- `기상청지점정보조회.md` - 지점정보 API 가이드
- `kma_aws_collector.py` - 실시간 데이터 수집기
- `kma_AWS.sql` - 기상 데이터 테이블

**🏪 소상공인 데이터 파일들**

- `소상공인시장진흥공단_상가(상권)정보.md` - 데이터 명세서
- `sbiz_create_smb_ods_table.sql` - ODS 테이블 생성
- `sbiz_load_smb_data.py` - CSV → ODS 적재
- `sbiz_create_fact_smb_master.sql` - FACT 테이블 생성
- `sbiz_insert_fact_smb_master.sql` - ODS → FACT 변환
- `sbiz_create_mart_addr3_top_category.sql` - MART 테이블 생성
- `sbiz_ods_vs_fact_mapping.sql` - 매핑 문서

### **📂 lg7pilot/**

**📰 네이버뉴스 크롤링 모듈**

- `lg7_scrap_naver_news_list.py` - 네이버뉴스 목록페이지에서 기사 URL을 수집
- `lg7_scrap_naver_news_item.py` - 수집된 URL에서 실제 뉴스 내용을 크롤링

**🔧 GeekNews 크롤링 모듈**

- `geeknews_crawling.py` - GeekNews 통합 크롤링 시스템 (최신글, 댓글페이지 URL 수집 및 콘텐츠 수집)
- `lg7_scrap_gn_list.py` - GeekNews 목록페이지에서 토픽 URL 수집
- `lg7_scrap_gn_item.py` - GeekNews 토픽의 상세 내용과 댓글 수집

**🗄️ 데이터베이스 스키마**

- `lg7_naver.sql` - 네이버뉴스 크롤링을 위한 데이터베이스 테이블 생성
- `geeknews_crawling_table.sql` - GeekNews 크롤링을 위한 데이터베이스 테이블 생성

**🚇 지하철 데이터**

- `tb_subway.sql` - 서울지하철 승차 데이터 테이블 생성 및 CSV 데이터 임포트
- `tb_subway_Quiz.sql` - 지하철 데이터 분석을 위한 퀴즈/예제 쿼리
- `import_data.csv` - 서울지하철 승차 현황 데이터

---

## 🚀 실습 실행 순서

### **소상공인 ETL 파이프라인 구축**

#### 1단계: ODS 구축

```bash
# 1. ODS 테이블 생성
SOURCE sbiz_create_smb_ods_table.sql;

# 2. CSV 데이터 적재 (270만건)
python sbiz_load_smb_data.py
```

#### 2단계: FACT 구축

```bash
# 3. FACT 테이블 생성
SOURCE sbiz_create_fact_smb_master.sql;

# 4. ODS → FACT 데이터 변환
SOURCE sbiz_insert_fact_smb_master.sql;
```

#### 3단계: MART 구축

```bash
# 5. MART 테이블 생성 및 분석
SOURCE sbiz_create_mart_addr3_top_category.sql;
```

### **웹 크롤링 실행**

#### 데이터베이스 설정

```sql
-- 네이버뉴스용
source lg7_naver.sql

-- GeekNews용
source geeknews_crawling_table.sql

-- 지하철 데이터용
source tb_subway.sql
```

#### 크롤링 실행

```bash
# GeekNews 통합 크롤링 (URL수집 + 콘텐츠수집)
python geeknews_crawling.py

# 네이버뉴스 크롤링
python lg7_scrap_naver_news_list.py    # URL 수집
python lg7_scrap_naver_news_item.py    # 콘텐츠 수집
```

---

## ⚙️ 주요 기능

### **🌐 웹 크롤링 기능**

- **로봇 배제 표준 준수**: 페이지 로딩 시 3-10초 대기로 서버 부하 방지
- **오류 처리**: 타임아웃 발생 시 브라우저 재시작 및 재시도 로직
- **중복 방지**: DB 조회를 통한 중복 URL 수집 방지
- **단계별 처리**: URL 수집 → 콘텐츠 크롤링 2단계 분리 처리

### **📊 데이터 관리**

- **상태 관리**: 크롤링 단계별 상태 추적 (처리전 → 수집중 → 완료/실패)
- **재시도 메커니즘**: 실패한 항목에 대한 재시도 카운트 관리
- **데이터 무결성**: 외래키 제약조건과 유니크 인덱스로 데이터 품질 보장

---

## 🛠️ 기술 스택

- **Python**: 메인 프로그래밍 언어
- **Playwright**: 동적 웹페이지 렌더링 및 브라우저 자동화
- **BeautifulSoup**: HTML 파싱 및 데이터 추출
- **MariaDB/MySQL**: 데이터 저장 및 관리
- **PyMySQL/MariaDB Connector**: 데이터베이스 연결

---

## 🗄️ 데이터베이스 구조

### **네이버뉴스**

- `news_scrap_ready`: 크롤링 대기 URL 관리
- `news_master`: 수집된 뉴스 기사 저장

### **GeekNews**

- `gn_scrap_ready`: GeekNews 토픽 URL 대기열
- `gn_master`: GeekNews 토픽 및 댓글 데이터 저장

### **서울지하철**

- `tb_subway`: 노선별/월별 승차 승객수 데이터

---

## 🎯 학습 성과 확인

### **ETL 파이프라인 완성도 체크**

- [ ] **Extract**: CSV 파일 270만건 적재 완료
- [ ] **Transform**: ODS → FACT 매핑 및 데이터 정제 완료
- [ ] **Load**: MART 테이블 생성 및 분석 결과 도출 완료

### **데이터 품질 확인**

```sql
-- 각 단계별 데이터 건수 비교
SELECT 'ODS' as layer, COUNT(*) as records FROM tb_smb_ods
UNION ALL
SELECT 'FACT' as layer, COUNT(*) as records FROM fact_smb_master
UNION ALL
SELECT 'MART' as layer, COUNT(*) as records FROM mart_addr3_top_category;
```

### **비즈니스 인사이트 도출**

- **전국에서 가장 많이 1위를 차지하는 업종은?**
- **상권이 가장 활발한 행정동은 어디?**
- **시도별 상권 분포 특성은?**

---

## 📈 데이터 활용

수집된 데이터는 다음과 같은 분석에 활용할 수 있습니다:

- 뉴스 트렌드 분석
- 기술 토픽 인기도 추이
- 댓글 감정 분석
- 서울지하철 이용 패턴 분석

---

## 🔗 추가 학습 자료

### **데이터 엔지니어링 심화 개념**

- **Apache Airflow**: ETL 워크플로우 오케스트레이션
- **Apache Spark**: 대용량 데이터 분산 처리
- **dbt (Data Build Tool)**: SQL 기반 데이터 변환
- **Apache Kafka**: 실시간 스트리밍 데이터 처리

### **클라우드 데이터 플랫폼**

- **AWS**: Redshift, Glue, EMR, Kinesis
- **GCP**: BigQuery, Dataflow, Pub/Sub
- **Azure**: Synapse Analytics, Data Factory

### **데이터 모델링**

- **Star Schema**: 팩트 테이블 중심의 차원 모델링
- **Snowflake Schema**: 정규화된 차원 테이블 구조
- **Data Vault**: 엔터프라이즈 데이터웨어하우스 모델링

---

## 📝 학습 노트

### **오늘의 핵심 깨달음**

```
1. ETL은 단순한 데이터 이동이 아니라 비즈니스 가치 창출 과정
2. ODS-FACT-MART 계층 구조로 데이터 품질과 성능 모두 확보
3. SQL의 WITH절과 윈도우 함수로 복잡한 분석도 효율적으로 처리
4. 대용량 데이터도 적절한 배치 처리와 인덱스로 안정적 처리 가능
```

### **다음 학습 계획**

- [ ] 실시간 스트리밍 ETL 구현
- [ ] 데이터 품질 모니터링 자동화
- [ ] 클라우드 기반 ETL 파이프라인 구축
- [ ] 머신러닝 파이프라인과 ETL 연동

---

## ⚠️ 주의사항

- 크롤링 시 로봇 배제 표준을 준수합니다
- 서버 부하 방지를 위해 적절한 딜레이를 유지합니다
- 개인정보나 민감한 정보는 수집하지 않습니다
- 사이트 이용약관을 준수하여 사용하시기 바랍니다

---

**🔧 개발환경**: Python 3.x, MariaDB/MySQL, Windows
