# 기상청 AWS 매분자료 수집 시스템 완전 가이드

## 📌 기본 정보
- **원본 URL**: https://apihub.kma.go.kr/
- **API 엔드포인트**: https://apihub.kma.go.kr/api/typ01/cgi-bin/url/nph-aws2_min
- **인증키**: dQ4QUFVQTUeOEFBVUA1HKg
- **데이터베이스**: cp_data
- **테이블**: tb_weather_aws1

## 🗂️ 파일 구성
- `kma_aws_collector.py`: 기상청 API 호출 및 데이터 수집/저장 메인 스크립트
- `kma_aws_scheduler.py`: 정기적 데이터 수집을 위한 스케줄러
- `kma_AWS.sql`: 데이터 저장용 테이블 생성 스크립트
- `requirements.txt`: 필요한 Python 패키지 목록

## ⚙️ 설치 및 설정

### 1. 패키지 설치
```bash
pip install -r requirements.txt
```

### 2. 데이터베이스 설정
- HeidiSQL에서 `kma_AWS.sql` 실행하여 테이블 생성
- MariaDB 연결 정보 확인 (user: lguplus7, database: cp_data)

### 3. API 키 설정
- 현재 설정된 인증키: `dQ4QUFVQTUeOEFBVUA1HKg`
- 필요시 `kma_aws_collector.py`의 `AUTH_KEY` 변수 수정

## 🚀 사용법

### 단발성 데이터 수집
```bash
python kma_aws_collector.py
```

### 정기적 데이터 수집 (10분마다)
```bash
python kma_aws_scheduler.py
```

## 📊 수집 데이터 구조
- **yyyymmddhhmi**: 관측시간 (년월일시분)
- **stn**: 지점번호
- **wd1, ws1**: 1분 평균 풍향/풍속
- **wds, wss**: 순간 최대 풍향/풍속  
- **wd10, ws10**: 10분 평균 풍향/풍속
- **ta**: 기온
- **re**: 강수량
- **rn_15m, rn_60m, rn_12h, rn_day**: 15분/1시간/12시간/일 강수량
- **hm**: 습도
- **pa, ps**: 현지/해면 기압
- **td**: 이슬점온도

## ⚠️ 주의사항
- 전체 지점 조회 시 10분 제한
- 1개 지점 조회 시 하루 이내 제한
- API 호출 실패 시 로그 확인 필요

## API 파라미터

| 인자명 | 의미 | 설명 |
|--------|------|------|
| **tm1** | 년월일시분(KST) | 조회할 시간구간의 시작시간 (없으면 종료시간과 같음)<br>기간: 전체지점이면 10분, 1개 지점이면 하루이내로 처리 |
| **tm2** | 년월일시분(KST) | 조회할 시간구간의 종료시간 (없으면 현재시간) |
| **stn** | 지점번호 | 해당 지점의 정보 표출 (0 이거나 없으면 전체지점) |
| **disp** | 표출형태 | 0: 변수별로 일정한 길이 유지, 포트란에 적합 (default)<br>1: 구분자(,)로 구분, 엑셀에 적합 |
| **help** | 도움말 | 0: 시작과 종료표시 + 변수명 (default)<br>1: 0 + 변수에 대한 설명<br>2: 전혀 표시않음 (값만 표시) |
| **authKey** | 인증키 | 발급된 API 인증키 |

## 예시 URL
```
https://apihub.kma.go.kr/api/typ01/cgi-bin/url/nph-aws2_min?tm2=202302010900&stn=0&disp=0&help=1&authKey=dQ4QUFVQTUeOEFBVUA1HKg
```