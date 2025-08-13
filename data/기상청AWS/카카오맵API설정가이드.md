# 카카오맵 API 설정 가이드

## 1. 카카오 Developer 계정 생성
1. https://developers.kakao.com/ 접속
2. 카카오 계정으로 로그인
3. "내 애플리케이션" → "애플리케이션 추가하기"

## 2. 애플리케이션 등록
1. **앱 이름**: `기상청지점정보수집`
2. **사업자명**: 개인/회사명 입력
3. **애플리케이션 생성** 클릭

## 3. REST API 키 발급
1. 생성된 애플리케이션 클릭
2. **요약정보** 탭에서 **REST API 키** 복사
3. 예시: `1234567890abcdef1234567890abcdef`

## 4. 플랫폼 등록
1. **플랫폼** 탭 클릭
2. **Web 플랫폼 등록** 클릭
3. **사이트 도메인**: `http://localhost` 입력

## 5. API 활성화
1. **제품 설정** → **카카오맵** 클릭
2. **Local** API 활성화
3. **좌표계 변환** 권한 확인

## 6. 스크립트에 API 키 설정
`kma_station_collector.py` 파일에서:
```python
# 기존
KAKAO_API_KEY = "YOUR_KAKAO_API_KEY_HERE"

# 변경 (실제 발급받은 키로 교체)
KAKAO_API_KEY = "1234567890abcdef1234567890abcdef"
```

## 7. 테스트 실행
```bash
python kma_station_collector.py
```

## 주의사항
- **월 300,000건** 무료 제한
- 지점정보 수집은 **1회성**이므로 충분함
- 키는 **보안**에 주의 (GitHub 등에 업로드 금지)