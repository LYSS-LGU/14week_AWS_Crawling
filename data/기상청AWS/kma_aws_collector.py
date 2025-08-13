# C:/data/기상청AWS/kma_aws_collector.py
# 기상청 API를 활용한 AWS 매분자료 수집 및 DB 저장

import mariadb
import sys
import requests
import json
from datetime import datetime, timedelta
import time

# 기상청 API 설정
BASE_URL = "https://apihub.kma.go.kr/api/typ01/cgi-bin/url/nph-aws2_min"
AUTH_KEY = "발급받은_API_KEY"

# 데이터베이스 연결 설정
try:
    conn_tar = mariadb.connect(
        user="lguplus7",
        password="발급받은_DB_PASSWORD",
        host="localhost",
        port=3306,
        database="cp_data"
    )
    print("데이터베이스 연결 성공")
except mariadb.Error as e:
    print(f"MariaDB 연결 에러: {e}")
    sys.exit(1)

tar_cur = conn_tar.cursor()

def get_weather_data(tm1=None, tm2=None, stn=0, disp=1, help_param=2):
    """
    기상청 AWS 매분자료 API 호출 함수
    
    Args:
        tm1: 시작시간 (YYYYMMDDHHMM)
        tm2: 종료시간 (YYYYMMDDHHMM) 
        stn: 지점번호 (0: 전체지점)
        disp: 표출형태 (1: CSV 형태)
        help_param: 도움말 (2: 값만 표시)
    
    Returns:
        API 응답 텍스트
    """
    params = {
        'authKey': AUTH_KEY,
        'stn': stn,
        'disp': disp,
        'help': help_param
    }
    
    if tm1:
        params['tm1'] = tm1
    if tm2:
        params['tm2'] = tm2
        
    try:
        response = requests.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        print(f"API 호출 성공: {response.url}")
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"API 호출 에러: {e}")
        return None

def parse_weather_data(raw_data):
    """
    API 응답 데이터 파싱 함수
    disp=1 (CSV 형태)로 받은 데이터를 파싱
    
    Args:
        raw_data: API 응답 원본 텍스트
        
    Returns:
        파싱된 데이터 리스트
    """
    if not raw_data:
        return []
    
    lines = raw_data.strip().split('\n')
    parsed_data = []
    
    for line_num, line in enumerate(lines, 1):
        if line.strip() and not line.startswith('#'):
            # CSV 형태로 분리 (쉼표로 구분)
            fields = line.split(',')
            print(f"라인 {line_num}: 필드 개수 = {len(fields)}, 데이터 = {line[:100]}...")
            if len(fields) >= 18:  # 최소 필드 수 확인 (실제 응답은 19개 필드)
                try:
                    data_dict = {
                        'yyyymmddhhmi': fields[0].strip(),
                        'stn': fields[1].strip(),
                        'wd1': fields[2].strip(),
                        'ws1': fields[3].strip(),
                        'wds': fields[4].strip(),
                        'wss': fields[5].strip(),
                        'wd10': fields[6].strip(),
                        'ws10': fields[7].strip(),
                        'ta': fields[8].strip(),
                        're': fields[9].strip(),
                        'rn_15m': fields[10].strip(),
                        'rn_60m': fields[11].strip(),
                        'rn_12h': fields[12].strip(),
                        'rn_day': fields[13].strip(),
                        'hm': fields[14].strip(),
                        'pa': fields[15].strip(),
                        'ps': fields[16].strip(),
                        'td': fields[17].strip(),
                        'org_data': line.strip(),
                        'update_dt': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    parsed_data.append(data_dict)
                except Exception as e:
                    print(f"데이터 파싱 에러: {e}, 라인: {line}")
                    continue
    
    return parsed_data

def save_to_database(data_list):
    """
    파싱된 데이터를 데이터베이스에 저장 (중복 체크 포함)
    
    Args:
        data_list: 파싱된 데이터 리스트
    """
    if not data_list:
        print("저장할 데이터가 없습니다.")
        return
    
    # 중복 체크 쿼리
    duplicate_check_query = """
    SELECT COUNT(*) FROM tb_weather_aws1 WHERE yyyymmddhhmi = ? AND stn = ?
    """
    
    # 데이터 삽입 쿼리
    insert_query = """
    INSERT INTO tb_weather_aws1 (
        yyyymmddhhmi, stn, wd1, ws1, wds, wss, wd10, ws10, 
        ta, re, rn_15m, rn_60m, rn_12h, rn_day, hm, pa, ps, td, 
        org_data, update_dt
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    saved_count = 0
    duplicate_count = 0
    
    for data in data_list:
        try:
            # 중복 체크: yyyymmddhhmi와 stn 동일한 데이터가 있는지 확인
            tar_cur.execute(duplicate_check_query, (data['yyyymmddhhmi'], data['stn']))
            existing_count = tar_cur.fetchone()[0]
            
            if existing_count > 0:
                duplicate_count += 1
                print(f"중복 데이터 스킵: {data['yyyymmddhhmi']}, 지점: {data['stn']}")
                continue
            
            # 중복이 아니면 삽입
            tar_cur.execute(insert_query, (
                data['yyyymmddhhmi'], data['stn'], data['wd1'], data['ws1'],
                data['wds'], data['wss'], data['wd10'], data['ws10'],
                data['ta'], data['re'], data['rn_15m'], data['rn_60m'],
                data['rn_12h'], data['rn_day'], data['hm'], data['pa'],
                data['ps'], data['td'], data['org_data'], data['update_dt']
            ))
            conn_tar.commit()
            saved_count += 1
            
        except mariadb.Error as e:
            print(f"데이터 저장 에러: {e}")
            print(f"문제 데이터: {data}")
    
    print(f"데이터베이스 저장 완료: {saved_count}건 (중복 스킵: {duplicate_count}건)")

def collect_recent_data():
    """
    최근 10분간의 전체 지점 데이터 수집
    """
    # 현재 시간 기준으로 10분 전 시간 계산
    now = datetime.now()
    tm2 = now.strftime('%Y%m%d%H%M')
    tm1 = (now - timedelta(minutes=10)).strftime('%Y%m%d%H%M')
    
    print(f"데이터 수집 시간 범위: {tm1} ~ {tm2}")
    
    # API 호출
    raw_data = get_weather_data(tm1=tm1, tm2=tm2, stn=0)
    
    if raw_data:
        print("API 응답 데이터 샘플:")
        print(raw_data[:500] + "..." if len(raw_data) > 500 else raw_data)
        
        # 데이터 파싱
        parsed_data = parse_weather_data(raw_data)
        print(f"파싱된 데이터 건수: {len(parsed_data)}")
        
        # 데이터베이스 저장
        if parsed_data:
            save_to_database(parsed_data)
        else:
            print("파싱된 데이터가 없습니다.")
    else:
        print("API 호출 실패")

if __name__ == "__main__":
    print("기상청 AWS 매분자료 수집 시작")
    collect_recent_data()
    print("데이터 수집 완료")
    
    # 연결 종료
    tar_cur.close()
    conn_tar.close()