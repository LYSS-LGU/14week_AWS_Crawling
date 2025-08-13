# C:\githome\14week_AWS\data\기상청AWS\sbiz_load_smb_data.py
# 소상공인시장진흥공단 상가정보 CSV 파일을 DB에 적재하는 스크립트 (강사님 버전)

import os
import csv
import mariadb
import sys
from datetime import datetime
import glob
import re

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

def check_table_exists(cursor):
    """테이블 존재 여부 확인"""
    query = """
    SELECT COUNT(*) 
    FROM information_schema.tables 
    WHERE table_schema = 'cp_data' AND table_name = 'tb_smb_ods'
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return result[0] > 0

def clean_text(text):
    """데이터 중간에 있는 더블 쿼트 및 특수문자 처리"""
    if text is None:
        return ''
    
    # 문자열로 변환
    text = str(text)
    
    # 더블 쿼트 제거 (데이터 중간에 있는 것들)
    text = text.replace('"', '')
    
    # 탭, 개행문자 제거
    text = text.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
    
    # 연속된 공백을 하나로 
    text = re.sub(r'\s+', ' ', text)
    
    # 앞뒤 공백 제거
    text = text.strip()
    
    return text

def insert_data(cursor, row_data):
    """데이터 삽입 - col1부터 col39까지"""
    insert_query = """
    INSERT INTO tb_smb_ods (
        col1, col2, col3, col4, col5, col6, col7, col8, col9, col10,
        col11, col12, col13, col14, col15, col16, col17, col18, col19, col20,
        col21, col22, col23, col24, col25, col26, col27, col28, col29, col30,
        col31, col32, col33, col34, col35, col36, col37, col38, col39
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    """
    cursor.execute(insert_query, row_data)

def get_existing_store_ids(cursor):
    """이미 존재하는 상가업소번호 조회 (중복 방지) - col1이 상가업소번호"""
    try:
        query = "SELECT col1 FROM tb_smb_ods WHERE col1 IS NOT NULL"
        cursor.execute(query)
        return set(row[0] for row in cursor.fetchall() if row[0])
    except:
        return set()

def process_csv_file(file_path, cursor, existing_ids):
    """CSV 파일 처리 - 더블 쿼트 이슈 해결"""
    file_name = os.path.basename(file_path)
    print(f"\n📂 처리 중: {file_name}")
    
    inserted_count = 0
    skipped_count = 0
    error_count = 0
    
    try:
        # 더블 쿼트 이슈 해결을 위해 quoting 옵션 조정
        with open(file_path, 'r', encoding='utf-8-sig', newline='') as f:
            # CSV 읽기 옵션 조정 - 더블 쿼트 이슈 해결
            csv_reader = csv.reader(f, 
                                  delimiter=',', 
                                  quotechar='"',
                                  skipinitialspace=True,
                                  quoting=csv.QUOTE_MINIMAL)
            
            header = next(csv_reader)  # 헤더 스킵
            print(f"  📋 헤더 컬럼 수: {len(header)}")
            
            for row_num, row in enumerate(csv_reader, start=2):
                try:
                    # 빈 행 스킵
                    if not row or all(not cell.strip() for cell in row):
                        continue
                    
                    # 컬럼 수가 39개가 아닌 경우 처리
                    if len(row) < 39:
                        # 부족한 컬럼은 빈 문자열로 채움
                        row.extend([''] * (39 - len(row)))
                    elif len(row) > 39:
                        # 초과 컬럼은 잘라냄
                        row = row[:39]
                        print(f"  ⚠️ 행 {row_num}: 컬럼 수 초과, 39개로 조정")
                    
                    # 첫 번째 컬럼(상가업소번호)로 중복 체크
                    store_id = clean_text(row[0])
                    
                    if not store_id:
                        error_count += 1
                        continue
                    
                    # 중복 체크
                    if store_id in existing_ids:
                        skipped_count += 1
                        continue
                    
                    # 모든 데이터 정리
                    cleaned_row = [clean_text(cell) for cell in row]
                    
                    # 데이터 삽입
                    insert_data(cursor, cleaned_row)
                    existing_ids.add(store_id)
                    inserted_count += 1
                    
                    # 진행상황 표시 (1000건마다)
                    if inserted_count % 1000 == 0:
                        print(f"  💾 {inserted_count:,}건 삽입 완료...")
                        
                except Exception as e:
                    error_count += 1
                    if error_count <= 5:  # 처음 5개 오류만 출력
                        print(f"  ❌ 행 {row_num} 오류: {str(e)[:100]}")
                    continue
    
    except Exception as e:
        print(f"  ❌ 파일 읽기 오류: {e}")
        return 0, 0, 0
    
    print(f"  ✅ 완료: 삽입 {inserted_count:,}건, 중복 {skipped_count:,}건, 오류 {error_count}건")
    return inserted_count, skipped_count, error_count

def main():
    """메인 함수"""
    print("=" * 60)
    print("🏪 소상공인 상가정보 CSV 데이터 적재 시작 (강사님 버전)")
    print("=" * 60)
    
    cursor = conn_tar.cursor()
    
    try:
        # 테이블 확인
        if not check_table_exists(cursor):
            print("❌ tb_smb_ods 테이블이 존재하지 않습니다.")
            print("   sbiz_create_smb_ods_table.sql을 먼저 실행해주세요.")
            return
        
        print("✅ tb_smb_ods 테이블 확인 완료")
        
        # 기존 데이터 확인
        existing_ids = get_existing_store_ids(cursor)
        print(f"📊 기존 데이터: {len(existing_ids):,}건")
        
        # CSV 파일 목록 가져오기
        # 경로 변경: 현재 프로젝트 구조에 맞게 수정
        csv_dir = "C:\\githome\\14week_AWS\\data\\기상청AWS\\소상공인시장진흥공단_상가(상권)정보_20250630"
        csv_files = glob.glob(os.path.join(csv_dir, "*.csv"))
        
        if not csv_files:
            print(f"❌ CSV 파일을 찾을 수 없습니다: {csv_dir}")
            return
        
        print(f"📁 처리할 CSV 파일: {len(csv_files)}개")
        
        # 전체 통계
        total_inserted = 0
        total_skipped = 0
        total_errors = 0
        
        # 각 CSV 파일 처리
        for csv_file in sorted(csv_files):
            inserted, skipped, errors = process_csv_file(csv_file, cursor, existing_ids)
            total_inserted += inserted
            total_skipped += skipped
            total_errors += errors
            
            # 파일마다 커밋 (메모리 관리)
            conn_tar.commit()
            print(f"  💾 DB 커밋 완료")
        
        print("\n" + "=" * 60)
        print("📊 전체 처리 결과")
        print(f"  ✅ 총 삽입: {total_inserted:,}건")
        print(f"  ⏭️ 총 중복: {total_skipped:,}건")
        print(f"  ❌ 총 오류: {total_errors}건")
        print(f"  📈 최종 데이터: {len(existing_ids):,}건")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ 처리 중 오류 발생: {e}")
        conn_tar.rollback()
        
    finally:
        cursor.close()
        conn_tar.close()
        print("🔌 DB 연결 종료")

if __name__ == "__main__":
    start_time = datetime.now()
    main()
    end_time = datetime.now()
    print(f"\n⏱️ 실행 시간: {end_time - start_time}")