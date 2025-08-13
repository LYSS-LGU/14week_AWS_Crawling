# -*- coding: utf-8 -*-
# C:\data\기상청AWS\check_duckdb.py
# DuckDB 파일 내용 확인용 스크립트

import duckdb
import os

def check_duckdb_file(db_path):
    """DuckDB 파일 내용 확인"""
    if not os.path.exists(db_path):
        print(f"[ERROR] 파일이 존재하지 않습니다: {db_path}")
        return
    
    print(f"[INFO] DuckDB 파일 확인: {db_path}")
    print("=" * 60)
    
    try:
        conn = duckdb.connect(db_path)
        
        # 1. 테이블 목록
        print("\n[TABLES] 테이블 목록:")
        tables = conn.execute("SHOW TABLES").fetchall()
        
        if not tables:
            print("  테이블이 없습니다.")
            return
            
        for table in tables:
            print(f"  - {table[0]}")
        
        # 2. 각 테이블 정보
        for table_name in [t[0] for t in tables]:
            print(f"\n[TABLE] {table_name}")
            print("-" * 40)
            
            # 건수
            count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"  데이터 건수: {count:,}건")
            
            # 컬럼 정보
            columns = conn.execute(f"DESCRIBE {table_name}").fetchall()
            print(f"  컬럼 수: {len(columns)}개")
            print("  컬럼 목록:")
            for col in columns:
                print(f"    - {col[0]} ({col[1]})")
            
            # 샘플 데이터 (처음 3건)
            if count > 0:
                print("  샘플 데이터 (3건):")
                samples = conn.execute(f"SELECT * FROM {table_name} LIMIT 3").fetchall()
                for i, row in enumerate(samples, 1):
                    print(f"    {i}: {row}")
        
        conn.close()
        
    except Exception as e:
        print(f"[ERROR] 오류 발생: {e}")

if __name__ == "__main__":
    db_path = "C:/data/기상청AWS/smb_data.duckdb"
    check_duckdb_file(db_path)