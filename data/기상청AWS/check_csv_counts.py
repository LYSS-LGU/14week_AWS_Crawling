# C:\data\기상청AWS\check_csv_counts.py
# CSV 파일별 데이터 건수 확인

import os
import glob

csv_dir = "C:\\data\\기상청AWS\\소상공인시장진흥공단_상가(상권)정보_20250630"
csv_files = glob.glob(os.path.join(csv_dir, "*.csv"))

total_lines = 0
print("=" * 60)
print("CSV 파일별 데이터 건수")
print("=" * 60)

for csv_file in sorted(csv_files):
    file_name = os.path.basename(csv_file)
    
    try:
        with open(csv_file, 'r', encoding='utf-8-sig') as f:
            lines = sum(1 for line in f) - 1  # 헤더 제외
            total_lines += lines
            print(f"{file_name}: {lines:,}건")
    except Exception as e:
        print(f"{file_name}: 오류 - {e}")

print("=" * 60)
print(f"총 데이터: {total_lines:,}건")
print(f"강사님 예상: 2,708,856건")
print(f"차이: {2708856 - total_lines:,}건")
print("=" * 60)