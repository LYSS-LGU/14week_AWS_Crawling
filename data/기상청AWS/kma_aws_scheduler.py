# C:/data/기상청AWS/kma_aws_scheduler.py  
# 기상청 AWS 매분자료 정기 수집 스케줄러

import schedule
import time
from kma_aws_collector import collect_recent_data
from datetime import datetime

def scheduled_collection():
    """
    스케줄된 데이터 수집 실행
    """
    print(f"\n=== {datetime.now()} 정기 수집 시작 ===")
    try:
        collect_recent_data()
        print(f"=== {datetime.now()} 정기 수집 완료 ===\n")
    except Exception as e:
        print(f"정기 수집 중 에러 발생: {e}\n")

def run_scheduler():
    """
    스케줄러 실행 - 매 10분마다 데이터 수집
    """
    print("기상청 AWS 매분자료 정기 수집 스케줄러 시작")
    print("매 10분마다 데이터를 수집합니다.")
    
    # 매 10분마다 실행하도록 스케줄 설정
    schedule.every(10).minutes.do(scheduled_collection)
    
    # 즉시 한 번 실행
    print("초기 데이터 수집 실행...")
    scheduled_collection()
    
    # 스케줄 실행 루프
    while True:
        schedule.run_pending()
        time.sleep(60)  # 1분마다 스케줄 확인

if __name__ == "__main__":
    run_scheduler()