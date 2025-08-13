# geeknews_crawling.py
"""
GeekNews (https://news.hada.io/) 크롤링 스크립트
최신글과 댓글을 수집하여 데이터베이스에 저장합니다.
"""

import pymysql  # 데이터베이스 연결용
import sys
import time
import os
from datetime import datetime
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import re

# Windows 한글 인코딩 문제 해결
os.environ['PYTHONIOENCODING'] = 'utf-8'
sys.stdout.reconfigure(encoding='utf-8')

# 데이터베이스 연결 설정 - 패스워드 이스케이프 처리
try:
    conn = pymysql.connect(
        user="lguplus7",
        password=r"발급받은_DB_PASSWORD",  # raw string으로 특수문자 처리
        host="localhost",
        port=3306,
        database="cp_data",
        charset='utf8mb4'
    )
    print("데이터베이스 연결 성공")
except Exception as e:
    print(f"데이터베이스 연결 오류: {e}")
    sys.exit(1)

# 커서 생성
cur = conn.cursor()

def save_url_to_ready(url, source_type='1'):
    """
    URL을 gn_scrap_ready 테이블에 저장합니다.
    source_type: '1' = GeekNews
    """
    # 유효한 GeekNews 아이템 URL인지 확인
    if not url.startswith('https://news.hada.io/topic?id='):
        print(f"    유효하지 않은 URL 형식: {url[:50]}...")
        return False
        
    try:
        # 중복 체크
        cur.execute("SELECT seq_no FROM gn_scrap_ready WHERE source_url = %s", (url,))
        if cur.fetchone():
            print(f"    이미 존재하는 URL: {url[:50]}...")
            return False
        
        # 새 URL 저장
        create_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cur.execute(
            "INSERT INTO gn_scrap_ready (source_type, source_url, status, create_dt) VALUES (%s, %s, '0', %s)",
            (source_type, url, create_time)
        )
        conn.commit()
        print(f"    새 URL 저장 성공: {url[:50]}...")
        return True
        
    except Exception as e:
        print(f"    URL 저장 오류: {e}")
        conn.rollback()  # 오류 시 롤백
        return False

def scrape_latest_items():
    """최신글 페이지에서 아이템들을 수집합니다."""
    
    print("\n최신글 수집 시작...")
    
    with sync_playwright() as p:
        # 브라우저 시작
        browser = p.firefox.launch(headless=True)
        page = browser.new_page()
        
        try:
            # 최신글 페이지로 이동
            print("페이지 로드 중: https://news.hada.io/new")
            page.goto("https://news.hada.io/new", timeout=30000)
            time.sleep(3)  # 로봇 배제 표준 준수 (3초 대기)
            
            # 페이지 내용 가져오기
            content = page.content()
            soup = BeautifulSoup(content, 'html.parser')
            
            # 다양한 셀렉터를 시도해서 아이템 찾기
            items = []
            
            # 방법 1: topictitle 클래스가 있는 div 직접 찾기
            topic_divs = soup.find_all('div', class_='topictitle')
            if topic_divs:
                print(f"방법1: topictitle 클래스로 {len(topic_divs)}개 발견")
                for topic_div in topic_divs:
                    parent = topic_div.parent
                    if parent:
                        items.append(parent)
            
            # 방법 2: 링크가 topic?id= 패턴인 것들 찾기 (상대경로)
            if not items:
                all_links = soup.find_all('a', href=True)
                topic_links = [link for link in all_links if link.get('href', '').startswith('topic?id=')]
                print(f"방법2: topic 링크로 {len(topic_links)}개 발견")
                for link in topic_links[:20]:  # 최대 20개로 제한
                    items.append(link.parent if link.parent else link)
            
            print(f"  🔍 찾은 아이템 수: {len(items)}개")
            
            saved_count = 0
            for idx, item in enumerate(items, 1):
                
                # 제목과 링크 추출 - 더 유연한 방식
                link_elem = None
                title = ""
                
                # 우선순위 변경: GeekNews 내부 링크를 먼저 찾기
                
                # 방법 1: 아이템 내에서 topic 링크 찾기 - 댓글 링크가 아닌 메인 링크 (최우선)
                topic_links = item.find_all('a', href=lambda x: x and 'topic?id=' in x)
                for t_link in topic_links:
                    href = t_link.get('href', '')
                    if 'go=comments' not in href:  # 댓글 페이지가 아닌 메인 페이지 링크
                        link_elem = t_link
                        title = t_link.get_text(strip=True)
                        break
                
                # 방법 2: topictitle div에서 제목 텍스트 가져오기 (링크는 위에서 찾은 것 사용)
                if link_elem:
                    title_div = item.find('div', class_='topictitle')
                    if title_div:
                        title_link = title_div.find('a')
                        if title_link:
                            title = title_link.get_text(strip=True)  # 외부 링크의 제목 텍스트 사용
                
                # 방법 3: 직접 링크인 경우 (마지막 수단)
                if not link_elem and item.name == 'a':
                    href = item.get('href', '')
                    if 'topic?id=' in href and 'go=comments' not in href:
                        link_elem = item
                        title = item.get_text(strip=True)
                
                if not link_elem:
                    continue
                
                # 제목과 URL 추출
                title = link_elem.get_text(strip=True)
                relative_url = link_elem.get('href', '')
                
                # 상대 경로를 절대 경로로 변환
                if relative_url.startswith('/topic?id='):
                    full_url = f"https://news.hada.io{relative_url}"
                elif relative_url.startswith('topic?id='):
                    full_url = f"https://news.hada.io/{relative_url}"
                elif relative_url.startswith('https://news.hada.io/topic?id='):
                    full_url = relative_url
                else:
                    continue  # 유효하지 않은 URL은 건너뜸
                
                # 크롤링 시간 기록
                crawl_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                # 모든 아이템 정보 출력
                print(f"\n[{idx}] {title[:50]}...")
                print(f"    URL: {full_url}")
                print(f"    크롤링 시간: {crawl_time}")
                
                # URL 저장 시도 및 결과 표시
                if save_url_to_ready(full_url):
                    saved_count += 1
                    print(f"    결과: 새 URL 저장 완료")
                else:
                    print(f"    결과: 이미 존재하는 URL (건너뜀)")
                
                # 3초 대기 (로봇 배제 표준)
                time.sleep(3)
            
            print(f"\n최신글 저장 결과: {saved_count}개 신규 저장")
            
            # "토픽 더 불러오기" 버튼 클릭 (1회)
            try:
                # 다양한 셀렉터로 더보기 버튼 찾기
                load_more_selectors = [
                    "div.next.commentTD > a",
                    ".next a",
                    "a:contains('토픽 더')",
                    "a[href*='page=']"
                ]
                
                load_more_button = None
                for selector in load_more_selectors:
                    try:
                        load_more_button = page.locator(selector)
                        if load_more_button.count() > 0:
                            break
                    except:
                        continue
                
                if load_more_button and load_more_button.count() > 0:
                    print("\n'토픽 더 불러오기' 클릭...")
                    time.sleep(3)  # 클릭 전 대기
                    load_more_button.click()
                    time.sleep(5)  # 로드 대기
                    
                    # 새로 로드된 콘텐츠에서 아이템 수집
                    content = page.content()
                    soup = BeautifulSoup(content, 'html.parser')
                    
                    # 다시 아이템 찾기
                    article = soup.select_one('body > main > article')
                    if article:
                        new_item_containers = article.select('div > div')
                        new_items = []
                        for container in new_item_containers:
                            if container.select_one('div.topictitle'):
                                new_items.append(container)
                        
                        print(f"전체 아이템 수: {len(new_items)}개 (기존: {len(items)}개)")
                        
                        additional_saved = 0
                        # 기존 아이템 수 이후의 새 아이템만 처리
                        for idx, item in enumerate(new_items[len(items):], len(items)+1):
                            # 동일한 유연한 링크 찾기 로직 적용
                            link_elem = None
                            title_div = item.find('div', class_='topictitle')
                            if title_div:
                                link_elem = title_div.find('a')
                            
                            if not link_elem and item.name == 'a':
                                link_elem = item
                                
                            if not link_elem:
                                # 댓글이 아닌 메인 topic 링크를 찾기
                                topic_links = item.find_all('a', href=lambda x: x and 'topic?id=' in x)
                                for t_link in topic_links:
                                    href = t_link.get('href', '')
                                    if 'go=comments' not in href:  # 댓글 페이지가 아닌 메인 페이지 링크
                                        link_elem = t_link
                                        break
                            
                            if not link_elem:
                                continue
                            
                            title = link_elem.get_text(strip=True)
                            relative_url = link_elem.get('href', '')
                            
                            if relative_url.startswith('/topic?id='):
                                full_url = f"https://news.hada.io{relative_url}"
                            elif relative_url.startswith('topic?id='):
                                full_url = f"https://news.hada.io/{relative_url}"
                            elif relative_url.startswith('https://news.hada.io/topic?id='):
                                full_url = relative_url
                            else:
                                continue
                                
                            crawl_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            print(f"\n[추가-{idx}] {title[:50]}...")
                            print(f"    URL: {full_url}")
                            print(f"    크롤링 시간: {crawl_time}")
                            
                            if save_url_to_ready(full_url):
                                additional_saved += 1
                                print(f"    결과: 새 URL 저장 완료")
                            else:
                                print(f"    결과: 이미 존재하는 URL (건너뜀)")
                            time.sleep(3)
                        
                        print(f"\n추가 저장: {additional_saved}개")
                else:
                    print("'토픽 더 불러오기' 버튼을 찾을 수 없음")
                    
            except Exception as e:
                print(f"더보기 클릭 오류: {e}")
                
        except Exception as e:
            print(f"최신글 수집 오류: {e}")
            import traceback
            traceback.print_exc()
            
        finally:
            try:
                browser.close()
            except:
                pass  # 브라우저가 이미 닫혀있어도 무시

def process_ready_urls():
    """gn_scrap_ready에서 처리 대기중인 URL들을 수집하여 gn_master에 저장합니다."""
    
    print("\n2단계: 콘텐츠 수집 시작...")
    
    try:
        # 처리 대기중인 URL들 조회 (status = '0')
        cur.execute("SELECT seq_no, source_url FROM gn_scrap_ready WHERE status = '0' ORDER BY seq_no LIMIT 10")
        ready_items = cur.fetchall()
        
        if not ready_items:
            print("처리할 URL이 없습니다.")
            return
            
        print(f"처리할 URL 개수: {len(ready_items)}개")
        
        with sync_playwright() as p:
            browser = p.firefox.launch(headless=True)
            page = browser.new_page()
            
            processed_count = 0
            for ready_seq_no, url in ready_items:
                try:
                    print(f"\n처리 중: {url}")
                    
                    # 상태를 '수집중'으로 변경
                    cur.execute("UPDATE gn_scrap_ready SET status = '2', update_dt = %s WHERE seq_no = %s",
                               (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), ready_seq_no))
                    conn.commit()
                    
                    # 페이지 로드
                    page.goto(url, timeout=30000)
                    time.sleep(3)  # 로봇 배제 표준
                    
                    # 페이지 내용 가져오기
                    content = page.content()
                    soup = BeautifulSoup(content, 'html.parser')
                    
                    # topic_id 추출 (URL에서)
                    import re
                    topic_match = re.search(r'topic\?id=(\d+)', url)
                    topic_id = topic_match.group(1) if topic_match else None
                    
                    # 콘텐츠 추출
                    news_title = ""
                    news_desc = ""
                    topic_info = ""
                    external_url = ""
                    topic_url_domain = ""
                    author = ""
                    vote_count = 0
                    comment_count = 0
                    
                    # 실제 제목 추출 - 여러 방법 시도
                    news_title = ""
                    
                    # 방법 1: #tr1 > h1
                    main_title_elem = soup.select_one('#tr1 > h1')
                    if main_title_elem:
                        news_title = main_title_elem.get_text(strip=True)
                    
                    # 방법 2: #tr1 전체
                    if not news_title:
                        tr1_elem = soup.select_one('#tr1')
                        if tr1_elem:
                            news_title = tr1_elem.get_text(strip=True)
                    
                    # 방법 3: h1 태그 전체 검색
                    if not news_title:
                        h1_elem = soup.find('h1')
                        if h1_elem:
                            news_title = h1_elem.get_text(strip=True)
                    
                    # 방법 4: title 태그
                    if not news_title:
                        title_elem = soup.find('title')
                        if title_elem:
                            title_text = title_elem.get_text(strip=True)
                            if " - GeekNews" in title_text:
                                news_title = title_text.replace(" - GeekNews", "")
                            else:
                                news_title = title_text
                    
                    # 제목이 여전히 없으면 URL에서 topic_id 사용
                    if not news_title:
                        news_title = f"Topic {topic_id}"
                    
                    # 외부 링크 정보 (div.topictitle)
                    title_div = soup.find('div', class_='topictitle')
                    if title_div:
                        # 외부 링크
                        external_links = title_div.find_all('a')
                        for link in external_links:
                            href = link.get('href', '')
                            if href.startswith('http') or (not href.startswith('topic?id=') and not href.startswith('/topic?id=')):
                                external_url = href
                                break
                        
                        # 도메인 정보 (span 태그)
                        topicurl_span = title_div.find('span')
                        if topicurl_span:
                            topic_url_domain = topicurl_span.get_text(strip=True)
                    
                    # 설명 (div.topicdesc)
                    desc_div = soup.find('div', class_='topicdesc')
                    if desc_div:
                        # topicdesc 내의 GeekNews 내부 링크 찾기 (class="c99 breakall")
                        desc_links = desc_div.find_all('a', class_='c99')
                        if desc_links:
                            news_desc = desc_links[0].get_text(strip=True)
                        else:
                            news_desc = desc_div.get_text(strip=True)
                    
                    # 토픽 정보 (div.topicinfo)
                    info_div = soup.find('div', class_='topicinfo')
                    if info_div:
                        # 전체 topicinfo 텍스트
                        topic_info = info_div.get_text(strip=True)
                        
                        # 작성자 추출 (/user?id= 패턴)
                        author_links = info_div.find_all('a')
                        for link in author_links:
                            if '/user?id=' in link.get('href', ''):
                                author = link.get_text(strip=True)
                                break
                        
                        # 시간 정보 추출 ("3시간전" 같은 텍스트)
                        import re
                        time_pattern = r'(\d+(?:분|시간|일)전)'
                        time_match = re.search(time_pattern, topic_info)
                        if time_match:
                            news_pub_date = time_match.group(1)
                        else:
                            news_pub_date = ""
                    
                    # 투표 수 (div.votenum)
                    vote_div = soup.find('div', class_='votenum')
                    if vote_div:
                        try:
                            vote_count = int(vote_div.get_text(strip=True))
                        except:
                            vote_count = 0
                    else:
                        vote_count = 0
                    
                    print(f"    제목: {news_title[:50]}...")
                    print(f"    설명: {news_desc[:50]}...")
                    print(f"    작성자: {author}")
                    print(f"    투표수: {vote_count}")
                    if topic_url_domain:
                        print(f"    도메인: {topic_url_domain}")
                    if news_pub_date:
                        print(f"    작성시간: {news_pub_date}")
                    
                    # gn_master에 저장 (topic_info 필드에 도메인 정보도 포함)
                    full_topic_info = topic_info
                    if topic_url_domain:
                        full_topic_info += f" | 도메인: {topic_url_domain}"
                    
                    crawl_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    cur.execute("""
                        INSERT INTO gn_master 
                        (ready_seq_no, topic_id, news_url, external_url, news_title, news_desc, 
                         topic_info, author, vote_count, comment_count, news_pub_date, crawl_dt)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                        news_title = VALUES(news_title),
                        news_desc = VALUES(news_desc),
                        topic_info = VALUES(topic_info),
                        author = VALUES(author),
                        vote_count = VALUES(vote_count),
                        news_pub_date = VALUES(news_pub_date),
                        crawl_dt = VALUES(crawl_dt)
                    """, (ready_seq_no, topic_id, url, external_url, news_title, news_desc,
                          full_topic_info, author, vote_count, comment_count, news_pub_date, crawl_dt))
                    
                    # 상태를 '수집완료'로 변경
                    cur.execute("UPDATE gn_scrap_ready SET status = '9', update_dt = %s WHERE seq_no = %s",
                               (crawl_dt, ready_seq_no))
                    conn.commit()
                    
                    processed_count += 1
                    print(f"    저장 완료!")
                    
                except Exception as e:
                    print(f"    오류: {e}")
                    # 상태를 '실패'로 변경
                    cur.execute("UPDATE gn_scrap_ready SET status = '5', error_msg = %s, update_dt = %s WHERE seq_no = %s",
                               (str(e)[:500], datetime.now().strftime('%Y-%m-%d %H:%M:%S'), ready_seq_no))
                    conn.commit()
                    
                time.sleep(3)  # 로봇 배제 표준
            
            print(f"\n콘텐츠 수집 완료: {processed_count}개 처리")
            browser.close()
            
    except Exception as e:
        print(f"콘텐츠 수집 오류: {e}")
        import traceback
        traceback.print_exc()

def scrape_comments():
    """댓글 페이지에서 아이템 URL들을 수집합니다."""
    
    print("\n댓글 페이지 수집 시작...")
    
    with sync_playwright() as p:
        # 브라우저 시작
        browser = p.firefox.launch(headless=True)
        page = browser.new_page()
        
        try:
            # 댓글 페이지로 이동
            print("페이지 로드 중: https://news.hada.io/comments")
            page.goto("https://news.hada.io/comments", timeout=30000)
            time.sleep(3)  # 로봇 배제 표준 준수
            
            # 페이지 내용 가져오기
            content = page.content()
            soup = BeautifulSoup(content, 'html.parser')
            
            # 댓글 찾기 (id가 cid로 시작하는 div)
            comments = soup.find_all('div', id=lambda x: x and x.startswith('cid'))
            print(f"찾은 댓글 수: {len(comments)}개")
            
            saved_count = 0
            unique_urls = set()  # 중복 제거용
            
            for idx, comment in enumerate(comments, 1):
                # 댓글 정보 영역 찾기
                comment_info = comment.find('div', class_='commentinfo')
                if not comment_info:
                    continue
                
                # 댓글이 달린 아이템의 링크 찾기
                # commentinfo 안의 모든 링크를 확인
                links = comment_info.find_all('a')
                item_url = None
                
                for link in links:
                    href = link.get('href', '')
                    # topic?id= 패턴을 찾음
                    if '/topic?id=' in href:
                        if href.startswith('/'):
                            item_url = f"https://news.hada.io{href}"
                        else:
                            item_url = href
                        break
                
                if not item_url:
                    continue
                
                # 댓글 시간 정보 (보통 두 번째 링크)
                time_elem = links[1] if len(links) > 1 else None
                comment_time = time_elem.get_text(strip=True) if time_elem else ""
                
                # 크롤링 시간
                crawl_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                # 중복 제거
                if item_url not in unique_urls and '/topic?id=' in item_url:
                    unique_urls.add(item_url)
                    
                    # 댓글 내용 일부 추출
                    comment_text = comment.find('div', class_='comment')
                    comment_preview = comment_text.get_text(strip=True)[:50] + "..." if comment_text else ""
                    
                    print(f"\n[댓글-{idx}] {item_url}")
                    print(f"    댓글 시간: {comment_time}")
                    print(f"    댓글 내용: {comment_preview}")
                    print(f"    크롤링 시간: {crawl_time}")
                    
                    # URL 저장
                    if save_url_to_ready(item_url):
                        saved_count += 1
                    
                    time.sleep(3)  # 로봇 배제 표준
            
            print(f"\n댓글 아이템 저장 결과: {saved_count}개 신규 저장")
            
        except Exception as e:
            print(f"댓글 수집 오류: {e}")
            import traceback
            traceback.print_exc()
            
        finally:
            try:
                browser.close()
            except:
                pass  # 브라우저가 이미 닫혀있어도 무시

def main():
    """메인 실행 함수"""
    
    print(f"\n{'='*60}")
    print(f"GeekNews 크롤링 시작: {datetime.now()}")
    print(f"{'='*60}")
    
    try:
        # 1. 최신글 수집
        scrape_latest_items()
        
        # 2. 댓글 페이지 수집
        scrape_comments()
        
        # 3. 콘텐츠 수집 (2단계)
        process_ready_urls()
        
        print(f"\n{'='*60}")
        print(f"크롤링 완료: {datetime.now()}")
        print(f"{'='*60}\n")
        
    except KeyboardInterrupt:
        print("\n\n사용자에 의해 중단됨")
        
    except Exception as e:
        print(f"\n오류 발생: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        # 데이터베이스 연결 종료
        cur.close()
        conn.close()
        print("데이터베이스 연결 종료")

if __name__ == "__main__":
    main()