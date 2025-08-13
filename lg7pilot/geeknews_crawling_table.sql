-- 데이터베이스 사용
  USE cp_data;

  -- 기존 뷰 삭제 (먼저 뷰부터)
  DROP VIEW IF EXISTS v_gn_recent;
  DROP VIEW IF EXISTS v_gn_status;

  -- 기존 테이블 삭제 (순서 중요: 외래키 관계 때문에 역순으로 삭제)
  DROP TABLE IF EXISTS gn_master;
  DROP TABLE IF EXISTS gn_scrap_ready;

  -- gn_scrap_ready 테이블 생성 (URL 수집 대기열)
  CREATE TABLE gn_scrap_ready (
      seq_no BIGINT(20) NOT NULL AUTO_INCREMENT COMMENT '순번',
      source_type CHAR(1) NOT NULL DEFAULT '1' COMMENT '소스 타입 (1:GeekNews)' COLLATE
  'utf8mb4_general_ci',
      source_url TINYTEXT NOT NULL COMMENT '수집할 URL' COLLATE 'utf8mb4_general_ci',
      status CHAR(1) NOT NULL DEFAULT '0' COMMENT '상태 (0:처리전, 1:수집처리대기, 2:수집중, 5:실패,        
  6:패키지취소, 7:저품질취소, 8:중복취소, 9:수집완료)' COLLATE 'utf8mb4_general_ci',
      retry_count INT NOT NULL DEFAULT 0 COMMENT '재시도 횟수',
      error_msg TEXT NULL DEFAULT NULL COMMENT '오류 메시지' COLLATE 'utf8mb4_general_ci',
      create_dt CHAR(19) NULL DEFAULT NULL COMMENT '생성일시' COLLATE 'utf8mb4_general_ci',
      update_dt CHAR(19) NULL DEFAULT NULL COMMENT '수정일시' COLLATE 'utf8mb4_general_ci',
      PRIMARY KEY (seq_no) USING BTREE,
      INDEX idx_source_type_status (source_type, status) USING BTREE,
      INDEX idx_source_url (source_url(255)) USING BTREE,
      INDEX idx_status_create (status, create_dt) USING BTREE
  )
  COMMENT='GeekNews 크롤링 URL 대기열'
  COLLATE='utf8mb4_general_ci'
  ENGINE=InnoDB
  AUTO_INCREMENT=1;

  -- gn_master 테이블 생성 (수집된 콘텐츠 저장)
  CREATE TABLE gn_master (
      seq_no BIGINT(20) NOT NULL AUTO_INCREMENT COMMENT '순번',
      ready_seq_no BIGINT(20) NULL DEFAULT NULL COMMENT 'gn_scrap_ready 참조번호',
      topic_id VARCHAR(20) NULL DEFAULT NULL COMMENT 'GeekNews 토픽 ID' COLLATE 'utf8mb4_general_ci',       
      news_url TINYTEXT NULL DEFAULT NULL COMMENT '원본 GeekNews URL' COLLATE 'utf8mb4_general_ci',
      external_url TEXT NULL DEFAULT NULL COMMENT '외부 링크 URL' COLLATE 'utf8mb4_general_ci',
      news_title TEXT NULL DEFAULT NULL COMMENT '제목' COLLATE 'utf8mb4_general_ci',
      news_desc TEXT NULL DEFAULT NULL COMMENT '설명' COLLATE 'utf8mb4_general_ci',
      topic_info TEXT NULL DEFAULT NULL COMMENT '토픽 정보' COLLATE 'utf8mb4_general_ci',
      author VARCHAR(100) NULL DEFAULT NULL COMMENT '작성자' COLLATE 'utf8mb4_general_ci',
      vote_count INT NULL DEFAULT 0 COMMENT '추천 수',
      comment_count INT NULL DEFAULT 0 COMMENT '댓글 수',
      view_count INT NULL DEFAULT 0 COMMENT '조회 수',
      news_comments LONGTEXT NULL DEFAULT NULL COMMENT '댓글 내용' COLLATE 'utf8mb4_general_ci',
      full_contents LONGTEXT NULL DEFAULT NULL COMMENT '전체 콘텐츠' COLLATE 'utf8mb4_general_ci',
      news_pub_date CHAR(30) NULL DEFAULT NULL COMMENT '게시일시' COLLATE 'utf8mb4_general_ci',
      news_update CHAR(19) NULL DEFAULT NULL COMMENT '마지막 업데이트' COLLATE 'utf8mb4_general_ci',        
      crawl_dt CHAR(19) NULL DEFAULT NULL COMMENT '크롤링 수행일시' COLLATE 'utf8mb4_general_ci',
      PRIMARY KEY (seq_no) USING BTREE,
      UNIQUE KEY uk_topic_id (topic_id) USING BTREE,
      INDEX idx_news_url (news_url(255)) USING BTREE,
      INDEX idx_ready_seq_no (ready_seq_no) USING BTREE,
      INDEX idx_author_pubdate (author, news_pub_date) USING BTREE,
      INDEX idx_crawl_dt (crawl_dt) USING BTREE,
      FOREIGN KEY (ready_seq_no) REFERENCES gn_scrap_ready(seq_no) ON DELETE SET NULL
  )
  COMMENT='GeekNews 수집된 콘텐츠 저장'
  COLLATE='utf8mb4_general_ci'
  ENGINE=InnoDB
  AUTO_INCREMENT=1;

  -- 뷰 생성
  CREATE VIEW v_gn_status AS
  SELECT
      r.status,
      CASE r.status
          WHEN '0' THEN '처리전'
          WHEN '1' THEN '수집처리대기'
          WHEN '2' THEN '수집중'
          WHEN '5' THEN '실패'
          WHEN '9' THEN '수집완료'
          ELSE '기타'
      END as status_name,
      COUNT(*) as count
  FROM gn_scrap_ready r
  GROUP BY r.status
  ORDER BY r.status;

  CREATE VIEW v_gn_recent AS
  SELECT
      m.topic_id,
      m.news_title,
      m.author,
      m.vote_count,
      m.comment_count,
      m.news_pub_date,
      m.crawl_dt,
      r.status as ready_status
  FROM gn_master m
  LEFT JOIN gn_scrap_ready r ON m.ready_seq_no = r.seq_no
  ORDER BY m.crawl_dt DESC
  LIMIT 50;

  SELECT '테이블 생성 완료!' as result;