cp_datacp_data-- =================================================
-- 1. 데이터베이스(저장 공간) 생성
-- =================================================
CREATE DATABASE IF NOT EXISTS cp_data;

-- =================================================
-- 2. 프로젝트 전용 사용자 생성 및 권한 부여
-- =================================================
-- 만약 'lguplus7' 사용자가 이미 있다면 삭제하고 새로 만듭니다.
DROP USER IF EXISTS 'lguplus7'@'localhost';
-- 'lguplus7' 사용자를 생성하고 비밀번호를 설정합니다.
CREATE USER 'lguplus7'@'localhost' IDENTIFIED BY '발급받은_DB_PASSWORD';
-- 'lguplus7' 사용자에게 'cp_data' 데이터베이스의 모든 권한을 부여합니다.
GRANT ALL PRIVILEGES ON cp_data.* TO 'lguplus7'@'localhost';
-- 변경된 권한을 시스템에 즉시 적용합니다.
FLUSH PRIVILEGES;

-- =================================================
-- 3. 생성한 데이터베이스를 사용하겠다고 지정
-- =================================================
USE cp_data;

-- =================================================
-- 4. 뉴스 기사 정보를 저장할 테이블 생성
-- =================================================
CREATE TABLE news_master (
    seq_no BIGINT(20) NOT NULL AUTO_INCREMENT,
    news_title TEXT NULL DEFAULT NULL,
    news_desc TEXT NULL DEFAULT NULL,
    news_category TEXT NULL DEFAULT NULL,
    news_author TEXT NULL DEFAULT NULL,
    publisher TINYTEXT NULL DEFAULT NULL,
    news_pub_date CHAR(30) NULL DEFAULT NULL,
    news_url TINYTEXT NULL DEFAULT NULL,
    news_update CHAR(19) NULL DEFAULT NULL,
    PRIMARY KEY (seq_no) USING BTREE,
    INDEX news_url (news_url(255)) USING BTREE
) COLLATE='utf8mb4_general_ci' ENGINE=InnoDB;

-- =================================================
-- 5. 스크랩할 뉴스 URL을 관리할 테이블 생성
-- =================================================
CREATE TABLE news_scrap_ready (
    seq_no BIGINT(20) NOT NULL AUTO_INCREMENT,
    source_type CHAR(1) NOT NULL DEFAULT '0',
    source_url TINYTEXT NOT NULL,
    status CHAR(1) NOT NULL DEFAULT '0' COMMENT '0:처리전, 1:수집처리대기, 2:수집중, 5:실패, 6: 패키지취소, 7: 저품질취소, 8: 중복취소, 9:수집완료',
    create_dt CHAR(19) NULL DEFAULT NULL,
    update_dt CHAR(19) NULL DEFAULT NULL,
    PRIMARY KEY (secp_dataq_no) USING BTREE,
    INDEX source_type_status (source_type, status) USING BTREE,
    INDEX source_url (source_url(255)) USING BTREE
) COLLATE='utf8mb4_general_ci' ENGINE=InnoDB;