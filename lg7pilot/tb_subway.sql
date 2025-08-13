-- subway_import_final_fix.sql

-- ======================================================
-- 1단계: 테이블을 깨끗하게 준비합니다.
-- ======================================================

-- 만약 'tb_subway' 테이블이 이미 존재한다면, 먼저 삭제합니다.
DROP TABLE IF EXISTS tb_subway;

-- 그리고 나서 새로 개선된 테이블을 생성합니다.
-- (참고: CSV 파일에 4개의 컬럼만 있으므로, 테이블도 4개에 맞게 수정했습니다.)
CREATE TABLE tb_subway (
    seq_no BIGINT NOT NULL AUTO_INCREMENT,
    t_type VARCHAR(50) NULL, -- '노선명'
    t_date DATE NULL,        -- '년월'
    t_count INT NULL DEFAULT 0,  -- '승차총승객수'
    PRIMARY KEY (seq_no)
) COLLATE='utf8mb4_general_ci' ENGINE=InnoDB;

-- ======================================================
-- 2단계: CSV 데이터를 올바른 설정으로 가져옵니다.
-- ======================================================

LOAD DATA LOCAL INFILE 'C:\\githome\\14week_AWS\\lg7pilot\\import_data.csv' -- ⚠️ 실제 파일 위치로 수정!
REPLACE
INTO TABLE `cp_data`.`tb_subway`
CHARACTER SET utf8mb4
-- 🚨 핵심: 데이터 구분자를 세미콜론(;)이 아닌 쉼표(,)로 명확하게 지정합니다.
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
ESCAPED BY '"'
LINES TERMINATED BY '\r\n'
-- CSV 파일의 첫 번째 줄(제목 줄)은 건너뜁니다.
IGNORE 1 LINES 
-- CSV의 컬럼을 순서대로 읽되, 불필요한 첫번째 컬럼은 건너뛰고
-- 날짜는 임시 변수(@t_date_str)에 잠시 저장합니다.
(@dummy, t_type, @t_date_str, t_count) 
-- SET 구문을 이용해 임시 변수에 담아둔 날짜 문자열('YYYYMM' 형식)을
-- STR_TO_DATE 함수로 정식 DATE 타입('YYYY-MM-DD' 형식)으로 변환하여 t_date 컬럼에 넣습니다.
SET t_date = STR_TO_DATE(@t_date_str, '%Y%m');





