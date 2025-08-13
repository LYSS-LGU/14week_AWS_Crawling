-- C:\data\기상청AWS\create_fact_smb_table.sql
-- 소상공인 상가정보 FACT 테이블 생성 및 ODS에서 데이터 이관

-- FACT 테이블 생성
CREATE TABLE `fact_smb_store` (
    `seq_no` INT AUTO_INCREMENT COMMENT '고유번호',
    `smb_id` CHAR(20) NOT NULL COMMENT '상가업소번호',
    `smb_name` TEXT COMMENT '상호명',
    `smb_subnm` VARCHAR(30) COMMENT '지점명',
    `cate1_cd` CHAR(2) COMMENT '상권업종대분류코드',
    `cate1_nm` VARCHAR(255) COMMENT '상권업종대분류명',
    `cate2_cd` CHAR(4) COMMENT '상권업종중분류코드',
    `cate2_nm` VARCHAR(255) COMMENT '상권업종중분류명',
    `cate3_cd` CHAR(6) COMMENT '상권업종소분류코드',
    `cate3_nm` VARCHAR(255) COMMENT '상권업종소분류명',
    `std_cd` CHAR(6) COMMENT '표준산업분류코드',
    `std_nm` VARCHAR(255) COMMENT '표준산업분류명',
    `addr1` VARCHAR(255) COMMENT '시도명',
    `addr2` VARCHAR(255) COMMENT '시군구명',
    `addr3` VARCHAR(255) COMMENT '행정동명',
    `addr4` VARCHAR(255) COMMENT '법정동명',
    `addr_num1` DECIMAL(4) COMMENT '지번본번지',
    `addr_num2` DECIMAL(4) COMMENT '지번부번지',
    `addr_bld` VARCHAR(255) COMMENT '건물명',
    `floor_nm` VARCHAR(10) COMMENT '층 위치',
    `lon` DECIMAL(16,12) COMMENT '경도',
    `lat` DECIMAL(16,12) COMMENT '위도',
    `create_dt` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '생성일시',
    `update_dt` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '수정일시',
    
    PRIMARY KEY (`seq_no`),
    UNIQUE KEY `uk_smb_id` (`smb_id`),
    INDEX `idx_category` (`cate1_cd`, `cate2_cd`, `cate3_cd`),
    INDEX `idx_address` (`addr1`, `addr2`, `addr3`),
    INDEX `idx_coordinates` (`lon`, `lat`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='소상공인 상가정보 FACT 테이블';

-- ODS에서 FACT 테이블로 데이터 이관 (매핑 적용)
INSERT INTO fact_smb_store (
    smb_id, smb_name, smb_subnm,
    cate1_cd, cate1_nm, cate2_cd, cate2_nm, cate3_cd, cate3_nm,
    std_cd, std_nm,
    addr1, addr2, addr3, addr4,
    addr_num1, addr_num2, addr_bld, floor_nm,
    lon, lat
)
SELECT 
    col1 AS smb_id,           -- 상가업소번호
    col2 AS smb_name,         -- 상호명
    col3 AS smb_subnm,        -- 지점명
    col4 AS cate1_cd,         -- 상권업종대분류코드
    col5 AS cate1_nm,         -- 상권업종대분류명
    col6 AS cate2_cd,         -- 상권업종중분류코드
    col7 AS cate2_nm,         -- 상권업종중분류명
    col8 AS cate3_cd,         -- 상권업종소분류코드
    col9 AS cate3_nm,         -- 상권업종소분류명
    col10 AS std_cd,          -- 표준산업분류코드
    col11 AS std_nm,          -- 표준산업분류명
    col13 AS addr1,           -- 시도명
    col15 AS addr2,           -- 시군구명
    col17 AS addr3,           -- 행정동명
    col19 AS addr4,           -- 법정동명
    CASE 
        WHEN col23 = '' OR col23 IS NULL THEN 0 
        ELSE CAST(col23 AS DECIMAL(4)) 
    END AS addr_num1,         -- 지번본번지
    CASE 
        WHEN col24 = '' OR col24 IS NULL THEN 0 
        ELSE CAST(col24 AS DECIMAL(4)) 
    END AS addr_num2,         -- 지번부번지
    col26 AS addr_bld,        -- 건물명
    col36 AS floor_nm,        -- 층 위치
    CASE 
        WHEN col38 = '' OR col38 IS NULL THEN 0 
        ELSE CAST(col38 AS DECIMAL(16,12)) 
    END AS lon,               -- 경도
    CASE 
        WHEN col39 = '' OR col39 IS NULL THEN 0 
        ELSE CAST(col39 AS DECIMAL(16,12)) 
    END AS lat                -- 위도
FROM tb_smb_ods
WHERE col1 IS NOT NULL AND col1 != ''  -- 상가업소번호가 있는 데이터만
ORDER BY col1;

-- 결과 확인
SELECT 
    '데이터 이관 완료' AS status,
    COUNT(*) AS total_count,
    COUNT(DISTINCT smb_id) AS unique_store_count
FROM fact_smb_store;

-- 샘플 데이터 확인
SELECT 
    smb_id, smb_name, cate1_nm, cate2_nm, addr1, addr2, addr3
FROM fact_smb_store 
LIMIT 10;