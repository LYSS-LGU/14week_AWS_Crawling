-- ================================================================
-- ODS에서 FACT 테이블로 데이터 이관
-- tb_smb_ods → fact_smb_master 테이블 데이터 INSERT
-- ================================================================

-- 기존 데이터 확인 (선택사항)
SELECT 'ODS 테이블 데이터 건수' AS status, COUNT(*) AS cnt FROM tb_smb_ods;

-- 데이터 이관 시작
INSERT INTO fact_smb_master 
(smb_id, smb_name, smb_subnm, cate1_cd, cate1_nm, cate2_cd, cate2_nm, cate3_cd, cate3_nm, std_cd, std_nm, addr1, addr2, addr3, addr4, addr_num1, addr_num2, addr_bld, floor_nm, lon, lat)
SELECT 
    col1,                                    -- 상가업소번호
    col2,                                    -- 상호명  
    col3,                                    -- 지점명
    col4,                                    -- 상권업종대분류코드
    col5,                                    -- 상권업종대분류명
    col6,                                    -- 상권업종중분류코드
    col7,                                    -- 상권업종중분류명
    col8,                                    -- 상권업종소분류코드
    col9,                                    -- 상권업종소분류명
    col10,                                   -- 표준산업분류코드
    col11,                                   -- 표준산업분류명
    col13,                                   -- 시도명 (col12 건너뜀)
    col15,                                   -- 시군구명 (col14 건너뜀)
    col17,                                   -- 행정동명 (col16 건너뜀)
    col19,                                   -- 법정동명 (col18 건너뜀)
    CASE 
        WHEN col23 = '' OR col23 IS NULL THEN 0 
        ELSE CONVERT(col23, DECIMAL(4)) 
    END,                                     -- 지번본번지 (안전한 변환)
    CASE 
        WHEN col24 = '' OR col24 IS NULL THEN 0 
        ELSE CONVERT(col24, DECIMAL(4)) 
    END,                                     -- 지번부번지 (안전한 변환)
    col26,                                   -- 건물명 (col25 건너뜀)
    col36,                                   -- 층 위치 (col27~35 건너뜀)
    CASE 
        WHEN col38 = '' OR col38 IS NULL THEN 0.000000000000 
        ELSE CONVERT(col38, DECIMAL(16,12)) 
    END,                                     -- 경도 (안전한 변환)
    CASE 
        WHEN col39 = '' OR col39 IS NULL THEN 0.000000000000 
        ELSE CONVERT(col39, DECIMAL(16,12)) 
    END                                      -- 위도 (안전한 변환)
FROM tb_smb_ods
WHERE col1 IS NOT NULL AND col1 != '' AND TRIM(col1) != ''  -- 유효한 상가업소번호만
ORDER BY col1;

-- ================================================================
-- 결과 확인 쿼리들
-- ================================================================

-- 1. 전체 건수 확인
SELECT 'FACT 테이블 총 데이터 건수' AS status, COUNT(*) AS total_count FROM fact_smb_master;

-- 2. 상가업소번호 중복 확인
SELECT 'FACT 테이블 중복 상가업소번호' AS status, COUNT(DISTINCT smb_id) AS unique_count FROM fact_smb_master;

-- 3. 시도별 분포 확인
SELECT addr1 AS 시도명, COUNT(*) AS 건수 
FROM fact_smb_master 
WHERE addr1 IS NOT NULL AND addr1 != ''
GROUP BY addr1 
ORDER BY COUNT(*) DESC;

-- 4. 샘플 데이터 확인 (상위 5건)
SELECT 
    smb_id, smb_name, cate1_nm, cate2_nm, addr1, addr2, addr3, lon, lat
FROM fact_smb_master 
ORDER BY smb_id 
LIMIT 5;