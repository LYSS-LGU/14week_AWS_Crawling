-- ================================================================
-- MART 테이블 생성: 행정동별 최다 상권소분류 랭킹
-- fact_smb_master → mart_addr3_top_category
-- ================================================================

-- MART 테이블 생성
CREATE TABLE mart_addr3_top_category (
    seq_no INT(11) NOT NULL AUTO_INCREMENT COMMENT '고유번호',
    addr1 VARCHAR(255) NOT NULL COMMENT '시도명' COLLATE 'utf8mb4_general_ci',
    addr2 VARCHAR(255) NOT NULL COMMENT '시군구명' COLLATE 'utf8mb4_general_ci', 
    addr3 VARCHAR(255) NOT NULL COMMENT '행정동명' COLLATE 'utf8mb4_general_ci',
    cate3_cd CHAR(6) NOT NULL COMMENT '상권업종소분류코드' COLLATE 'utf8mb4_general_ci',
    cate3_nm VARCHAR(255) NOT NULL COMMENT '상권업종소분류명' COLLATE 'utf8mb4_general_ci',
    store_count INT(11) NOT NULL COMMENT '해당 업종 점포수',
    total_stores INT(11) NOT NULL COMMENT '해당 동 전체 점포수',
    market_share DECIMAL(5,2) NOT NULL COMMENT '시장점유율(%)',
    create_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '생성일시',
    
    PRIMARY KEY (seq_no) USING BTREE,
    UNIQUE KEY uk_addr3 (addr1, addr2, addr3),
    INDEX idx_addr (addr1, addr2, addr3),
    INDEX idx_category (cate3_cd, cate3_nm),
    INDEX idx_store_count (store_count DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci 
COMMENT='행정동별 최다 상권소분류 MART 테이블';

-- MART 데이터 생성 및 삽입
INSERT INTO mart_addr3_top_category 
(addr1, addr2, addr3, cate3_cd, cate3_nm, store_count, total_stores, market_share)
WITH category_count AS (
    -- 1단계: 행정동별 + 소분류별 점포수 집계
    SELECT 
        addr1,
        addr2, 
        addr3,
        cate3_cd,
        cate3_nm,
        COUNT(*) as store_count
    FROM fact_smb_master
    WHERE addr1 IS NOT NULL AND addr1 != ''
      AND addr2 IS NOT NULL AND addr2 != ''
      AND addr3 IS NOT NULL AND addr3 != ''
      AND cate3_cd IS NOT NULL AND cate3_cd != ''
      AND cate3_nm IS NOT NULL AND cate3_nm != ''
    GROUP BY addr1, addr2, addr3, cate3_cd, cate3_nm
),
addr3_total AS (
    -- 2단계: 행정동별 전체 점포수 집계
    SELECT 
        addr1,
        addr2,
        addr3,
        COUNT(*) as total_stores
    FROM fact_smb_master
    WHERE addr1 IS NOT NULL AND addr1 != ''
      AND addr2 IS NOT NULL AND addr2 != ''
      AND addr3 IS NOT NULL AND addr3 != ''
    GROUP BY addr1, addr2, addr3
),
ranked_category AS (
    -- 3단계: 행정동별 소분류 랭킹 (점포수 기준)
    SELECT 
        c.addr1,
        c.addr2,
        c.addr3,
        c.cate3_cd,
        c.cate3_nm,
        c.store_count,
        t.total_stores,
        ROUND((c.store_count * 100.0 / t.total_stores), 2) as market_share,
        ROW_NUMBER() OVER (
            PARTITION BY c.addr1, c.addr2, c.addr3 
            ORDER BY c.store_count DESC, c.cate3_cd
        ) as rn
    FROM category_count c
    JOIN addr3_total t ON c.addr1 = t.addr1 
                       AND c.addr2 = t.addr2 
                       AND c.addr3 = t.addr3
)
-- 4단계: 랭킹 1위만 선택
SELECT 
    addr1,
    addr2, 
    addr3,
    cate3_cd,
    cate3_nm,
    store_count,
    total_stores,
    market_share
FROM ranked_category
WHERE rn = 1  -- 각 행정동별 1위만
ORDER BY addr1, addr2, addr3;

-- ================================================================
-- 결과 확인 쿼리들
-- ================================================================

-- 1. MART 테이블 총 건수 확인
SELECT 'MART 테이블 총 건수' AS status, COUNT(*) AS total_count FROM mart_addr3_top_category;

-- 2. 시도별 행정동 분포
SELECT addr1 AS 시도명, COUNT(*) AS 행정동수 
FROM mart_addr3_top_category 
GROUP BY addr1 
ORDER BY COUNT(*) DESC;

-- 3. 최다 업종 TOP 10
SELECT cate3_nm AS 업종명, COUNT(*) AS 행정동수, AVG(market_share) AS 평균점유율
FROM mart_addr3_top_category 
GROUP BY cate3_nm 
ORDER BY COUNT(*) DESC 
LIMIT 10;

-- 4. 점포수가 가장 많은 행정동 TOP 10
SELECT addr1, addr2, addr3, cate3_nm, store_count, total_stores, market_share
FROM mart_addr3_top_category 
ORDER BY total_stores DESC 
LIMIT 10;

-- 5. 샘플 데이터 확인
SELECT addr1, addr2, addr3, cate3_nm, store_count, total_stores, market_share
FROM mart_addr3_top_category 
ORDER BY addr1, addr2, addr3
LIMIT 20;