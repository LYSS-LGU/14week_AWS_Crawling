-- 과제 해결을 위한 전체 SQL 쿼리 모음

-- =================================================================
-- 과제 1: 지하철 1호선의 2018년도 전체 이용자 수 구하기
-- =================================================================
-- WHERE 절을 이용해 '1호선'과 '2018년' 데이터만 필터링한 후,
-- 승차총승객수(t_count)의 합계를 구합니다.
-- FORMAT 함수는 숫자를 1,000 단위 쉼표가 있는 문자열로 만들어줍니다.
SELECT
    FORMAT(SUM(t_count), 0) AS '1호선_2018년_총_이용자수'
FROM
    tb_subway
WHERE
    t_type = '1호선' AND YEAR(t_date) = 2018;


-- =================================================================
-- 과제 2: 2018년도 각 호선별 가장 이용자가 많은 월 출력하기
-- =================================================================
-- WITH 구문을 사용해 복잡한 쿼리를 단계별로 작성합니다.
WITH monthly_summary AS (
    -- 1단계: 호선별, 월별로 이용자 수의 합계를 구합니다.
    SELECT
        t_type,
        MONTH(t_date) AS month,
        SUM(t_count) AS total_users
    FROM
        tb_subway
    WHERE
        YEAR(t_date) = 2018 -- 2018년 데이터만 사용
    GROUP BY
        t_type, MONTH(t_date)
),
ranked_summary AS (
    -- 2단계: 1단계에서 구한 결과를 바탕으로, 호선별로 이용자 수가 많은 순서(DESC)대로 순위를 매깁니다.
    SELECT
        t_type,
        month,
        total_users,
        RANK() OVER (PARTITION BY t_type ORDER BY total_users DESC) as rnk
    FROM
        monthly_summary
)
-- 3단계: 순위가 1등인(가장 이용자가 많은) 월만 선택하여 최종 결과를 출력합니다.
SELECT 
    t_type AS '호선', 
    month AS '가장_이용객이_많은_월',
    FORMAT(total_users, 0) AS '월_이용자수'
FROM
    ranked_summary
WHERE
    rnk = 1
ORDER BY
    t_type;


-- =================================================================
-- 과제 3: 2018년도 각 호선별 가장 이용자가 적은 월 출력하기
-- =================================================================
-- 2번 문제와 거의 동일하며, 순서를 '적은' 순서(ASC)로 바꾸기만 하면 됩니다.
WITH monthly_summary AS (
    SELECT
        t_type,
        MONTH(t_date) AS month,
        SUM(t_count) AS total_users
    FROM
        tb_subway
    WHERE
        YEAR(t_date) = 2018
    GROUP BY
        t_type, MONTH(t_date)
),
ranked_summary AS (
    -- 이용자 수가 '적은' 순서(ASC)대로 순위를 매깁니다.
    SELECT
        t_type,
        month,
        total_users,
        RANK() OVER (PARTITION BY t_type ORDER BY total_users ASC) as rnk
    FROM
        monthly_summary
)
SELECT 
    t_type AS '호선', 
    month AS '가장_이용객이_적은_월',
    FORMAT(total_users, 0) AS '월_이용자수'
FROM
    ranked_summary
WHERE
    rnk = 1
ORDER BY
    t_type;
