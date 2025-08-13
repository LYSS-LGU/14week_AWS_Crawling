INSERT INTO mart_weather_aws_hour
(yyyymmddhh, stn_id, stn_ko, ta_min, ta_max)
SELECT SUBSTRING(yyyymmddhhmi, 1, 10) AS yyyymmddhh
, stn AS stn_id
, (SELECT STN_KO FROM tb_weather_tcn t WHERE t.STN_ID = stn) AS stn_ko
, MIN(ta) AS ta_min
, MAX(ta) AS ta_max
FROM fact_weather_aws1
WHERE ta > -90.0
GROUP BY yyyymmddhh, stn_id