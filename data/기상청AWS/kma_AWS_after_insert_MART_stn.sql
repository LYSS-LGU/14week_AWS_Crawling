CREATE table mart_weather_aws_day
as SELECT SUBSTRING(yyyymmddhhmi, 1, 8) AS yyyymmdd
, stn AS stn_id
, (SELECT STN_KO FROM tb_weather_tcn t WHERE t.STN_ID = stn) AS stn_ko
, MIN(ta) AS ta_min
, MAX(ta) AS ta_max
FROM fact_weather_aws1
WHERE ta > -90.0
GROUP BY yyyymmdd, stn_id