CREATE TABLE mart_weather_aws_hour (
    yyyymmddhh CHAR(10) NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',
    stn_id CHAR(4) NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',
    stn_ko VARCHAR(50) NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',
    ta_min DECIMAL(6,2) NULL DEFAULT NULL,
    ta_max DECIMAL(6,2) NULL DEFAULT NULL
)
COLLATE='utf8mb4_general_ci'
ENGINE=InnoDB
;