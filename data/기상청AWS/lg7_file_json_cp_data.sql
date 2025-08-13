CREATE TABLE tb_cp (
    seq_no BIGINT(20) NOT NULL AUTO_INCREMENT,
    document_id LONGTEXT NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',
    contents_title LONGTEXT NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',
    sentence_id LONGTEXT NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',
    sentence_title LONGTEXT NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',
    sentence_text LONGTEXT NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',
    json_data LONGTEXT NULL DEFAULT NULL COLLATE 'utf8mb4_bin',
    create_dt CHAR(50) NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',
    PRIMARY KEY (seq_no) USING BTREE,
    CONSTRAINT json_data CHECK (json_valid(json_data))
)
COLLATE='utf8mb4_general_ci'
ENGINE=InnoDB
AUTO_INCREMENT=278575
;