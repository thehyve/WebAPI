CREATE SEQUENCE ${ohdsiSchema}.seq_pnc_stdy START WITH 1 INCREMENT BY 1 MAXVALUE 9223372036854775807 NO CYCLE;

CREATE TABLE ${ohdsiSchema}.panacea_study
(
--    study_id    BIGINT NOT NULL DEFAULT NEXT VALUE FOR ${ohdsiSchema}.seq_pnc_stdy PRIMARY KEY,
    study_id INT PRIMARY KEY,
    study_name  VARCHAR(255),
    study_desc  VARCHAR(255),
    concept_set_def TEXT,
    cohort_definition_id INT,
    study_detail TEXT,
    switch_window INT,
    study_duration INT,
    start_date date,
    end_date date,
    min_unit_days INT,
    min_unit_counts INT,
    gap_threshold NUMERIC(5, 2),
    concept_set_id	INT,
    create_time TIMESTAMP
);

CREATE SEQUENCE ${ohdsiSchema}.seq_pnc_tx_stg_cmb START WITH 1 INCREMENT BY 1 MAXVALUE 9223372036854775807 NO CYCLE;

CREATE TABLE ${ohdsiSchema}.pnc_tx_stage_combination
(
    pnc_tx_stg_cmb_id INT PRIMARY KEY,
    study_id    INT
--    CONSTRAINT fk_pnctxcmb_pncstdy FOREIGN KEY (study_id) REFERENCES ${ohdsiSchema}.panacea_study (study_id)
);

CREATE SEQUENCE ${ohdsiSchema}.seq_pnc_tx_stg_cmb_mp START WITH 1 INCREMENT BY 1 MAXVALUE 9223372036854775807 NO CYCLE;

CREATE TABLE ${ohdsiSchema}.pnc_tx_stage_combination_map
(
    pnc_tx_stg_cmb_mp_id INT PRIMARY KEY,
    pnc_tx_stg_cmb_id INT,
    concept_id BIGINT not null,
    concept_name VARCHAR(255)
);

CREATE TABLE ${ohdsiSchema}.pnc_study_summary
(
    study_id INT  NOT NULL,
--    source_id INT,
    study_results TEXT,
    study_results_2 TEXT,
    study_results_filtered TEXT,
    last_update_time TIMESTAMP
);

--CREATE SEQUENCE ${ohdsiSchema}.seq_pnc_stdy_smry
--START WITH 1
--INCREMENT BY 1
--MAXVALUE 9223372036854775807 NO CYCLE;

CREATE TABLE ${ohdsiSchema}.pnc_study_summary_path
(
    pnc_stdy_smry_id  SERIAL PRIMARY KEY,
    study_id INT,
--    source_id INT,
    tx_path_parent_key BIGINT,
    tx_stg_cmb    VARCHAR(255),
    tx_stg_cmb_pth VARCHAR(4000),
	tx_seq         BIGINT,
    tx_stg_cnt      BIGINT,
    tx_stg_percentage numeric(5,2),
    tx_stg_avg_dr   INT,
    tx_stg_avg_gap  INT,
    tx_avg_frm_strt INT,
    tx_rslt_version INT
);

--CREATE TABLE ${ohdsiSchema}.pnc_tmp_pt_sq_ct
--(
--    pnc_pt_sq_ct_id BIGINT not null primary key,
--    study_id  BIGINT FOREIGN KEY REFERENCES WebAPI.dbo.panacea_study (study_id),
--    source_id INT,
--    person_id BIGINT not null,
--    tx_seq BIGINT,
--    concept_id BIGINT not null,
--    concept_name VARCHAR(255),
--    idx_start_date date not null,
--    idx_end_date date not null,
--    duration_days INT not null
--);

--CREATE TABLE ${ohdsiSchema}.pnc_tmp_pt_stg_sq_ct
--(
--    pnc_pt_stg_sq_id bigint not null primary key,
--    study_id  bigint not null foreign key references WebAPI.dbo.panacea_study (study_id),
--    source_id int,
--    person_id bigint not null,
--    tx_stg_cmb_id bigint not null foreign key references WebAPI.dbo.pnc_tx_stage_combination (pnc_tx_stg_cmb_id),
--    tx_seq bigint,
--    stg_start_date date not null,
--    stg_end_date date not null,
--    stg_duration_days int not null
--);

--todo: CREATE INDEX pnc_smry_pth_idx ON WebAPI.dbo.pnc_study_summary_path (tx_stg_cmb_pth);
--CREATE INDEX pnc_smry_pth_qry_idx ON ${ohdsiSchema}.pnc_study_summary_path (study_id, source_id, tx_rslt_version);
CREATE INDEX pnc_smry_pth_qry_idx ON ${ohdsiSchema}.pnc_study_summary_path (study_id, tx_rslt_version);
CREATE INDEX pnc_smry_pth_prnt_idx ON ${ohdsiSchema}.pnc_study_summary_path (tx_path_parent_key, tx_rslt_version);

---------following are sql server temp table workaround------------
CREATE TABLE ${ohdsiSchema}.pnc_tmp_ptsq_ct
(
  job_execution_id BIGINT,
  study_id INT,
  source_id INT,
  person_id INT,
  tx_seq INT,
  concept_id INT,
  concept_name VARCHAR(255),
  idx_start_date DATE,
  idx_end_date DATE,
  duration_days INT
);

CREATE TABLE ${ohdsiSchema}.pnc_tmp_ptstg_ct
(
  job_execution_id BIGINT,
  study_id INT,
  source_id INT,
  person_id INT,
  tx_stg_cmb_id INT,
  tx_seq INT,
  stg_start_date DATE,
  stg_end_date DATE,
  stg_duration_days INT
);

CREATE TABLE ${ohdsiSchema}.pnc_tmp_cmb_sq_ct
(
	job_execution_id BIGINT,
	person_id INT,
	combo_ids VARCHAR(255),
	tx_seq INT,	
	combo_seq VARCHAR(400),
    start_date date,
    end_date date,
    combo_duration INT,
    result_version INT,
    gap_days INT
);

-----summary generation----------------------
CREATE TABLE ${ohdsiSchema}.pnc_tmp_smry_msql_cmb
(
    job_execution_id BIGINT,
    pnc_tx_stg_cmb_id BIGINT,
    concept_ids varchar(500),
    conceptsArray varchar(4000),
	conceptsName varchar(4000)
-- TODO: test this (4000 should be enough for one combo)
--    conceptsArray text,
--	conceptsName text    
);

CREATE TABLE ${ohdsiSchema}.pnc_tmp_indv_jsn
(
    job_execution_id BIGINT,
    rnum float,
    table_row_id int,
	rslt_version int,
	JSON varchar(4000)
);

CREATE TABLE ${ohdsiSchema}.pnc_tmp_unq_trtmt
(
    job_execution_id BIGINT,
    rnum float,
    pnc_stdy_smry_id INT,
  	rslt_version int,
    path_cmb_ids varchar(800),
    path_unique_treatment varchar(4000)
);

CREATE TABLE ${ohdsiSchema}.pnc_tmp_unq_pth_id
(
    job_execution_id BIGINT,
    pnc_tx_smry_id BIGINT,
    concept_id BIGINT,
    concept_order int,
    concept_count int,
    conceptsName varchar(1000),
    conceptsArray varchar(1500)
);

------------filtered summary only tables-----------
CREATE TABLE ${ohdsiSchema}.pnc_tmp_smrypth_fltr
(
    job_execution_id BIGINT,
    pnc_stdy_smry_id INT,
    study_id    BIGINT,
    source_id int,
    tx_path_parent_key  BIGINT,
    tx_stg_cmb    VARCHAR(255),
    tx_stg_cmb_pth VARCHAR(4000),
    tx_seq          BIGINT,
    tx_stg_cnt      BIGINT,
    tx_stg_percentage float,
    tx_stg_avg_dr   int,
    tx_stg_avg_gap   int,
    tx_avg_frm_strt int,
    tx_rslt_version int
);

CREATE TABLE ${ohdsiSchema}.pnc_tmp_smry_ancstr
(
    job_execution_id BIGINT,
    pnc_stdy_parent_id    BIGINT,
    pnc_ancestor_id    BIGINT,
    reallevel int
);
