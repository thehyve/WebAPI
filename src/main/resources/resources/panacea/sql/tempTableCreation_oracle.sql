
IF OBJECT_ID('tempdb..#_pnc_ptsq_ct', 'U') IS NOT NULL
  DROP TABLE #_pnc_ptsq_ct;

CREATE TABLE #_pnc_ptsq_ct
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

IF OBJECT_ID('tempdb..#_pnc_ptstg_ct', 'U') IS NOT NULL
  DROP TABLE #_pnc_ptstg_ct;

CREATE TABLE #_pnc_ptstg_ct
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

IF OBJECT_ID('tempdb..#_pnc_tmp_cmb_sq_ct', 'U') IS NOT NULL
  DROP TABLE #_pnc_tmp_cmb_sq_ct;

CREATE TABLE #_pnc_tmp_cmb_sq_ct
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
