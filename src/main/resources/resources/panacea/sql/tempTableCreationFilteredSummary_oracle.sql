IF OBJECT_ID('tempdb..#_pnc_smrypth_fltr', 'U') IS NOT NULL
  DROP TABLE #_pnc_smrypth_fltr;

CREATE TABLE #_pnc_smrypth_fltr
(
    job_execution_id BIGINT,
    pnc_stdy_smry_id int,
    study_id    int,
    source_id int,
    tx_path_parent_key  int,
    tx_stg_cmb    VARCHAR(255),
    tx_stg_cmb_pth VARCHAR(4000),
    tx_seq          int,
    tx_stg_cnt      int,
    tx_stg_percentage float,
    tx_stg_avg_dr   int,
    tx_stg_avg_gap   int,
    tx_avg_frm_strt   NUMBER(*,0),
    tx_rslt_version int
);


IF OBJECT_ID('tempdb..#_pnc_smry_ancstr', 'U') IS NOT NULL
  DROP TABLE #_pnc_smry_ancstr;

CREATE TABLE #_pnc_smry_ancstr
(
    job_execution_id BIGINT,
    pnc_stdy_parent_id    int,
    pnc_ancestor_id    int,
    reallevel int
);
