IF OBJECT_ID('tempdb..#_pnc_smry_msql_cmb', 'U') IS NOT NULL
  DROP TABLE #_pnc_smry_msql_cmb;
 
CREATE TABLE #_pnc_smry_msql_cmb
(
    job_execution_id BIGINT,
    pnc_tx_stg_cmb_id int,
    concept_ids varchar(500),
    conceptsArray varchar(4000),
	conceptsName varchar(4000)
-- TODO: test this (4000 should be enough for one combo)
--    conceptsArray text,
--	conceptsName text    
);

IF OBJECT_ID('tempdb..#_pnc_indv_jsn', 'U') IS NOT NULL
  DROP TABLE #_pnc_indv_jsn;
 
CREATE TABLE #_pnc_indv_jsn
(
    job_execution_id BIGINT,
    rnum float,
    table_row_id int,
	rslt_version int,
	JSON varchar(4000)
);


IF OBJECT_ID('tempdb..#_pnc_unq_trtmt', 'U') IS NOT NULL
  DROP TABLE #_pnc_unq_trtmt;

CREATE TABLE #_pnc_unq_trtmt
(
    job_execution_id BIGINT,
    rnum float,
    pnc_stdy_smry_id int,
  	rslt_version int,
    path_cmb_ids varchar(800),
    path_unique_treatment varchar(4000)
);


IF OBJECT_ID('tempdb..#_pnc_unq_pth_id', 'U') IS NOT NULL
  DROP TABLE #_pnc_unq_pth_id;

CREATE TABLE #_pnc_unq_pth_id
(
    job_execution_id BIGINT,
    pnc_tx_smry_id int,
    concept_id int,
    concept_order int,
    concept_count int,
    conceptsName varchar(1000),
    conceptsArray varchar(1500)
);
