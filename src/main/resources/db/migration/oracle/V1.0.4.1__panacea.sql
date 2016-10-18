CREATE TABLE ${ohdsiSchema}.panacea_study
(
    study_id    NUMBER(*,0),
    study_name  VARCHAR2(255 BYTE),
    study_desc  VARCHAR2(255 BYTE),
    concept_set_def CLOB,
    cohort_definition_id NUMBER(10,0),
    study_detail CLOB,
    switch_window NUMBER(10,0),
    study_duration NUMBER(10,0),
    start_date date,
    end_date date,
    min_unit_days number(*, 0),
    min_unit_counts number(*, 0),
    gap_threshold number(*, 2),
    concept_set_id	NUMBER(10,0),
    create_time timestamp(6),
    CONSTRAINT pk_pnc_stdy PRIMARY KEY (study_id)
);

CREATE SEQUENCE ${ohdsiSchema}.seq_pnc_stdy
MINVALUE 1
START WITH 1
INCREMENT BY 1
CACHE 10;

CREATE TABLE ${ohdsiSchema}.pnc_tx_stage_combination
(
    pnc_tx_stg_cmb_id    NUMBER(*,0),
    study_id    NUMBER(*,0),
    CONSTRAINT pk_pnc_tx_stg_cmb PRIMARY KEY (pnc_tx_stg_cmb_id)
--    CONSTRAINT fk_pnctxcmb_pncstdy FOREIGN KEY (study_id) REFERENCES panacea_study (study_id)
);

CREATE SEQUENCE ${ohdsiSchema}.seq_pnc_tx_stg_cmb
MINVALUE 1
START WITH 1
INCREMENT BY 1
CACHE 10;

CREATE TABLE ${ohdsiSchema}.pnc_tx_stage_combination_map
(
    pnc_tx_stg_cmb_mp_id    NUMBER(*,0),
    pnc_tx_stg_cmb_id    NUMBER(*,0),
    concept_id number(*,0) not null,
    concept_name VARCHAR2(255 BYTE),
    CONSTRAINT pk_pnc_tx_stg_cmb_mp PRIMARY KEY (pnc_tx_stg_cmb_mp_id)
);


CREATE SEQUENCE ${ohdsiSchema}.seq_pnc_tx_stg_cmb_mp
MINVALUE 1
START WITH 1
INCREMENT BY 1
CACHE 10;

CREATE TABLE ${ohdsiSchema}.pnc_study_summary
(
    study_id    NUMBER(*,0),
--    source_id NUMBER(*,0),
    study_results clob,
    study_results_2 clob,
    study_results_filtered clob,
    last_update_time timestamp(6)
);

CREATE TABLE ${ohdsiSchema}.pnc_study_summary_path
(
    pnc_stdy_smry_id    NUMBER(*,0),
    study_id    NUMBER(*,0),
--    source_id NUMBER(*,0),
    tx_path_parent_key  NUMBER(*,0),
    tx_stg_cmb    VARCHAR2(255 BYTE),
    tx_stg_cmb_pth VARCHAR2(4000 BYTE),
    tx_seq          NUMBER(*,0),
    tx_stg_cnt      NUMBER(*,0),
    tx_stg_percentage NUMBER(*,2),
    tx_stg_avg_dr   NUMBER(*,0),
    tx_stg_avg_gap   NUMBER(*,0),
    tx_avg_frm_strt   NUMBER(*,0),    
    tx_rslt_version NUMBER(*, 0),
    CONSTRAINT pk_pnc_stdy_smry_pth PRIMARY KEY (pnc_stdy_smry_id)
);

--alter table ${ohdsiSchema}.pnc_study_summary MODIFY (study_id NUMBER(*, 0) CONSTRAINT pnc_stdy_smry_stdy_id NOT NULL); 
--alter table ${ohdsiSchema}.pnc_study_summary ADD CONSTRAINT fk_pncstdysmry_pncstdy foreign key (study_id) references ${ohdsiSchema}.panacea_study (study_id);
--alter table ${ohdsiSchema}.pnc_study_summary MODIFY (tx_stg_cmb_id NUMBER(*, 0) CONSTRAINT pnc_stdy_smry_cmb_id NOT NULL); 
--alter table ${ohdsiSchema}.pnc_study_summary ADD CONSTRAINT fk_pncstdysmry_pncstgcmb foreign key (tx_stg_cmb_id) references ${ohdsiSchema}.pnc_tx_stage_combination (pnc_tx_stg_cmb_id);
--ALTER TABLE ${ohdsiSchema}.pnc_study_summary_path ADD tx_stg_percentage NUMBER(*,2);

CREATE SEQUENCE ${ohdsiSchema}.seq_pnc_stdy_smry
MINVALUE 1
START WITH 1
INCREMENT BY 1
CACHE 10;

--CREATE TABLE ${ohdsiSchema}.pnc_tmp_pt_sq_ct
--(
--    pnc_pt_sq_ct_id    NUMBER(*,0),
--    study_id    NUMBER(*,0),
--    source_id NUMBER(*,0),
--    person_id number(*,0),
--    tx_seq          NUMBER(*,0),
--    concept_id number(*,0),
--    concept_name VARCHAR2(255 BYTE),
--    idx_start_date date,
--    idx_end_date date,
--    duration_days number(10,0),
--    CONSTRAINT pk_pnc_tmp_pt_sq_ct PRIMARY KEY (pnc_pt_sq_ct_id)
--);

--alter table ${ohdsiSchema}.pnc_tmp_pt_sq_ct MODIFY (study_id NUMBER(*, 0) CONSTRAINT pnc_tmp_pt_sq_ct_stdy_id NOT NULL); 
--alter table ${ohdsiSchema}.pnc_tmp_pt_sq_ct ADD CONSTRAINT fk_pnctmpptsqct_pncstdy foreign key (study_id) references ${ohdsiSchema}.panacea_study (study_id);
--alter table ${ohdsiSchema}.pnc_tmp_pt_sq_ct MODIFY (person_id NUMBER(*, 0) CONSTRAINT pnc_tmp_pt_sq_ct_psn_id NOT NULL); 
--alter table ${ohdsiSchema}.pnc_tmp_pt_sq_ct MODIFY (concept_id NUMBER(*, 0) CONSTRAINT pnc_tmp_pt_sq_ct_cncpt_id NOT NULL); 
--alter table ${ohdsiSchema}.pnc_tmp_pt_sq_ct MODIFY (idx_start_date date CONSTRAINT pnc_tmp_pt_sq_ct_strt NOT NULL); 
--alter table ${ohdsiSchema}.pnc_tmp_pt_sq_ct MODIFY (idx_end_date date CONSTRAINT pnc_tmp_pt_sq_ct_end NOT NULL); 
--alter table ${ohdsiSchema}.pnc_tmp_pt_sq_ct MODIFY (duration_days number(10,0) CONSTRAINT pnc_tmp_pt_sq_ct_dr NOT NULL);     

--CREATE SEQUENCE seq_pnc_tmp_pt_sq_ct
--MINVALUE 1
--START WITH 1
--INCREMENT BY 1
--CACHE 10;

--CREATE TABLE ${ohdsiSchema}.pnc_tmp_pt_stg_sq_ct
--(
--    pnc_pt_stg_sq_id    NUMBER(*,0),
--    study_id    NUMBER(*,0),
--    source_id NUMBER(*,0),
--    person_id number(*,0),
--    tx_stg_cmb_id    NUMBER(*,0),
--    tx_seq NUMBER(*,0),
--    stg_start_date date,
--    stg_end_date date,
--    stg_duration_days  number(10, 0),
--    CONSTRAINT pk_pnc_pt_stg_sq_ct PRIMARY KEY (pnc_pt_stg_sq_id)
--);

--alter table ${ohdsiSchema}.pnc_tmp_pt_stg_sq_ct MODIFY (study_id NUMBER(*, 0) CONSTRAINT pnc_tmp_pt_stg_sq_ct_stdy_id NOT NULL); 
--alter table ${ohdsiSchema}.pnc_tmp_pt_stg_sq_ct ADD CONSTRAINT fk_pnctmpptstgsqct_pncstdy foreign key (study_id) references ${ohdsiSchema}.panacea_study (study_id);
--alter table ${ohdsiSchema}.pnc_tmp_pt_stg_sq_ct MODIFY (person_id NUMBER(*, 0) CONSTRAINT pnc_tmp_pt_stg_sq_ct_psn_id NOT NULL); 
--alter table ${ohdsiSchema}.pnc_tmp_pt_stg_sq_ct MODIFY (tx_stg_cmb_id NUMBER(*,0) CONSTRAINT pnc_tmp_pt_stg_sq_ct_cmb_id NOT NULL); 
--alter table ${ohdsiSchema}.pnc_tmp_pt_stg_sq_ct ADD CONSTRAINT fk_pnctmpptstgsqct_pncstgcmb foreign key (tx_stg_cmb_id) references ${ohdsiSchema}.pnc_tx_stage_combination (pnc_tx_stg_cmb_id);

--alter table ${ohdsiSchema}.pnc_tmp_pt_stg_sq_ct MODIFY (stg_start_date date CONSTRAINT pnc_tmp_pt_stg_sq_ct_strt NOT NULL); 
--alter table ${ohdsiSchema}.pnc_tmp_pt_stg_sq_ct MODIFY (stg_end_date date CONSTRAINT pnc_tmp_pt_stg_sq_ct_end NOT NULL); 
--alter table ${ohdsiSchema}.pnc_tmp_pt_stg_sq_ct MODIFY (stg_duration_days number(10,0) CONSTRAINT pnc_tmp_pt_stg_sq_ct_dr NOT NULL);     

CREATE SEQUENCE ${ohdsiSchema}.seq_pnc_pt_stg_sq_ct
MINVALUE 1
START WITH 1
INCREMENT BY 1
CACHE 10;


CREATE INDEX pnc_smry_pth_idx ON pnc_study_summary_path (tx_stg_cmb_pth);
--CREATE INDEX pnc_smry_pth_qry_idx ON pnc_study_summary_path (study_id, source_id, tx_rslt_version);
CREATE INDEX pnc_smry_pth_qry_idx ON pnc_study_summary_path (study_id, tx_rslt_version);
CREATE INDEX pnc_smry_pth_prnt_idx ON pnc_study_summary_path (tx_path_parent_key, tx_rslt_version);
