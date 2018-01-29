{DEFAULT @cdm_schema = 'OMOPV5_DE'}
{DEFAULT @results_schema = 'OHDSI'}
{DEFAULT @ohdsi_schema = 'OHDSI'}
{DEFAULT @cohortDefId = 915}
{DEFAULT @studyId = 18}
{DEFAULT @drugConceptId = '1301025,1328165,1771162,19058274,918906,923645,933724,1310149,1125315'}
{DEFAULT @procedureConceptId = '1301025,1328165,1771162,19058274,918906,923645,933724,1310149,1125315'}

@tempTableCreationOracle

@insertFromDrugEra
@insertFromProcedure

-- sql server compatibility changes: no use of rownum, %%physloc%% for rowid (feed in with code for different dialect)
--MERGE INTO #_pnc_ptsq_ct ptsq
--USING
--(SELECT rank() OVER (PARTITION BY person_id
--  ORDER BY person_id, idx_start_date, idx_end_date, ROWNUM) real_tx_seq,
--  rowid AS the_rowid
--  FROM #_pnc_ptsq_ct 
--) ptsq1
--ON
--(
--     ptsq.rowid = ptsq1.the_rowid
--)
--WHEN MATCHED THEN UPDATE SET ptsq.tx_seq = ptsq1.real_tx_seq;

MERGE INTO @pnc_ptsq_ct ptsq
USING
(SELECT rank() OVER (PARTITION BY person_id
  ORDER BY person_id, idx_start_date, idx_end_date, concept_id) real_tx_seq,
  @rowIdString AS the_rowid
  FROM @pnc_ptsq_ct
  WHERE job_execution_id = @jobExecId
) ptsq1
ON
(
     ptsq.@rowIdString = ptsq1.the_rowid
)
WHEN MATCHED THEN UPDATE SET ptsq.tx_seq = ptsq1.real_tx_seq;


--IF OBJECT_ID('tempdb..#_pnc_sngl_cmb', 'U') IS NOT NULL
--  DROP TABLE #_pnc_sngl_cmb;

--CREATE TABLE #_pnc_sngl_cmb
--(
--  tx_stg_cmb_id INT,
--  concept_id INT,
--  concept_name VARCHAR(255)
--);

--get single concept combo
--insert into #_pnc_sngl_cmb (tx_stg_cmb_id, concept_id, concept_name)
--(select comb.pnc_tx_stg_cmb_id, combmap.concept_id, combmap.concept_name from @results_schema.pnc_tx_stage_combination comb
--join @results_schema.pnc_tx_stage_combination_map combMap 
--on combmap.pnc_tx_stg_cmb_id = comb.pnc_tx_stg_cmb_id
--join 
--(select comb.pnc_tx_stg_cmb_id, count(*) from @results_schema.pnc_tx_stage_combination comb
--join @results_schema.pnc_tx_stage_combination_map combMap 
--on combmap.pnc_tx_stg_cmb_id = comb.pnc_tx_stg_cmb_id
--where comb.study_id = @studyId
--group by comb.pnc_tx_stg_cmb_id
--having count(*) = 1) multiple_ids_combo
--on multiple_ids_combo.pnc_tx_stg_cmb_id = comb.pnc_tx_stg_cmb_id
--);


--MERGE INTO @results_schema.pnc_tx_stage_combination_map combo
--USING
--  (SELECT DISTINCT ptsq.concept_id concept_id, ptsq.concept_name concept_name FROM #_pnc_ptsq_ct ptsq
--    WHERE ptsq.concept_id NOT IN 
--      (select exist_combo.concept_id from 
--        (SELECT comb_map.concept_id concept_id FROM @results_schema.pnc_tx_stage_combination comb
--        JOIN @results_schema.pnc_tx_stage_combination_map comb_map
--        ON comb_map.pnc_tx_stg_cmb_id = comb.pnc_tx_stg_cmb_id
--        WHERE comb.study_id = @studyId) exist_combo
--      )
--  ) adding_concept
--  ON
--  (
--    1 = 0
--  )
--WHEN NOT MATCHED THEN INSERT (PNC_TX_STG_CMB_MP_ID, PNC_TX_STG_CMB_ID, CONCEPT_ID, CONCEPT_NAME)
--VALUES (@results_schema.seq_pnc_tx_stg_cmb_mp.NEXTVAL, @results_schema.seq_pnc_tx_stg_cmb.NEXTVAL, adding_concept.concept_id, adding_concept.concept_name);

@insertIntoComboMapString

--move into specific db script with @insertIntoComboMapString
--MERGE INTO @results_schema.pnc_tx_stage_combination comb
--USING
--  (
--    SELECT combo_map.pnc_tx_stg_cmb_id pnc_tx_stg_cmb_id FROM @results_schema.pnc_tx_stage_combination_map combo_map
--  ) adding_combo
--  ON
--  (
--    comb.pnc_tx_stg_cmb_id = adding_combo.pnc_tx_stg_cmb_id
--  )
--WHEN NOT MATCHED THEN INSERT (PNC_TX_STG_CMB_ID,STUDY_ID)
--VALUES (adding_combo.pnc_tx_stg_cmb_id, @studyId);


-- insert from #_pnc_ptsq_ct ptsq into #_pnc_ptstg_ct (remove same patient/same drug small time window inside large time window. EX: 1/2/2015 ~ 1/31/2015 inside 1/1/2015 ~ 3/1/2015)
-- use single concept combo and avoid duplicate combo for the same concept if there's multiple single combo for same concept by min() value 
insert into @pnc_ptstg_ct (job_execution_id, study_id, source_id, person_id, tx_stg_cmb_id, stg_start_date, stg_end_date, stg_duration_days)
select @jobExecId, insertingPTSQ.study_id, insertingPTSQ.source_id, insertingPTSQ.person_id, insertingPTSQ.pnc_tx_stg_cmb_id, insertingPTSQ.idx_start_date, insertingPTSQ.idx_end_date, insertingPTSQ.duration_days
from (select ptsq.study_id, ptsq.source_id, ptsq.person_id, ptsq.idx_start_date, ptsq.idx_end_date, ptsq.duration_days, combo.pnc_tx_stg_cmb_id from @pnc_ptsq_ct ptsq,
  (select min(comb.pnc_tx_stg_cmb_id) pnc_tx_stg_cmb_id, combmap.concept_id concept_id, combmap.concept_name concept_name from @results_schema.pnc_tx_stage_combination comb
  	join @results_schema.pnc_tx_stage_combination_map combMap 
	on combmap.pnc_tx_stg_cmb_id = comb.pnc_tx_stg_cmb_id
	join 
	(select comb.pnc_tx_stg_cmb_id, count(*) cnt from @results_schema.pnc_tx_stage_combination comb
	join @results_schema.pnc_tx_stage_combination_map combMap 
	on combmap.pnc_tx_stg_cmb_id = comb.pnc_tx_stg_cmb_id
	where comb.study_id = @studyId
	group by comb.pnc_tx_stg_cmb_id
	having count(*) = 1) multiple_ids_combo
	on multiple_ids_combo.pnc_tx_stg_cmb_id = comb.pnc_tx_stg_cmb_id
	group by combmap.concept_id, combmap.concept_name
  ) combo
--where ptsq.rowid not in
where ptsq.@rowIdString not in
--  (select ptsq2.rowid from #_pnc_ptsq_ct ptsq1
  (select ptsq2.@rowIdString from @pnc_ptsq_ct ptsq1
    join @pnc_ptsq_ct ptsq2
    on ptsq1.person_id = ptsq2.person_id
    and ptsq1.concept_id = ptsq2.concept_id
    and ptsq2.job_execution_id = @jobExecId
    where (
      (ptsq1.job_execution_id = @jobExecId)
      and (ptsq2.idx_start_date > ptsq1.idx_start_date)
      and (ptsq2.idx_end_date < ptsq1.idx_end_date
      or ptsq2.idx_end_date = ptsq1.idx_end_date))
    or ((ptsq2.idx_start_date > ptsq1.idx_start_date
      or ptsq2.idx_start_date = ptsq1.idx_start_date
      ) and (ptsq2.idx_end_date < ptsq1.idx_end_date)
      and (ptsq1.job_execution_id = @jobExecId))
  )
and ptsq.study_id = @studyId
and ptsq.job_execution_id = @jobExecId
AND combo.concept_id = ptsq.concept_id
--order by ptsq.person_id, ptsq.idx_start_date, ptsq.idx_end_date
) insertingPTSQ
order by person_id, idx_start_date, idx_end_date;


-- take care of expanded time window for same patient/same drug. 
-- EX: 2/1/2015 ~ 4/1/2015 ptstg2, 1/1/2015 ~ 3/1/2015 ptstg1. Update ptstg1 with later end date and delete ptstg2   
merge into @pnc_ptstg_ct ptstg
using
  (
    select updateRowID updateRowID, max(realEndDate) as realEndDate from 
    (
--      select ptstg2.rowid deleteRowId, ptstg1.rowid updateRowID,
      select ptstg2.@rowIdString deleteRowId, ptstg1.@rowIdString updateRowID,
        case 
          when ptstg1.stg_end_date > ptstg2.stg_end_date then ptstg1.stg_end_date
          when ptstg2.stg_end_date > ptstg1.stg_end_date then ptstg2.stg_end_date
          when ptstg2.stg_end_date = ptstg1.stg_end_date then ptstg2.stg_end_date
        end as realEndDate
      from @pnc_ptstg_ct ptstg1
      join @pnc_ptstg_ct ptstg2
      on ptstg1.person_id = ptstg2.person_id
      and ptstg1.tx_stg_cmb_id = ptstg2.tx_stg_cmb_id
      and ptstg2.job_execution_id = @jobExecId
      where ptstg2.stg_start_date < ptstg1.stg_end_date
        and ptstg2.stg_start_date > ptstg1.stg_start_date
        and ptstg1.job_execution_id = @jobExecId
    ) innerT group by updateRowID
  ) ptstgExpandDate
  on
  (
--     ptstg.rowid = ptstgExpandDate.updateRowID
     ptstg.@rowIdString = ptstgExpandDate.updateRowID
  )
  WHEN MATCHED then update set ptstg.stg_end_date = ptstgExpandDate.realEndDate,
-- sqlserver:   ptstg.stg_duration_days = (ptstgExpandDate.realEndDate - ptstg.stg_start_date + 1);
	ptstg.stg_duration_days = DATEDIFF(DAY, ptstg.stg_start_date, ptstgExpandDate.realEndDate) + 1;

    
delete from @pnc_ptstg_ct 
--where ptstg.rowid in
where @rowIdString in 
  (
--    select ptstg2.rowid deleteRowId
    select ptstg2.@rowIdString deleteRowId
    from @pnc_ptstg_ct ptstg1
    join @pnc_ptstg_ct ptstg2
    on ptstg1.person_id = ptstg2.person_id
    and ptstg1.tx_stg_cmb_id = ptstg2.tx_stg_cmb_id
    and ptstg2.job_execution_id = @jobExecId
    where ptstg2.stg_start_date < ptstg1.stg_end_date
      and ptstg2.stg_start_date > ptstg1.stg_start_date
      and ptstg1.job_execution_id = @jobExecId
  );


--TRUNCATE TABLE #_pnc_ptsq_ct;
--DROP TABLE #_pnc_ptsq_ct;

--TRUNCATE TABLE #_pnc_ptstg_ct;
--DROP TABLE #_pnc_ptstg_ct;

--TRUNCATE TABLE #_pnc_sngl_cmb;
--DROP TABLE #_pnc_sngl_cmb;

--TRUNCATE TABLE #_pnc_tmp_cmb_sq_ct;
--DROP TABLE #_pnc_tmp_cmb_sq_ct;
