{DEFAULT @cdm_schema = 'OMOPV5_DE'}
{DEFAULT @results_schema = 'OHDSI'}
{DEFAULT @ohdsi_schema = 'OHDSI'}
{DEFAULT @cohortDefId = 915}
{DEFAULT @studyId = 18}
{DEFAULT @drugConceptId = '1301025,1328165,1771162,19058274,918906,923645,933724,1310149,1125315'}
{DEFAULT @procedureConceptId = '1301025,1328165,1771162,19058274,918906,923645,933724,1310149,1125315'}

@tempTableCreationOracle

DELETE FROM @pnc_ptsq_ct where job_execution_id = @jobExecId and study_id = @studyId; 

@insertFromDrugEra
@insertFromProcedure

@insertIntoComboMapString

DELETE FROM @pnc_ptstg_ct where job_execution_id = @jobExecId and study_id = @studyId; 

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
--remove rowIdString
--where ptsq.@rowIdString not in
where 
--  (select ptsq2.rowid from #_pnc_ptsq_ct ptsq1
--remove rowIdString
--  (select ptsq2.@rowIdString from @pnc_ptsq_ct ptsq1
ptsq.study_id = @studyId
and ptsq.job_execution_id = @jobExecId
AND combo.concept_id = ptsq.concept_id
--order by ptsq.person_id, ptsq.idx_start_date, ptsq.idx_end_date
) insertingPTSQ
where not exists
(
	select * from 
    (select ptsq2.* from @pnc_ptsq_ct ptsq1
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
  	) filteredPTSQ
    where
        job_execution_id = filteredPTSQ.job_execution_id
        and study_id = filteredPTSQ.study_id
        and source_id = filteredPTSQ.source_id
        and person_id = filteredPTSQ.person_id
        and concept_id = filteredPTSQ.concept_id
        and concept_name = filteredPTSQ.concept_name
        and idx_start_date = filteredPTSQ.idx_start_date
        and idx_end_date = filteredPTSQ.idx_end_date
        and duration_days = filteredPTSQ.duration_days
        and tx_seq = filteredPTSQ.tx_seq
)
--order by person_id, idx_start_date, idx_end_date;

/*
-- take care of expanded time window for same patient/same drug. 
-- EX: 2/1/2015 ~ 4/1/2015 ptstg2, 1/1/2015 ~ 3/1/2015 ptstg1. Update ptstg1 with later end date and delete ptstg2   
--OHDSI-75 refactor merge...
--merge into @pnc_ptstg_ct ptstg
--using
--  (
--    select updateRowID updateRowID, max(realEndDate) as realEndDate from 
--    (
------      select ptstg2.rowid deleteRowId, ptstg1.rowid updateRowID,
--      select ptstg2.@rowIdString deleteRowId, ptstg1.@rowIdString updateRowID,
--        case 
--          when ptstg1.stg_end_date > ptstg2.stg_end_date then ptstg1.stg_end_date
--          when ptstg2.stg_end_date > ptstg1.stg_end_date then ptstg2.stg_end_date
--          when ptstg2.stg_end_date = ptstg1.stg_end_date then ptstg2.stg_end_date
--        end as realEndDate
--      from @pnc_ptstg_ct ptstg1
--      join @pnc_ptstg_ct ptstg2
--      on ptstg1.person_id = ptstg2.person_id
--      and ptstg1.tx_stg_cmb_id = ptstg2.tx_stg_cmb_id
--      and ptstg2.job_execution_id = @jobExecId
--      where ptstg2.stg_start_date < ptstg1.stg_end_date
--        and ptstg2.stg_start_date > ptstg1.stg_start_date
--        and ptstg1.job_execution_id = @jobExecId
--    ) innerT group by updateRowID
--  ) ptstgExpandDate
--  on
--  (
------     ptstg.rowid = ptstgExpandDate.updateRowID
--     ptstg.@rowIdString = ptstgExpandDate.updateRowID
--  )
--  WHEN MATCHED then update set ptstg.stg_end_date = ptstgExpandDate.realEndDate,
------ sqlserver:   ptstg.stg_duration_days = (ptstgExpandDate.realEndDate - ptstg.stg_start_date + 1);
--	ptstg.stg_duration_days = DATEDIFF(DAY, ptstg.stg_start_date, ptstgExpandDate.realEndDate) + 1;
--remove rowIdString
--with ptstgExpandDate (updateRowID, realEndDate) as
with ptstgExpandDate (job_execution_id, study_id, source_id, person_id, tx_stg_cmb_id, 
	stg_start_date, stg_duration_days, tx_seq, realEndDate) as
  (
--remove rowIdString
--    select updateRowID updateRowID, max(realEndDate) as realEndDate from
    select job_execution_id, study_id, source_id, person_id, tx_stg_cmb_id, 
			stg_start_date, stg_duration_days, tx_seq, max(realEndDate) as realEndDate from 
    (
----      select ptstg2.rowid deleteRowId, ptstg1.rowid updateRowID,
--remove rowIdString
--      select ptstg2.@rowIdString deleteRowId, ptstg1.@rowIdString updateRowID,
      select ptstg1.*,
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
--remove rowIdString
--    ) innerT group by updateRowID
    ) innerT group by job_execution_id, study_id, source_id, person_id, tx_stg_cmb_id, 
			stg_start_date, stg_duration_days, tx_seq
  )
  update @pnc_ptstg_ct
  set stg_end_date = ptstgExpandDate.realEndDate,
---- sqlserver:   ptstg.stg_duration_days = (ptstgExpandDate.realEndDate - ptstg.stg_start_date + 1);
--remove rowIdString
--	stg_duration_days = DATEDIFF(DAY, stg_start_date, ptstgExpandDate.realEndDate) + 1
	stg_duration_days = DATEDIFF(DAY, @pnc_ptstg_ct.stg_start_date, ptstgExpandDate.realEndDate) + 1
  from ptstgExpandDate
--remove rowIdString
--  where @rowIdString = ptstgExpandDate.updateRowID;
  where 
		@pnc_ptstg_ct.job_execution_id = ptstgExpandDate.job_execution_id
		and @pnc_ptstg_ct.study_id = ptstgExpandDate.study_id
    and @pnc_ptstg_ct.source_id = ptstgExpandDate.source_id
		and @pnc_ptstg_ct.person_id = ptstgExpandDate.person_id
		and @pnc_ptstg_ct.tx_stg_cmb_id = ptstgExpandDate.tx_stg_cmb_id
		and @pnc_ptstg_ct.stg_start_date = ptstgExpandDate.stg_start_date        
		and @pnc_ptstg_ct.stg_duration_days = ptstgExpandDate.stg_duration_days
		and @pnc_ptstg_ct.tx_seq = ptstgExpandDate.tx_seq;

delete from @pnc_ptstg_ct 
--where ptstg.rowid in
--remove rowIdString
--where @rowIdString in
where exists 
  (
--    select ptstg2.rowid deleteRowId
--remove rowIdString
    select * from
--    select ptstg2.@rowIdString deleteRowId
    (select ptstg2.*
    from @pnc_ptstg_ct ptstg1
    join @pnc_ptstg_ct ptstg2
    on ptstg1.person_id = ptstg2.person_id
    and ptstg1.tx_stg_cmb_id = ptstg2.tx_stg_cmb_id
    and ptstg2.job_execution_id = @jobExecId
    where ptstg2.stg_start_date < ptstg1.stg_end_date
      and ptstg2.stg_start_date > ptstg1.stg_start_date
--remove rowIdString
--      and ptstg1.job_execution_id = @jobExecId
			and ptstg1.job_execution_id = @jobExecId) innerDelete
    where 
			@pnc_ptstg_ct.job_execution_id = innerDelete.job_execution_id
      and @pnc_ptstg_ct.study_id = innerDelete.study_id
      and @pnc_ptstg_ct.source_id = innerDelete.source_id
			and @pnc_ptstg_ct.person_id = innerDelete.person_id
      and @pnc_ptstg_ct.tx_stg_cmb_id = innerDelete.tx_stg_cmb_id
      and @pnc_ptstg_ct.stg_start_date = innerDelete.stg_start_date        
      and @pnc_ptstg_ct.stg_duration_days = innerDelete.stg_duration_days
      and @pnc_ptstg_ct.tx_seq = innerDelete.tx_seq      
  );

--TRUNCATE TABLE #_pnc_ptsq_ct;
--DROP TABLE #_pnc_ptsq_ct;

--TRUNCATE TABLE #_pnc_ptstg_ct;
--DROP TABLE #_pnc_ptstg_ct;

--TRUNCATE TABLE #_pnc_sngl_cmb;
--DROP TABLE #_pnc_sngl_cmb;

--TRUNCATE TABLE #_pnc_tmp_cmb_sq_ct;
--DROP TABLE #_pnc_tmp_cmb_sq_ct;
*/