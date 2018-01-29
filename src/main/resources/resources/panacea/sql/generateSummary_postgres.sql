delete from @pnc_smry_msql_cmb where job_execution_id = @jobExecId;
delete from @pnc_indv_jsn where job_execution_id = @jobExecId;
delete from @pnc_unq_trtmt where job_execution_id = @jobExecId;
delete from @pnc_unq_pth_id where job_execution_id = @jobExecId;
delete from @pnc_smrypth_fltr where job_execution_id = @jobExecId;
delete from @pnc_smry_ancstr where job_execution_id = @jobExecId;


delete from @results_schema.pnc_study_summary_path where study_id = @studyId ;

insert into @results_schema.pnc_study_summary_path (study_id, tx_path_parent_key, tx_stg_cmb, tx_stg_cmb_pth, tx_seq, tx_stg_cnt, tx_stg_avg_dr, tx_stg_avg_gap, tx_rslt_version, tx_avg_frm_strt)
select @studyId, null, aggregatePath.combo_ids, aggregatePath.combo_seq, aggregatePath.tx_seq, aggregatePath.patientCount, aggregatePath.averageDurationDays, aggregatePath.averageGapDays, aggregatePath.result_version, aggregatePath.avgFrmCohortStart 
from
   (select ptTxPath.combo_ids combo_ids, ptTxPath.combo_seq combo_seq, ptTxPath.tx_seq tx_seq, count(*) patientCount, avg(ptTxPath.combo_duration) averageDurationDays, avg(ptTxPath.gap_days) averageGapDays, ptTxPath.result_version result_version,
	avg(DATEDIFF(DAY, co.cohort_start_date, ptTxPath.start_date) + 1) avgFrmCohortStart
  		from @pnc_tmp_cmb_sq_ct ptTxPath
  		join @ohdsi_schema.cohort co
  		on co.subject_id = ptTxPath.person_id
--  		and co.cohort_definition_id = (select cohort_definition_id
--    from @results_schema.panacea_study where study_id = @studyId)
			and co.cohort_definition_id = @cohort_definition_id
    where ptTxPath.job_execution_id = @jobExecId
    group by ptTxPath.combo_ids, ptTxPath.combo_seq, ptTxPath.tx_seq, ptTxPath.result_version) aggregatePath;

-- version = 1
with m1 (the_rowid, parentKey, pPath, parentCount, childCount, percentage) as
  (
	select pathsum.CTID as the_rowid, parentpath.pnc_stdy_smry_id as parentKey, updateParentPath.parentPath pPath, 
    parentPath.tx_stg_cnt parentCount, pathSum.tx_stg_cnt childCount, isnull(ROUND(cast(pathSum.tx_stg_cnt as float)/cast(parentPath.tx_stg_cnt as float) * 100,2),0) percentage
    from @results_schema.pnc_study_summary_path pathSum
    join (select CTID as rowid, SUBSTRING(tx_stg_cmb_pth , 0 , len(tx_stg_cmb_pth) - len(tx_stg_cmb) ) as parentPath
    from @results_schema.pnc_study_summary_path
    where study_id = @studyId and tx_rslt_version = 1 
    ) updateParentPath
    on updateParentPath.rowid = pathSum.CTID
    join @results_schema.pnc_study_summary_path parentPath
    on updateParentPath.parentPath = parentPath.tx_stg_cmb_pth
    and parentPath.study_id = @studyId
    and parentPath.tx_rslt_version = 1
    where pathSum.study_id = @studyId 
    and pathSum.tx_rslt_version = 1 
    and parentPath.tx_rslt_version = 1 
    group by pathsum.CTID, parentpath.pnc_stdy_smry_id, updateParentPath.parentPath, parentPath.tx_stg_cnt, pathSum.tx_stg_cnt
  )
  update @results_schema.pnc_study_summary_path
  set tx_path_parent_key = m1.parentKey, tx_stg_percentage = m1.percentage
  from m1
  where CTID = m1.the_rowid;


with m1 (the_rowid, totalRootCount, parentCount, childCount, percentage) as
  (
    select pathsum.CTID as the_rowid, rootCount.totalRootCount,
    rootCount.totalRootCount parentCount, pathSum.tx_stg_cnt childCount, isnull(ROUND(cast(pathSum.tx_stg_cnt as float)/cast(rootCount.totalRootCount as float) * 100,2),0) percentage
    from @results_schema.pnc_study_summary_path pathSum, (select sum(tx_stg_cnt) totalRootCount from @results_schema.pnc_study_summary_path
    where tx_path_parent_key is null and tx_rslt_version = 1
      and study_id = @studyId 
      ) rootCount
    where tx_path_parent_key is null
    and pathSum.study_id = @studyId 
    and pathsum.tx_rslt_version = 1
  )
  update @results_schema.pnc_study_summary_path
  set tx_stg_percentage = m1.percentage
  from m1
  where CTID = m1.the_rowid;
  
  
-- version = 2
with m1 (the_rowid, parentKey, pPath, parentCount, childCount, percentage) as (
	select pathsum.CTID as the_rowid, parentpath.pnc_stdy_smry_id as parentKey, updateParentPath.parentPath pPath, 
    parentPath.tx_stg_cnt parentCount, pathSum.tx_stg_cnt childCount, isnull(ROUND(cast(pathSum.tx_stg_cnt as float)/cast(parentPath.tx_stg_cnt as float) * 100,2),0) percentage
    from @results_schema.pnc_study_summary_path pathSum
    join (select CTID as rowid, SUBSTRING(tx_stg_cmb_pth , 0 , len(tx_stg_cmb_pth) - len(tx_stg_cmb) ) as parentPath
    from @results_schema.pnc_study_summary_path
    where study_id = @studyId and tx_rslt_version = 2 
    ) updateParentPath
    on updateParentPath.rowid = pathSum.CTID
    join @results_schema.pnc_study_summary_path parentPath
    on updateParentPath.parentPath = parentPath.tx_stg_cmb_pth
    and parentPath.study_id = @studyId
    and parentPath.tx_rslt_version = 2
    where pathSum.study_id = @studyId 
    and pathSum.tx_rslt_version = 2 
    and parentPath.tx_rslt_version = 2 
    group by pathsum.CTID, parentpath.pnc_stdy_smry_id, updateParentPath.parentPath, parentPath.tx_stg_cnt, pathSum.tx_stg_cnt
  )
  update @results_schema.pnc_study_summary_path
  set tx_path_parent_key = m1.parentKey, tx_stg_percentage = m1.percentage
  from m1
  where CTID = m1.the_rowid;

with m1 (the_rowid, totalRootCount, parentCount, childCount, percentage) as
  (
    select pathsum.CTID as the_rowid, rootCount.totalRootCount,
    rootCount.totalRootCount parentCount, pathSum.tx_stg_cnt childCount, isnull(ROUND(cast(pathSum.tx_stg_cnt as float)/cast(rootCount.totalRootCount as float) * 100,2),0) percentage
    from @results_schema.pnc_study_summary_path pathSum, (select sum(tx_stg_cnt) totalRootCount from @results_schema.pnc_study_summary_path
    where tx_path_parent_key is null and tx_rslt_version = 2
      and study_id = @studyId 
      ) rootCount
    where tx_path_parent_key is null
    and pathSum.study_id = @studyId 
    and pathsum.tx_rslt_version = 2
  )
  update @results_schema.pnc_study_summary_path
  set tx_stg_percentage = m1.percentage
  from m1
  where CTID = m1.the_rowid;
  

delete from @results_schema.pnc_study_summary where study_id = @studyId ;


---------------ms sql collapse/merge multiple rows to concatenate strings (JSON string for conceptsArrary and conceptsName) ------
insert into @pnc_smry_msql_cmb (job_execution_id, pnc_tx_stg_cmb_id, concept_ids, conceptsArray, conceptsName)
select @jobExecId, comb_id, concept_ids, conceptsArray, conceptsName 
from
(
	select @jobExecId, comb.pnc_tx_stg_cmb_id comb_id,
    string_agg(combMap.concept_id::character varying, ',') concept_ids,
    '[' || string_agg('{"innerConceptName":' || '"' || combMap.concept_name  || '"' ||
    ',"innerConceptId":' || combMap.concept_id || '}'::character varying, ',') || ']' conceptsArray,
    string_agg(combMap.concept_name::character varying, ',') conceptsName
    from @results_schema.pnc_tx_stage_combination comb
    join @results_schema.pnc_tx_stage_combination_map combMap 
    on comb.pnc_tx_stg_cmb_id = combmap.pnc_tx_stg_cmb_id
    where comb.study_id = @studyId
    group by comb.pnc_tx_stg_cmb_id
) studyCombo;

    
-----------------generate rows of JSON (based on hierarchical data, without using oracle connect/level, each path is a row) insert into temp table----------------------

-------------------------------version 1 insert into temp table----------------------------------------------
IF OBJECT_ID('tempdb..#pnc_tmp_smry', 'U') IS NOT NULL
  DROP TABLE #pnc_tmp_smry;

create table #pnc_tmp_smry(
	rnum int, 
	combo_id bigint,
	current_path varchar(4000),
	path_seq bigint,
	avg_duration int, 
	pt_count int, 
	pt_percentage numeric(5, 2),
	concept_names varchar(4000),
	combo_concepts varchar(4000),
	lvl int,
	parent_concept_names varchar(4000),
	parent_combo_concepts varchar(4000),
	avg_gap int,
	gap_pcnt numeric(5, 2),
	uniqueConceptsName varchar(4000),
	uniqueConceptsArray varchar(4000),
	uniqueConceptCount int,
	daysFromStart int
);

WITH RECURSIVE t1( pnc_stdy_smry_id, tx_path_parent_key, lvl, 
	tx_stg_cmb, tx_stg_cmb_pth, tx_seq, tx_stg_avg_dr, tx_stg_cnt, 
	tx_stg_percentage, depthOrder, parent_comb) AS (
        SELECT 
           pnc_stdy_smry_id as pnc_stdy_smry_id, tx_path_parent_key as tx_path_parent_key,
           1 AS lvl,
           tx_stg_cmb as tx_stg_cmb, tx_stg_cmb_pth as tx_stg_cmb_pth, tx_seq as tx_seq, tx_stg_avg_dr as tx_stg_avg_dr, tx_stg_cnt as tx_stg_cnt, tx_stg_percentage as tx_stg_percentage
           ,CAST(pnc_stdy_smry_id AS character varying)|| '' as depthOrder, CAST(null as character varying) as parent_comb
          FROM   @results_schema.pnc_study_summary_path
        WHERE pnc_stdy_smry_id in (select pnc_stdy_smry_id from @results_schema.pnc_study_summary_path
              where 
              study_id = @studyId
              and tx_rslt_version = 1
              and tx_path_parent_key is null)
        UNION ALL
        SELECT 
              t2.pnc_stdy_smry_id as pnc_stdy_smry_id, t2.tx_path_parent_key as tx_path_parent_key,
              t1.lvl+1 as lvl,
              t2.tx_stg_cmb as tx_stg_cmb, t2.tx_stg_cmb_pth as tx_stg_cmb_pth, t2.tx_seq as tx_seq, t2.tx_stg_avg_dr as tx_stg_avg_dr, t2.tx_stg_cnt as tx_stg_cnt, t2.tx_stg_percentage as tx_stg_percentage
              ,t1.depthOrder+'.'+CAST(t2.pnc_stdy_smry_id AS varchar(max)) as depthOrder, ISNULL(t1.tx_stg_cmb,'') as parent_comb
        FROM   @results_schema.pnc_study_summary_path t2, t1
        WHERE  t2.tx_path_parent_key = t1.pnc_stdy_smry_id
      )
	  insert into #pnc_tmp_smry (rnum, combo_id, current_path, path_seq, avg_duration, pt_count, pt_percentage,	concept_names, combo_concepts, lvl,	parent_concept_names, parent_combo_concepts)
      SELECT row_number() over(order by depthOrder) as rnum, 
		cast(tx_stg_cmb as bigint) as combo_id,
		tx_stg_cmb_pth as current_path,
		tx_seq as path_seq,
		tx_stg_avg_dr as avg_duration,
		tx_stg_cnt as pt_count,
		tx_stg_percentage as pt_percentage,
	    concepts.conceptsName                as concept_names,
		concepts.conceptsArray               as combo_concepts,
		lvl as lvl,
	    parentConcepts.conceptsName			 as parent_concept_names,
		parentConcepts.conceptsArray            as parent_combo_concepts
      FROM t1
  join @pnc_smry_msql_cmb concepts 
  on concepts.pnc_tx_stg_cmb_id = cast(t1.tx_stg_cmb as bigint)
  and concepts.job_execution_id = @jobExecId
  left join @pnc_smry_msql_cmb parentConcepts 
  on parentConcepts.pnc_tx_stg_cmb_id = cast(t1.parent_comb as bigint)
  and parentConcepts.job_execution_id = @jobExecId
  order by depthOrder;
  
insert into @pnc_indv_jsn(job_execution_id, rnum, table_row_id, rslt_version, JSON)
select @jobExecId as jobExecId, rnum as rnum, table_row_id as table_row_id, rslt_version as rslt_version, JSON as JSON
from
(
select allRoots.rnum as rnum, cast(1 as bigint) as table_row_id, 1 as rslt_version, 
CASE 
    WHEN rnum = 1 THEN '{"comboId": "root","children": [' || substring(JSON_SNIPPET, 2, len(JSON_SNIPPET))
    ELSE JSON_SNIPPET
END
as JSON
from 
(
  select 
  rnum as rnum,
  CASE 
    WHEN Lvl = 1 THEN ',{'
    WHEN Lvl - LAG(Lvl) OVER (order by rnum) = 1 THEN ',"children" : [{' 
    ELSE ',{' 
  END 
  || ' "comboId" : ' || cast(combo_id as varchar(max))|| ' '
  || ' ,"conceptName" : "' || concept_names || '" '  
  || ' ,"patientCount" : ' || cast(pt_count as varchar(max))|| ' '
  || ' ,"percentage" : "' || cast(pt_percentage as varchar(max)) || '" '  
  || ' ,"avgDuration" : ' || cast(avg_duration as varchar(max)) || ' '
  || ',"concepts" : ' || combo_concepts 
  || CASE WHEN Lvl > 1 THEN    
        ',"parentConcept": { "parentConceptName": "' || cast(parent_concept_names as text) || '", ' || '"parentConcepts":' || cast(parent_combo_concepts as text)   || '}'
    ELSE ''
    END 
  || CASE WHEN LEAD(Lvl, 1, 1) OVER (order by rnum) - Lvl <= 0
--sql server: simplify without rpad... no formatting here
--     THEN '}' + rpad( ' ', 1+ (-2 * (LEAD(Lvl, 1, 1) OVER (order by rnum) - Lvl)), ']}' )
--     THEN '}' + ']}'
--     THEN '}' + replicate(']}', 1+ (-2 * (LEAD(Lvl, 1, 1) OVER (order by rnum) - Lvl)))
--     THEN '}' + replicate(']}', 1+ (-2 * (LEAD(Lvl, 1, 1) OVER (order by rnum) - Lvl)))
	THEN '}' || repeat(']}', lvl - LEAD(Lvl, 1, 1) OVER (order by rnum)) 
     ELSE ''
  END as JSON_SNIPPET
from #pnc_tmp_smry connect_by_query
) allRoots
union all
select lastRow.rnum as rnum, lastRow.table_row_id as table_row_id, 1 as rslt_version, ']}' as JSON from (
	select distinct 1000000000 as rnum, 1 as table_row_id from @results_schema.pnc_study_summary_path) lastRow
) allRootsAndLastPadding;

-------------------------------------version 1 into summary table-------------------------------------
insert into @results_schema.pnc_study_summary (study_id, study_results)
select @studyId, JSON from (
select distinct
  string_agg(tab1.JSON::character varying, '') as JSON
FROM @pnc_indv_jsn tab1
where tab1.job_execution_id = @jobExecId
and tab1.table_row_id = 1
and tab1.rslt_version = 1
group by table_row_id
) mergeJsonRowsTable;


------------------try unique path here-----------------
WITH RECURSIVE t1(combo_id, current_path1, pnc_stdy_smry_id, parent_key, modified_path, Lvl, depthOrder) AS (
        SELECT 
          tx_stg_cmb                            as combo_id
          ,tx_stg_cmb_pth                       as current_path1
          ,pnc_stdy_smry_id                     as pnc_stdy_smry_id
          ,tx_path_parent_key                   as parent_key
          ,cast(tx_stg_cmb as varchar(max))                           as modified_path
          ,1                                    as Lvl
          ,cast(pnc_stdy_smry_id AS varchar(max))+''                 as depthOrder
          FROM   @results_schema.pnc_study_summary_path
  		WHERE pnc_stdy_smry_id in (select pnc_stdy_smry_id from @results_schema.pnc_study_summary_path
        where 
	        study_id = @studyId
        	and tx_rslt_version = 2
	        and tx_path_parent_key is null)
        UNION ALL
        SELECT 
          t2.tx_stg_cmb                           as combo_id
          ,t2.tx_stg_cmb_pth                       as current_path1
          ,t2.pnc_stdy_smry_id                     as pnc_stdy_smry_id
          ,t2.tx_path_parent_key                   as parent_key
          ,cast(t1.modified_path as varchar(max))+'>'+cast(t2.tx_stg_cmb as varchar(max))       as modified_path
          ,lvl+1                                as Lvl
          ,t1.depthOrder+'.'+cast(t2.pnc_stdy_smry_id AS varchar(max)) as depthOrder
        FROM    @results_schema.pnc_study_summary_path t2, t1
        WHERE  t2.tx_path_parent_key = t1.pnc_stdy_smry_id
      )
	 insert into @pnc_unq_trtmt(job_execution_id, rnum, pnc_stdy_smry_id, path_cmb_ids)
	 select @jobExecId, row_number() over(order by depthOrder) as rnum, pnc_stdy_smry_id as pnc_stdy_smry_id, modified_path as modified_path
	 from t1
order by depthOrder;

--update path_unique_treatment for current path unit concpetIds
WITH RECURSIVE t1(combo_id, current_path1, pnc_stdy_smry_id, parent_key, modified_path, modified_concepts, Lvl, depthOrder) AS (
        SELECT 
          rootPath.tx_stg_cmb                            as combo_id
          ,tx_stg_cmb_pth                       as current_path1
          ,pnc_stdy_smry_id                     as pnc_stdy_smry_id
          ,tx_path_parent_key                   as parent_key
          ,cast(tx_stg_cmb as varchar(max))                           as modified_path
          ,cast(comb.concept_ids as varchar(max))                     as modified_concepts
          ,1                                    as Lvl
          ,cast(pnc_stdy_smry_id as varchar(max))+''                 as depthOrder
          FROM @results_schema.pnc_study_summary_path rootPath
          join @pnc_smry_msql_cmb comb
					on cast(rootPath.tx_stg_cmb as bigint) = comb.pnc_tx_stg_cmb_id
					and comb.job_execution_id = @jobExecId
  		WHERE pnc_stdy_smry_id in (select pnc_stdy_smry_id from @results_schema.pnc_study_summary_path
        where 
	        study_id = @studyId
        	and tx_rslt_version = 2
	        and tx_path_parent_key is null)
        UNION ALL
        SELECT 
          t2.tx_stg_cmb                           as combo_id
          ,t2.tx_stg_cmb_pth                       as current_path1
          ,t2.pnc_stdy_smry_id                     as pnc_stdy_smry_id
          ,t2.tx_path_parent_key                   as parent_key
          ,t1.modified_path+'>'+cast(t2.tx_stg_cmb as varchar(max))       as modified_path
--this case clause simplify caltulation of duplicate concept_ids by just assert if concept_ids string already in parents ids ',id1,id2,id3,'
--compare path too: if previous path contains current path combo '>combo_id>', no need to append again
--concepts_ids in #_pnc_smry_msql_cmb.concept_ids should help
          ,
          CASE 
--		     WHEN CHARINDEX(modified_concepts, ',' + comb.concept_ids + ',') > 0
		     WHEN CHARINDEX(',' + comb.concept_ids + ',', modified_concepts) > 0
--    		 or CHARINDEX(modified_path, '>' + t2.tx_stg_cmb + '>') > 0
    		 or CHARINDEX('>' + t2.tx_stg_cmb + '>', modified_path) > 0
		     THEN modified_concepts
    		 ELSE modified_concepts+','+cast(comb.concept_ids as varchar(max))
		  END
												as modified_concepts
          ,lvl+1                                as Lvl
          ,depthOrder+'.'+cast(t2.pnc_stdy_smry_id as varchar(max)) as depthOrder
        FROM (@results_schema.pnc_study_summary_path t2
        join @pnc_smry_msql_cmb comb
        on cast(t2.tx_stg_cmb as bigint) = comb.pnc_tx_stg_cmb_id
        and comb.job_execution_id = @jobExecId), t1
        WHERE  t2.tx_path_parent_key = t1.pnc_stdy_smry_id
      ),
      m1(rnum, combo_id, modified_path, modified_concepts, lvl, current_path1, pnc_stdy_smry_id, parent_key, depthOrder) as
      (
				SELECT row_number() over(order by depthOrder) as rnum, combo_id, modified_path, modified_concepts, lvl, current_path1, pnc_stdy_smry_id, parent_key, depthOrder
				FROM   t1
--sql server change: no order by here... need test...
--order by depthOrder
      )
	  update @pnc_unq_trtmt m
    set path_unique_treatment = m1.modified_concepts
    from t1, m1
    where 
			m1.pnc_stdy_smry_id = m.pnc_stdy_smry_id
		  and m.job_execution_id = @jobExecId;
  
--split conceptIds with "," from #_pnc_unq_trtmt per smry_id and insert order by lastPos (which is used as the order of the concepts in the path)
WITH RECURSIVE splitter_cte(smry_id, origin, pos, lastPos) AS (
      SELECT 
        pnc_stdy_smry_id smry_id,
        path_unique_treatment as origin,
        STRPOS( path_unique_treatment,',') as pos, 
        0 as lastPos
      from @pnc_unq_trtmt
      where job_execution_id = @jobExecId
      UNION ALL
      SELECT 
        smry_id as smry_id,
        origin as origin, 
			 	pos + STRPOS( substring(origin, pos + 1), ',') as pos,  
        pos as lastPos
      FROM splitter_cte
      WHERE pos > 0
      and pos != lastPos
      and pos < length(origin)
    )
insert into @pnc_unq_pth_id (job_execution_id, pnc_tx_smry_id, concept_id, concept_order)
SELECT 
--origin,
	  	@jobExecId,
      smry_id, 
      cast(SUBSTRING(origin, lastPos + 1,
        case when pos = 0 then 80000
        when pos - lastPos - 1 < 0 then length(origin)
        else pos - lastPos  - 1
        end) as bigint) as ids,
--    pos,
--    pos - lastPos -1,
      lastPos
    FROM splitter_cte
--try this (need more test): for order by conceptId for keep uniquenes of concept namesl like "Warfrin,Asparin" or "Asprin,Warfrin"
--    order by smry_id, lastPos) coneptIds;
    order by smry_id, ids;

--sql server workaround.......
delete from @pnc_unq_pth_id 
where
 job_execution_id = @jobExecId
 and (concept_id = 0 or concept_id is null); 

--delete duplicate concept_id per smry_id in the path if it's not the first on in the path by min(concept_order)
delete from @pnc_unq_pth_id 
 where CTID in (select conceptIds.CTID from @pnc_unq_pth_id conceptIds, 
  (select pnc_tx_smry_id, concept_id, min(concept_order) as concept_order
    from @pnc_unq_pth_id
    where job_execution_id = @jobExecId
    group by pnc_tx_smry_id, concept_id
  ) uniqueIds
where
  conceptIds.pnc_tx_smry_id = uniqueIds.pnc_tx_smry_id
  and conceptIds.concept_id = uniqueIds.concept_id
  and conceptIds.concept_order != uniqueIds.concept_order
  and conceptIds.job_execution_id = @jobExecId
)
and job_execution_id = @jobExecId;

--update conceptsArray and conceptName JSON by join concept table
with m1(pnc_tx_smry_id, conceptsArray, conceptsName) as 
(
	select path.pnc_tx_smry_id,
    '[' || string_agg('{"innerConceptName":' || '"' || concepts.concept_name  || '"' || 
    ',"innerConceptId":' || concepts.concept_id::character varying || '}', ',') || ']' conceptsArray,
    string_agg(concepts.concept_name, ',') conceptsName
    from @pnc_unq_pth_id path
    join @cdm_schema.concept concepts
    on path.concept_id = concepts.concept_id
    where path.job_execution_id = @jobExecId
    group by path.pnc_tx_smry_id
)
update @pnc_unq_pth_id m
set conceptsArray = m1.conceptsArray,
  conceptsName = m1.conceptsName
from m1
where
  m.pnc_tx_smry_id = m1.pnc_tx_smry_id
  and m.job_execution_id = @jobExecId;
  
--update @pnc_unq_pth_id 
--set conceptsArray = '[' + substring(conceptsArray, 3, len(conceptsArray))
--where job_execution_id = @jobExecId;

with m1(pnc_tx_smry_id, conceptCount) as 
(
	select path1.pnc_tx_smry_id,
    count(distinct concepts.concept_id) conceptCount
    from @pnc_unq_pth_id path1
    join @cdm_schema.concept concepts
    on path1.concept_id = concepts.concept_id
    where path1.job_execution_id = @jobExecId
    group by path1.pnc_tx_smry_id
)
update @pnc_unq_pth_id m
set concept_count = m1.conceptCount
from m1
where 
  m.pnc_tx_smry_id = m1.pnc_tx_smry_id
  and m.job_execution_id = @jobExecId;


--delete duplicat smry_id rows (now we have smry_id with it's unique concepts conceptsArray and conceptsName)
delete from @pnc_unq_pth_id 
where CTID in (select conceptIds.CTID from @pnc_unq_pth_id conceptIds, 
  (select pnc_tx_smry_id, min(concept_order) as concept_order
    from @pnc_unq_pth_id
    where job_execution_id = @jobExecId
    group by pnc_tx_smry_id
  ) uniqueIds
where
  conceptIds.pnc_tx_smry_id = uniqueIds.pnc_tx_smry_id
  and conceptIds.concept_order != uniqueIds.concept_order
  and conceptIds.job_execution_id = @jobExecId
)
and job_execution_id = @jobExecId;

--------------------version 2----------------------------------------------
--IF OBJECT_ID('tempdb..#pnc_tmp_smry', 'U') IS NOT NULL
--  DROP TABLE #pnc_tmp_smry;

--create table #pnc_tmp_smry(
--	rnum int, 
--	combo_id bigint,
--	current_path varchar(4000),
--	path_seq bigint,
--	avg_duration int, 
--	pt_count int, 
--	pt_percentage numeric(5, 2),
--	concept_names varchar(4000),
--	combo_concepts varchar(4000),
--	lvl int,
--	parent_concept_names varchar(4000),
--	parent_combo_concepts varchar(4000)
--);
----------------------------------version 2 into temp table ------------------------------------------
--    ,uniqueConcepts.conceptsName		  as uniqueConceptsName
--    ,uniqueConcepts.conceptsArray		  as uniqueConceptsArray
--    ,uniqueConcepts.concept_count		  as uniqueConceptCount    
--smry.tx_avg_frm_strt				  as daysFromStart

--    ,tx_stg_avg_gap                       as avg_gap
--    ,NVL(ROUND(tx_stg_avg_gap/tx_stg_avg_dr * 100,2),0)   as gap_pcnt

delete from #pnc_tmp_smry;

WITH RECURSIVE t1( pnc_stdy_smry_id, tx_path_parent_key, lvl, 
	tx_stg_cmb, tx_stg_cmb_pth, tx_seq, tx_stg_avg_dr, tx_stg_cnt, 
	tx_stg_percentage, tx_stg_avg_gap, depthOrder, parent_comb, daysFromStart) AS (
	    SELECT 
           pnc_stdy_smry_id as pnc_stdy_smry_id, tx_path_parent_key as tx_path_parent_key,
           1 AS lvl,
           tx_stg_cmb as tx_stg_cmb, tx_stg_cmb_pth as tx_stg_cmb_pth, tx_seq as tx_seq, tx_stg_avg_dr as tx_stg_avg_dr, tx_stg_cnt as tx_stg_cnt,
           tx_stg_percentage as tx_stg_percentage
           ,tx_stg_avg_gap as  tx_stg_avg_gap
           ,CAST(pnc_stdy_smry_id AS varchar(max))+'' as depthOrder, 
					 CAST(null as character varying) as parent_comb
           ,tx_avg_frm_strt				  as daysFromStart
          FROM   @results_schema.pnc_study_summary_path
        WHERE pnc_stdy_smry_id in (select pnc_stdy_smry_id from @results_schema.pnc_study_summary_path
              where 
              study_id = @studyId
              and tx_rslt_version = 2
              and tx_path_parent_key is null)
        UNION ALL
        SELECT 
              t2.pnc_stdy_smry_id as pnc_stdy_smry_id, t2.tx_path_parent_key as tx_path_parent_key,
              t1.lvl+1 as lvl,
              t2.tx_stg_cmb as tx_stg_cmb, t2.tx_stg_cmb_pth as tx_stg_cmb_pth, t2.tx_seq as tx_seq, t2.tx_stg_avg_dr as tx_stg_avg_dr, 
              t2.tx_stg_cnt as tx_stg_cnt, t2.tx_stg_percentage as tx_stg_percentage
              ,t2.tx_stg_avg_gap as tx_stg_avg_gap 
              ,t1.depthOrder+'.'+CAST(t2.pnc_stdy_smry_id AS varchar(max)) as depthOrder, ISNULL(t1.tx_stg_cmb,'') as parent_comb
              ,t2.tx_avg_frm_strt				  as daysFromStart
        FROM   @results_schema.pnc_study_summary_path t2, t1
        WHERE  t2.tx_path_parent_key = t1.pnc_stdy_smry_id
      )
	  insert into #pnc_tmp_smry (rnum, combo_id, current_path, path_seq, avg_duration, pt_count, pt_percentage,	concept_names, 
	  	combo_concepts, lvl, parent_concept_names, parent_combo_concepts,
		avg_gap, gap_pcnt, uniqueConceptsName, uniqueConceptsArray, uniqueConceptCount, daysFromStart)
      SELECT row_number() over(order by depthOrder) as rnum, 
		cast(tx_stg_cmb as bigint) as combo_id, 
		tx_stg_cmb_pth as current_path,
		tx_seq as path_seq,
		tx_stg_avg_dr as avg_duration,
		tx_stg_cnt as pt_count,
		tx_stg_percentage as pt_percentage,
	    concepts.conceptsName as concept_names,
		concepts.conceptsArray as combo_concepts,
		lvl as lvl,
	    parentConcepts.conceptsName as parent_concept_names,
		parentConcepts.conceptsArray as parent_combo_concepts
    	,tx_stg_avg_gap                       as avg_gap
    	,isnull(ROUND(cast(tx_stg_avg_gap as float)/cast(tx_stg_avg_dr as float) * 100,2),0)   as gap_pcnt
    	,uniqueConcepts.conceptsName		  as uniqueConceptsName
    	,uniqueConcepts.conceptsArray		  as uniqueConceptsArray
    	,uniqueConcepts.concept_count		  as uniqueConceptCount    
		,daysFromStart	   					  as daysFromStart
      FROM t1
	  join @pnc_smry_msql_cmb concepts 
	  on concepts.pnc_tx_stg_cmb_id = cast(t1.tx_stg_cmb as bigint)
  	  and concepts.job_execution_id = @jobExecId
  	  left join @pnc_smry_msql_cmb parentConcepts 
  	  on parentConcepts.pnc_tx_stg_cmb_id = cast(t1.parent_comb as bigint)
  	  and parentConcepts.job_execution_id = @jobExecId
	  join (select pnc_tx_smry_id, conceptsName, conceptsArray, concept_count from @pnc_unq_pth_id where job_execution_id = @jobExecId) uniqueConcepts
	  on uniqueConcepts.pnc_tx_smry_id = t1.pnc_stdy_smry_id  	  
  	  order by depthOrder;

insert into @pnc_indv_jsn(job_execution_id, rnum, table_row_id, rslt_version, JSON)
select @jobExecId as jobExecId, rnum as rnum, table_row_id as table_row_id, rslt_version as rslt_version, JSON as JSON
from
(
select allRoots.rnum as rnum, cast(1 as bigint) as table_row_id, 2 as rslt_version, 
CASE 
--	WHEN rnum = 1 THEN '{"comboId": "root","children": [' + substring(JSON_SNIPPET, 2, len(JSON_SNIPPET))
--    ELSE JSON_SNIPPET
    WHEN rnum = 1 THEN '{"comboId": "root"' 
    + ',"totalCountFirstTherapy":'
    + (select cast(sum(tx_stg_cnt) as varchar(max)) from @results_schema.pnc_study_summary_path 
    	where study_id = @studyId 
        and tx_path_parent_key is null
        and tx_seq = 1
        and tx_rslt_version = 2)
    + ',"totalCohortCount":'
    + (select cast(count( distinct subject_id) as varchar(max)) from @ohdsi_schema.cohort
--        where cohort_definition_id = (select cohort_definition_id from 
--        @results_schema.panacea_study 
--        where study_id = @studyId)
  			where cohort_definition_id = @cohort_definition_id )
    + ',"firstTherapyPercentage":'
    + (select cast(isnull(ROUND(cast(firstCount.firstCount as float)/cast(cohortTotal.cohortTotal as float) * 100,2),0) as varchar(max)) firstTherrapyPercentage from 
        (select sum(tx_stg_cnt) as firstCount from @results_schema.pnc_study_summary_path 
        where study_id = @studyId 
        and tx_path_parent_key is null
        and tx_seq = 1
        and tx_rslt_version = 2) firstCount,  
        (select count( distinct subject_id) as cohortTotal from @ohdsi_schema.cohort
--        where cohort_definition_id = (select cohort_definition_id from 
--        @results_schema.panacea_study 
--        where study_id = @studyId)
  			where cohort_definition_id = @cohort_definition_id ) cohortTotal)
    +',"children": [' 
    + substring(JSON_SNIPPET, 2, len(JSON_SNIPPET))
    ELSE JSON_SNIPPET
END
as JSON
from 
(
  select 
  rnum as rnum,
  CASE 
    WHEN Lvl = 1 THEN ',{'
    WHEN Lvl - LAG(Lvl) OVER (order by rnum) = 1 THEN ',"children" : [{' 
    ELSE ',{' 
  END 
  + ' "comboId" : ' + cast(combo_id as varchar(max))+ ' '
  + ' ,"conceptName" : "' + concept_names + '" '  
  + ' ,"patientCount" : ' + cast(pt_count as varchar(max))+ ' '
  + ' ,"percentage" : "' + cast(pt_percentage as varchar(max)) + '" '  
  + ' ,"avgDuration" : ' + cast(avg_duration as varchar(max)) + ' '
  + ' ,"avgGapDay" : ' + cast(avg_gap as varchar(max)) + ' '
  + ' ,"gapPercent" : "' + cast(gap_pcnt as varchar(max))+ '" '
  + ' ,"daysFromCohortStart" : ' + cast(daysFromStart as varchar(max)) + ' '
  + ',"concepts" : ' + combo_concepts 
  + ',"uniqueConceptsName" : "' + uniqueConceptsName + '" '
  + ',"uniqueConceptsArray" : ' + uniqueConceptsArray
  + ' ,"uniqueConceptCount" : ' + cast(uniqueConceptCount as varchar(max))+ ' '
  + CASE WHEN Lvl > 1 THEN    
        ',"parentConcept": { "parentConceptName": "' || parent_concept_names || '", '  
        || '"parentConcepts":' || parent_combo_concepts   || '}'
    ELSE ''
    END 
   || CASE WHEN LEAD(Lvl, 1, 1) OVER (order by rnum) - Lvl <= 0
--sql server: simplify without rpad... no formatting here
--     THEN '}' || rpad( ' ', 1+ (-2 * (LEAD(Lvl, 1, 1) OVER (order by rnum) - Lvl)), ']}' )
--     THEN '}' + ']}'
     THEN '}' || repeat(']}', lvl - LEAD(Lvl, 1, 1) OVER (order by rnum))
     ELSE ''
  END as JSON_SNIPPET
from #pnc_tmp_smry connect_by_query
) allRoots
union all
select lastRow.rnum as rnum, lastRow.table_row_id as table_row_id, 2 as rslt_version, ']}' as JSON from (
	select distinct 1000000000 as rnum, 1 as table_row_id from @results_schema.pnc_study_summary_path) lastRow
) allRootsAndLastPadding;
  	  
-------------------------------------version 2 into summary table-------------------------------------
--merge into @results_schema.pnc_study_summary m
--using
--(
--	select 1 as rnum, 2 as table_row_id, json from (
--	select distinct row_number() over(order by t1.rnum) as rnum, t1.table_row_id as table_row_id,
--  	STUFF((SELECT distinct '' + t2.JSON
--    	     from @pnc_indv_jsn t2
--        	 where t1.table_row_id = t2.table_row_id
--        	 and t2.rslt_version = 2
--			 and t2.job_execution_id = @jobExecId
--            	FOR XML PATH(''), TYPE
--	            ).value('.', 'NVARCHAR(MAX)') 
--    	    ,1,0,'') as json
--	from @pnc_indv_jsn t1
--	where t1.rslt_version = 2
--	and t1.job_execution_id = @jobExecId) oneJson
--) m1
--on
--(
--  m.study_id = @studyId and m.source_id = @sourceId
--)
--WHEN MATCHED THEN UPDATE SET m.study_results_2 = m1.json, m.last_update_time = CURRENT_TIMESTAMP;
update @results_schema.pnc_study_summary set study_results_2 =
(select JSON from (
	select distinct individualResult.table_row_id,
		string_agg(JSON::character varying, '')
		 AS JSON
	from (select rnum, table_row_id, rslt_version, JSON
		from @pnc_indv_jsn t1
		where t1.rslt_version = 2
		and t1.job_execution_id = @jobExecId
		and t1.table_row_id = 1
	) individualResult
	group by individualResult.table_row_id
) mergeJsonRowsTable), 
last_update_time = CURRENT_TIMESTAMP 
where study_id = @studyId ;
