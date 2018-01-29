@tempTableCreationSummary_oracle

--recreate #_pnc_smry_msql_cmb and #_pnc_indv_jsn for making the filtered version tasklet run (they are not created from generateSummary script)
---------------collapse/merge multiple rows to concatenate strings (JSON string for conceptsArrary and conceptsName) ------

insert into @pnc_smry_msql_cmb (job_execution_id, pnc_tx_stg_cmb_id, concept_ids, conceptsArray, conceptsName)
select @jobExecId, comb_id, concept_ids, conceptsArray, conceptsName 
from
(
	select comb.pnc_tx_stg_cmb_id comb_id,
    LISTAGG(combMap.concept_id, ',') WITHIN GROUP (order by comb.pnc_tx_stg_cmb_id) over (PARTITION BY comb.pnc_tx_stg_cmb_id) concept_ids,
    '[' || LISTAGG('{"innerConceptName":' || '"' || combMap.concept_name  || '"' || 
    ',"innerConceptId":' || combMap.concept_id || '}', ',') WITHIN GROUP (order by comb.pnc_tx_stg_cmb_id) over (PARTITION BY comb.pnc_tx_stg_cmb_id) || ']' conceptsArray,
    LISTAGG(combMap.concept_name, ',') WITHIN GROUP (order by comb.pnc_tx_stg_cmb_id) over (PARTITION BY comb.pnc_tx_stg_cmb_id) conceptsName
    from @results_schema.pnc_tx_stage_combination comb
    join @results_schema.pnc_tx_stage_combination_map combMap 
    on comb.pnc_tx_stg_cmb_id = combmap.pnc_tx_stg_cmb_id
    where comb.study_id = @studyId
--    group by comb.pnc_tx_stg_cmb_id
) studyCombo;

-----------------generate rows of JSON (based on hierarchical data, each path is a row) insert into temp table----------------------
-------------------------filtering based on filter out conditions -----------------------
insert into @pnc_smrypth_fltr 
select @jobExecId, pnc_stdy_smry_id, study_id, @sourceId, tx_path_parent_key, tx_stg_cmb, tx_stg_cmb_pth,
    tx_seq,
    tx_stg_cnt,
    tx_stg_percentage,
    tx_stg_avg_dr,
    tx_stg_avg_gap,
    tx_avg_frm_strt,
    tx_rslt_version 
    from @results_schema.pnc_study_summary_path
        where 
        study_id = @studyId
        and tx_rslt_version = 2;

----------- delete rows that do not qualify the conditions for fitlering out-----------
delete from @pnc_smrypth_fltr where pnc_stdy_smry_id not in (
    select pnc_stdy_smry_id from #_pnc_smrypth_fltr qualified
--TODO!!!!!! change this with real condition string
--    where tx_stg_avg_dr >= 50);
--    where tx_stg_avg_gap < 150
@constraintSql
)
and job_execution_id = @jobExecId;


--table to hold null parent ids (which have been deleted from #_pnc_smrypth_fltr as not qualified rows) and all their ancestor_id with levels
insert into @pnc_smry_ancstr
select @jobExecId, nullParentKey, pnc_stdy_smry_id, realLevel
from (
  select validancestor.nullParentKey, validancestor.ancestorlevel, 
  case 
      when path.pnc_stdy_smry_id is not null then validancestor.ancestorlevel
      when path.pnc_stdy_smry_id is null then 1000000000
    end as realLevel,
  validancestor.parentid, path.pnc_stdy_smry_id
  from @pnc_smrypth_fltr path
  right join
  (select smry.tx_path_parent_key nullParentKey, nullParentAncestors.l ancestorLevel, nullParentAncestors.parent parentId from @pnc_smrypth_fltr smry
    join
    (SELECT pnc_stdy_smry_id, ancestor AS parent, l
      FROM
      (
        SELECT pnc_stdy_smry_id, tx_path_parent_key, LEVEL-1 l, 
        connect_by_root pnc_stdy_smry_id ancestor
        FROM @results_schema.pnc_study_summary_path
        where 
        study_id = @studyId
        and tx_rslt_version = 2
      	CONNECT BY PRIOR pnc_stdy_smry_id = tx_path_parent_key
      ) t
      WHERE t.ancestor <> t.pnc_stdy_smry_id
      and t.pnc_stdy_smry_id in 
      (select tx_path_parent_key from @pnc_smrypth_fltr where tx_path_parent_key not in (select PNC_STDY_SMRY_ID from @pnc_smrypth_fltr
      	where job_execution_id = @jobExecId)
      	and job_execution_id = @jobExecId)
    ) nullParentAncestors
    on smry.tx_path_parent_key = nullParentAncestors.pnc_stdy_smry_id
    where smry.job_execution_id = @jobExecId) validAncestor
  on path.pnc_stdy_smry_id = validAncestor.parentId
  where path.job_execution_id = @jobExecId);

--update null parent key in #_pnc_smrypth_fltr with valid ancestor id which exists in #_pnc_smrypth_fltr or null (null is from level set to 1000000 from table of #_pnc_smry_ancstr)
merge into @pnc_smrypth_fltr m
using
  (
    select path.pnc_stdy_smry_id, updateParent.pnc_ancestor_id from @pnc_smrypth_fltr path,
    (select pnc_stdy_parent_id, pnc_ancestor_id
    	from (select pnc_stdy_parent_id, pnc_ancestor_id, reallevel, 
    	row_number() over (partition by pnc_stdy_parent_id order by reallevel) rn
    	from @pnc_smry_ancstr where job_execution_id = @jobExecId)
    where rn = 1) updateParent
    where path.tx_path_parent_key = updateParent.pnc_stdy_parent_id
    and path.job_execution_id = @jobExecId
  ) m1
  on
  (
     m.pnc_stdy_smry_id = m1.pnc_stdy_smry_id
     and m.job_execution_id = @jobExecId
  )
  WHEN MATCHED then update set m.tx_path_parent_key = m1.pnc_ancestor_id;
  
--update path (tx_stg_cmb_pth with modified_path in the recursive query) in #_pnc_smrypth_fltr
merge into @pnc_smrypth_fltr m
using
(
  WITH t1(job_execution_id, combo_id, current_path, pnc_stdy_smry_id, parent_key, modified_path, Lvl, depthOrder) AS (
        SELECT
           job_execution_id						as job_execution_id
          ,tx_stg_cmb                           as combo_id
          ,tx_stg_cmb_pth                       as current_path
          ,pnc_stdy_smry_id                     as pnc_stdy_smry_id
          ,tx_path_parent_key                   as parent_key
          ,tx_stg_cmb                           as modified_path
          ,1                                    as Lvl
          ,pnc_stdy_smry_id||''                 as depthOrder
          FROM   @pnc_smrypth_fltr
        WHERE tx_path_parent_key is null
        and job_execution_id = @jobExecId
        UNION ALL
        SELECT 
           t2.job_execution_id 					   as job_execution_id
		  ,t2.tx_stg_cmb                           as combo_id
          ,t2.tx_stg_cmb_pth                       as current_path
          ,t2.pnc_stdy_smry_id                     as pnc_stdy_smry_id
          ,t2.tx_path_parent_key                   as parent_key
          ,modified_path||'>'||t2.tx_stg_cmb       as modified_path
          ,lvl+1                                as Lvl
          ,depthOrder||'.'||t2.pnc_stdy_smry_id as depthOrder
        FROM    @pnc_smrypth_fltr t2, t1
        WHERE  t2.tx_path_parent_key = t1.pnc_stdy_smry_id
        and t2.job_execution_id = @jobExecId
      )
      SELECT row_number() over(order by depthOrder) as rnum, combo_id, modified_path, lvl, current_path, pnc_stdy_smry_id, parent_key, depthOrder
      FROM   t1
      order by depthOrder
) m1
on
(
  m.pnc_stdy_smry_id = m1.pnc_stdy_smry_id
  and m.job_execution_id = @jobExecId
)
WHEN MATCHED then update set m.tx_stg_cmb_pth = m1.modified_path;


------------------try unique path here-----------------
insert into @pnc_unq_trtmt(job_execution_id, rnum, pnc_stdy_smry_id, path_cmb_ids)
select @jobExecId, rnum, pnc_stdy_smry_id, modified_path
from (
WITH t1(job_execution_id, combo_id, current_path, pnc_stdy_smry_id, parent_key, modified_path, Lvl, depthOrder) AS (
        SELECT
          job_execution_id						as job_execution_id
          ,tx_stg_cmb                           as combo_id
          ,tx_stg_cmb_pth                       as current_path
          ,pnc_stdy_smry_id                     as pnc_stdy_smry_id
          ,tx_path_parent_key                   as parent_key
          ,tx_stg_cmb                           as modified_path
          ,1                                    as Lvl
          ,pnc_stdy_smry_id||''                 as depthOrder
          FROM   @pnc_smrypth_fltr
        WHERE tx_path_parent_key is null
        and job_execution_id = @jobExecId
        UNION ALL
        SELECT 
          t2.job_execution_id	  				   as job_execution_id
          ,t2.tx_stg_cmb                           as combo_id
          ,t2.tx_stg_cmb_pth                       as current_path
          ,t2.pnc_stdy_smry_id                     as pnc_stdy_smry_id
          ,t2.tx_path_parent_key                   as parent_key
          ,modified_path||'>'||t2.tx_stg_cmb       as modified_path
          ,lvl+1                                as Lvl
          ,depthOrder||'.'||t2.pnc_stdy_smry_id as depthOrder
        FROM    @pnc_smrypth_fltr t2, t1
        WHERE  t2.tx_path_parent_key = t1.pnc_stdy_smry_id
        and t2.job_execution_id = @jobExecId
      )
      SELECT row_number() over(order by depthOrder) as rnum, combo_id, modified_path, lvl, current_path, pnc_stdy_smry_id, parent_key, depthOrder
      FROM   t1
order by depthOrder
);

--update path_unique_treatment for current path unit concpetIds
merge into @pnc_unq_trtmt m
using
(
WITH t1(job_execution_id, combo_id, current_path, pnc_stdy_smry_id, parent_key, modified_path, modified_concepts, Lvl, depthOrder) AS (
        SELECT 
          rootPath.job_execution_id						as job_execution_id
          ,tx_stg_cmb                           as combo_id
          ,tx_stg_cmb_pth                       as current_path
          ,pnc_stdy_smry_id                     as pnc_stdy_smry_id
          ,tx_path_parent_key                   as parent_key
          ,tx_stg_cmb                           as modified_path
          ,comb.concept_ids                     as modified_concepts
          ,1                                    as Lvl
          ,pnc_stdy_smry_id||''                 as depthOrder
          FROM @pnc_smrypth_fltr rootPath
          join @pnc_smry_msql_cmb comb
          on rootPath.tx_stg_cmb = comb.pnc_tx_stg_cmb_id
          and comb.job_execution_id = @jobExecId
        WHERE tx_path_parent_key is null
        and rootPath.job_execution_id = @jobExecId
        UNION ALL
        SELECT 
          t2.job_execution_id					   as job_execution_id
          ,t2.tx_stg_cmb                           as combo_id
          ,t2.tx_stg_cmb_pth                       as current_path
          ,t2.pnc_stdy_smry_id                     as pnc_stdy_smry_id
          ,t2.tx_path_parent_key                   as parent_key
          ,modified_path||'>'||t2.tx_stg_cmb       as modified_path
--this case clause simplify caltulation of duplicate concept_ids by just assert if concept_ids string already in parents ids ',id1,id2,id3,'
--compare path too: if previous path contains current path combo '>combo_id>', no need to append again
--concepts_ids in #_pnc_smry_msql_cmb.concept_ids should help
          ,
          CASE 
		    WHEN instr(modified_concepts, ',' || comb.concept_ids || ',') > 0
		    or instr(modified_path, '>' || t2.tx_stg_cmb || '>') > 0
		    THEN modified_concepts
    		ELSE modified_concepts||','||comb.concept_ids
		  END
												as modified_concepts
          ,lvl+1                                as Lvl
          ,depthOrder||'.'||t2.pnc_stdy_smry_id as depthOrder
        FROM (#_PNC_SMRYPTH_FLTR t2
        join @pnc_smry_msql_cmb comb
        on t2.tx_stg_cmb = comb.pnc_tx_stg_cmb_id
        and comb.job_execution_id = @jobExecId
        and t2.job_execution_id = @jobExecId), t1
        WHERE  t2.tx_path_parent_key = t1.pnc_stdy_smry_id
        and t2.job_execution_id = @jobExecId
      )
      SELECT row_number() over(order by depthOrder) as rnum, combo_id, modified_path, modified_concepts, lvl, current_path, pnc_stdy_smry_id, parent_key, depthOrder
      FROM   t1
order by depthOrder
) m1
on
(
  m1.pnc_stdy_smry_id = m.pnc_stdy_smry_id
  and m.job_execution_id = @jobExecId
)
WHEN MATCHED then update set m.path_unique_treatment = m1.modified_concepts;

--split conceptIds with "," from #_pnc_unq_trtmt per smry_id and insert order by lastPos (which is used as the order of the concepts in the path)
insert into @pnc_unq_pth_id (job_execution_id, pnc_tx_smry_id, concept_id, concept_order)
select @jobExecId, smry_id, ids, lastPos from
    (WITH splitter_cte(job_execution_id, smry_id, origin, pos, lastPos) AS (
      SELECT 
        job_execution_id job_execution_id,
        pnc_stdy_smry_id smry_id,
        path_unique_treatment as origin,
        instr(path_unique_treatment, ',') as pos, 
        0 as lastPos
      from @pnc_unq_trtmt
      where job_execution_id = @jobExecId
      UNION ALL
      SELECT 
        job_execution_id as job_execution_id,
        smry_id as smry_id,
        origin as origin, 
        instr(origin, ',', pos + 1) as pos, 
        pos as lastPos
      FROM splitter_cte
      WHERE pos > 0
      and job_execution_id = @jobExecId
    )
    SELECT 
      smry_id, 
      origin, 
      SUBSTR(origin, lastPos + 1,
        case when pos = 0 then 80000
        else pos - lastPos -1 end) as ids,
      pos,
      lastPos
    FROM splitter_cte
    where job_execution_id = @jobExecId
--try this (need more test): for order by conceptId for keep uniquenes of concept namesl like "Warfrin,Asparin" or "Asprin,Warfrin"
--    order by smry_id, lastPos) coneptIds;
    order by smry_id, ids) coneptIds;


--delete duplicate concept_id per smry_id in the path if it's not the first on in the path by min(concept_order)
delete from @pnc_unq_pth_id 
where rowid in (select conceptIds.rowid from @pnc_unq_pth_id conceptIds, 
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
);

--update conceptsArray and conceptName JSON by join concept table
merge into @pnc_unq_pth_id m
using
(
	select path.pnc_tx_smry_id,
    '[' || LISTAGG('{"innerConceptName":' || '"' || concepts.concept_name  || '"' || 
    ',"innerConceptId":' || concepts.concept_id || '}', ',') WITHIN GROUP (order by path.pnc_tx_smry_id) || ']' conceptsArray,
    LISTAGG(concepts.concept_name, ',') WITHIN GROUP (order by path.pnc_tx_smry_id) conceptsName
    , count(distinct concepts.concept_id) conceptCount
    from @pnc_unq_pth_id path
    join @cdm_schema.concept concepts
    on path.concept_id = concepts.concept_id
    where path.job_execution_id = @jobExecId
    group by path.pnc_tx_smry_id
) m1
on
(
  m.pnc_tx_smry_id = m1.pnc_tx_smry_id
  and m.job_execution_id = @jobExecId
)
WHEN MATCHED then update set m.conceptsArray = m1.conceptsArray,
 m.conceptsName = m1.conceptsName
 ,m.concept_count = m1.conceptCount;

--delete duplicat smry_id rows (now we have smry_id with it's unique concepts conceptsArray and conceptsName)
delete from @pnc_unq_pth_id 
where rowid in (select conceptIds.rowid from @pnc_unq_pth_id conceptIds, 
  (select pnc_tx_smry_id, min(concept_order) as concept_order
    from @pnc_unq_pth_id
    where job_execution_id = @jobExecId
    group by pnc_tx_smry_id
  ) uniqueIds
where
  conceptIds.pnc_tx_smry_id = uniqueIds.pnc_tx_smry_id
  and conceptIds.concept_order != uniqueIds.concept_order
  and conceptIds.job_execution_id = @jobExecId
);

------------------------version 2 of filtered JSON into temp table-----------------
insert into @pnc_indv_jsn(job_execution_id, rnum, table_row_id, JSON)
select @jobExecId, rnum, table_row_id, JSON 
from
(
select allRoots.rnum rnum, 1 table_row_id,
CASE 
--    WHEN rnum = 1 THEN '{"comboId": "root","children": [' || substr(JSON_SNIPPET, 2, length(JSON_SNIPPET))
    WHEN rnum = 1 THEN '{"comboId": "root"' 
    || ',"totalCountFirstTherapy":'
    || (select sum(tx_stg_cnt) from #_pnc_smrypth_fltr 
        where tx_path_parent_key is null
        and tx_seq = 1)
    || ',"totalCohortCount":'
    || (select count( distinct subject_id) from @ohdsi_schema.cohort
--        where cohort_definition_id = (select cohort_definition_id from 
--        @results_schema.panacea_study 
--        where study_id = @studyId)
			where cohort_definition_id = @cohort_definition_id)
    || ',"firstTherapyPercentage":'
    || (select NVL(ROUND(firstCount.firstCount/cohortTotal.cohortTotal * 100,2),0) firstTherrapyPercentage from 
        (select sum(tx_stg_cnt) as firstCount from #_pnc_smrypth_fltr 
        where tx_path_parent_key is null
        and tx_seq = 1) firstCount,  
        (select count( distinct subject_id) as cohortTotal from @ohdsi_schema.cohort
--        where cohort_definition_id = (select cohort_definition_id from 
--        @results_schema.panacea_study 
--        where study_id = @studyId)
				where cohort_definition_id = @cohort_definition_id) cohortTotal)
    ||',"children": [' 
    || substr(JSON_SNIPPET, 2, length(JSON_SNIPPET))
    ELSE JSON_SNIPPET
END
as JSON
from 
(WITH connect_by_query as (
		select  
        individualPathNoParentConcepts.rnum                               as rnum
      ,individualPathNoParentConcepts.combo_id                            as combo_id
      ,individualPathNoParentConcepts.current_path                        as current_path
      ,individualPathNoParentConcepts.path_seq                            as path_seq
      ,individualPathNoParentConcepts.avg_duration                        as avg_duration
	  ,individualPathNoParentConcepts.avg_gap                       	  as avg_gap
      ,individualPathNoParentConcepts.gap_pcnt							  as gap_pcnt
      ,individualPathNoParentConcepts.pt_count                            as pt_count
      ,individualPathNoParentConcepts.pt_percentage                       as pt_percentage
      ,individualPathNoParentConcepts.concept_names                       as concept_names
      ,individualPathNoParentConcepts.combo_concepts                      as combo_concepts
      ,individualPathNoParentConcepts.Lvl                                 as Lvl
     ,parentConcepts.conceptsName                                         as parent_concept_names
     ,parentConcepts.conceptsArray                                        as parent_combo_concepts
     ,individualPathNoParentConcepts.uniqueConceptsName					  as uniqueConceptsName
     ,individualPathNoParentConcepts.uniqueConceptsArray				  as uniqueConceptsArray
     ,individualPathNoParentConcepts.uniqueConceptCount					  as uniqueConceptCount
     ,individualPathNoParentConcepts.daysFromStart						  as daysFromStart
    from 
    (SELECT 
     ROWNUM                               as rnum
    ,tx_stg_cmb                           as combo_id
    ,tx_stg_cmb_pth                       as current_path
    ,tx_seq                               as path_seq
    ,tx_stg_avg_dr                        as avg_duration
    ,tx_stg_avg_gap                       as avg_gap
    ,NVL(ROUND(tx_stg_avg_gap/tx_stg_avg_dr * 100,2),0)   as gap_pcnt
    ,tx_stg_cnt                           as pt_count
    ,tx_stg_percentage                    as pt_percentage
    ,concepts.conceptsName                as concept_names
    ,concepts.conceptsArray               as combo_concepts
    ,LEVEL                                as Lvl
    ,pnc_stdy_smry_id                     as self_id
    ,tx_path_parent_key                   as parent_id
    ,prior tx_stg_cmb                     as parent_comb
    ,uniqueConcepts.conceptsName		  as uniqueConceptsName
    ,uniqueConcepts.conceptsArray		  as uniqueConceptsArray
    ,uniqueConcepts.concept_count		  as uniqueConceptCount
    ,smry.tx_avg_frm_strt				  as daysFromStart
  FROM @pnc_smrypth_fltr smry
  join @pnc_smry_msql_cmb concepts
  on concepts.pnc_tx_stg_cmb_id = smry.tx_stg_cmb
  and concepts.job_execution_id = @jobExecId
  join (select pnc_tx_smry_id, conceptsName, conceptsArray， concept_count from @pnc_unq_pth_id
  where job_execution_id = @jobExecId) uniqueConcepts
  on uniqueConcepts.pnc_tx_smry_id = smry.pnc_stdy_smry_id
  and smry.job_execution_id = @jobExecId
  START WITH pnc_stdy_smry_id in (select pnc_stdy_smry_id from @pnc_smrypth_fltr
        where 
--        study_id = 19
--        and source_id = 2
--        and tx_rslt_version = 2
        tx_path_parent_key is null
        and job_execution_id = @jobExecId)
  CONNECT BY PRIOR pnc_stdy_smry_id = tx_path_parent_key
  ORDER SIBLINGS BY pnc_stdy_smry_id
  ) individualPathNoParentConcepts
  left join @pnc_smry_msql_cmb parentConcepts
  on parentConcepts.pnc_tx_stg_cmb_id = individualPathNoParentConcepts.parent_comb
  and parentConcepts.job_execution_id = @jobExecId
  order by rnum
)
select 
  rnum rnum,
  CASE 
    WHEN Lvl = 1 THEN ',{'
    WHEN Lvl - LAG(Lvl) OVER (order by rnum) = 1 THEN ',"children" : [{' 
    ELSE ',{' 
  END 
  || ' "comboId" : ' || combo_id || ' '
  || ' ,"conceptName" : "' || concept_names || '" '  
  || ' ,"patientCount" : ' || pt_count || ' '
  || ' ,"percentage" : "' || pt_percentage || '" '  
  || ' ,"avgDuration" : ' || avg_duration || ' '
  || ' ,"avgGapDay" : ' || avg_gap || ' '
  || ' ,"gapPercent" : "' || gap_pcnt || '" '
  || ' ,"daysFromCohortStart" : ' || daysFromStart || ' '
  || ',"concepts" : ' || combo_concepts
  || ',"uniqueConceptsName" : "' || uniqueConceptsName || '" '
  || ',"uniqueConceptsArray" : ' || uniqueConceptsArray
  || ' ,"uniqueConceptCount" : ' || uniqueConceptCount || ' '
  || CASE WHEN Lvl > 1 THEN    
        ',"parentConcept": { "parentConceptName": "' || parent_concept_names || '", '  
        || '"parentConcepts":' || parent_combo_concepts   || '}'
     ELSE  NULL
     END 
  || CASE WHEN LEAD(Lvl, 1, 1) OVER (order by rnum) - Lvl <= 0 
     THEN '}' || rpad( ' ', 1+ (-2 * (LEAD(Lvl, 1, 1) OVER (order by rnum) - Lvl)), ']}' )
     ELSE NULL 
  END as JSON_SNIPPET
from connect_by_query
order by rnum) allRoots
union all
select rnum as rnum, table_row_id as table_row_id, ']}' as JSON from (
--	select distinct 1/0F as rnum, 1 as table_row_id from #_pnc_smrypth_fltr)
	select distinct 1000000000 as rnum, 1 as table_row_id from @pnc_smrypth_fltr)
) individualJsonRows;

------------------------version 2 of filtered JSON into summary table-----------------
update @results_schema.pnc_study_summary set study_results_filtered = 
(select JSON from (
	select individualResult.table_row_id,
		DBMS_XMLGEN.CONVERT (
     	EXTRACT(
       		xmltype('<?xml version="1.0"?><document>' ||
               XMLAGG(
                 XMLTYPE('<V>' || DBMS_XMLGEN.CONVERT(JSON)|| '</V>')
                 order by rnum).getclobval() || '</document>'),
               '/document/V/text()').getclobval(),1) AS JSON
	from (select rnum, table_row_id, json
		from @pnc_indv_jsn t1
		where job_execution_id = @jobExecId
	) individualResult
	group by individualResult.table_row_id
) mergeJsonRowsTable ),
last_update_time = CURRENT_TIMESTAMP 
where study_id = @studyId ;


IF OBJECT_ID('tempdb..#_pnc_smrypth_fltr', 'U') IS NOT NULL
  DROP TABLE #_pnc_smrypth_fltr;
IF OBJECT_ID('tempdb..#_pnc_smry_ancstr', 'U') IS NOT NULL
  DROP TABLE #_pnc_smry_ancstr;
IF OBJECT_ID('tempdb..#_pnc_ptsq_ct', 'U') IS NOT NULL
  DROP TABLE #_pnc_ptsq_ct;
IF OBJECT_ID('tempdb..#_pnc_ptstg_ct', 'U') IS NOT NULL
  DROP TABLE #_pnc_ptstg_ct;
IF OBJECT_ID('tempdb..#_pnc_tmp_cmb_sq_ct', 'U') IS NOT NULL
  DROP TABLE #_pnc_tmp_cmb_sq_ct;
IF OBJECT_ID('tempdb..#_pnc_indv_jsn', 'U') IS NOT NULL
  DROP TABLE #_pnc_indv_jsn;
IF OBJECT_ID('tempdb..#_pnc_smry_msql_cmb', 'U') IS NOT NULL
  DROP TABLE #_pnc_smry_msql_cmb;
IF OBJECT_ID('tempdb..#_pnc_unq_trtmt', 'U') IS NOT NULL
  DROP TABLE #_pnc_unq_trtmt;
IF OBJECT_ID('tempdb..#_pnc_unq_pth_id', 'U') IS NOT NULL
  DROP TABLE #_pnc_unq_pth_id;
IF OBJECT_ID('tempdb..#_pnc_tmp_mssql_seq_id', 'U') IS NOT NULL
  DROP TABLE #_pnc_tmp_mssql_seq_id;
