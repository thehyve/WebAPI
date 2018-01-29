--delete from @pnc_smry_msql_cmb where job_execution_id = @jobExecId;
--delete from @pnc_indv_jsn where job_execution_id = @jobExecId;
delete from @pnc_unq_trtmt where job_execution_id = @jobExecId;
delete from @pnc_unq_pth_id where job_execution_id = @jobExecId;
delete from @pnc_smrypth_fltr where job_execution_id = @jobExecId;
delete from @pnc_smry_ancstr where job_execution_id = @jobExecId;

--delete from #pnc_tmp_smry;
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

--recreate #_pnc_smry_msql_cmb and #_pnc_smry_msql_cmb for making the filtered version tasklet run (they are not created from generateSummary script)
---------------ms sql collapse/merge multiple rows to concatenate strings (JSON string for conceptsArrary and conceptsName) ------
-- refactored to remove not supported SqlRender "for xml path()" look into PanaceaPatientDrugComboTasklet.insertPncSmryMsqlCmb
--insert into @pnc_smry_msql_cmb (job_execution_id, pnc_tx_stg_cmb_id, concept_ids, conceptsArray, conceptsName)
--select @jobExecId, comb_id, concept_ids, conceptsArray, conceptsName 
--from
--(
--select distinct @jobExecId as jobExecId, tab1.pnc_tx_stg_cmb_id as comb_id,
--  STUFF((SELECT distinct CAST(tab2.concept_id as varchar(max)) + ','
--         from 
--         (select comb.study_id as study_id, comb.pnc_tx_stg_cmb_id as pnc_tx_stg_cmb_id, combmap.concept_id as concept_id, combmap.concept_name as concept_name
--            from @results_schema.pnc_tx_stage_combination comb
--            join @results_schema.pnc_tx_stage_combination_map combMap 
--              on comb.pnc_tx_stg_cmb_id = combmap.pnc_tx_stg_cmb_id
--              where comb.study_id = @studyId
--         ) tab2
--         where tab1.pnc_tx_stg_cmb_id = tab2.pnc_tx_stg_cmb_id
--            FOR XML PATH(''), TYPE
--            ).value('.', 'NVARCHAR(MAX)') 
--        ,1,0,'') as concept_ids,
--  '[' + STUFF((SELECT distinct ',{"innerConceptName":' + '"' + tab2.concept_name + '"' +   
--    ',"innerConceptId":' + convert(varchar, tab2.concept_id) + '}'
--         from 
--         (select comb.study_id as study_id, comb.pnc_tx_stg_cmb_id as pnc_tx_stg_cmb_id, combmap.concept_id as concept_id, combmap.concept_name as concept_name
--            from @results_schema.pnc_tx_stage_combination comb
--            join @results_schema.pnc_tx_stage_combination_map combMap 
--              on comb.pnc_tx_stg_cmb_id = combmap.pnc_tx_stg_cmb_id
--              where comb.study_id = @studyId
--         ) tab2
--         where tab1.pnc_tx_stg_cmb_id = tab2.pnc_tx_stg_cmb_id
--            FOR XML PATH(''), TYPE
--            ).value('.', 'NVARCHAR(MAX)') 
--        ,1,0,'') +  ']' as conceptsArray,
--  STUFF((SELECT distinct tab2.concept_name + ','
--         from 
--         (select comb.study_id as study_id, comb.pnc_tx_stg_cmb_id as pnc_tx_stg_cmb_id, combmap.concept_id as concept_id, combmap.concept_name as concept_name
--            from @results_schema.pnc_tx_stage_combination comb
--            join @results_schema.pnc_tx_stage_combination_map combMap 
--              on comb.pnc_tx_stg_cmb_id = combmap.pnc_tx_stg_cmb_id
--              where comb.study_id = @studyId
--         ) tab2
--         where tab1.pnc_tx_stg_cmb_id = tab2.pnc_tx_stg_cmb_id
--            FOR XML PATH(''), TYPE
--            ).value('.', 'NVARCHAR(MAX)') 
--        ,1,0,'') as conceptsName
--from (select comb.study_id as study_id, comb.pnc_tx_stg_cmb_id as pnc_tx_stg_cmb_id, combmap.concept_id as concept_id, combmap.concept_name as concept_name
--        from @results_schema.pnc_tx_stage_combination comb
--        join @results_schema.pnc_tx_stage_combination_map combMap 
--          on comb.pnc_tx_stg_cmb_id = combmap.pnc_tx_stg_cmb_id
--          where comb.study_id = @studyId) tab1
--) studyCombo;
--
--update @pnc_smry_msql_cmb 
--set conceptsArray = '[' + substring(conceptsArray, 3, len(conceptsArray))
--where job_execution_id = @jobExecId;

-----------------generate rows of JSON (based on hierarchical data, without using oracle connect/level, each path is a row) insert into temp table----------------------
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
    select pnc_stdy_smry_id from @pnc_smrypth_fltr qualified
--TODO!!!!!! change this with real condition string
--    where tx_stg_avg_dr >= 50);
--    where tx_stg_avg_gap < 150
@constraintSql
)
and job_execution_id = @jobExecId;


--table to hold null parent ids (which have been deleted from #_pnc_smrypth_fltr as not qualified rows) and all their ancestor_id with levels
--refactor big hierarchical recursive CTE
--with AncestryTree (pnc_stdy_smry_id, ancestor, lvl) as (
--  		  	select 
--		      pnc_stdy_smry_id as pnc_stdy_smry_id,
--    		  tx_path_parent_key as ancestor, 
--    		  0 as lvl
--	  	  	  FROM @results_schema.pnc_study_summary_path
--  			  where
--				study_id = @studyId
--        		and tx_rslt_version = 2
--    			and tx_path_parent_key is not null
--  			union all
--	  		select 
--    			Items.pnc_stdy_smry_id as pnc_stdy_smry_id,
--    			t.ancestor as ancestor, 
--    			t.lvl + 1 as lvl
--	  		from AncestryTree t 
--  			join @results_schema.pnc_study_summary_path items 
--  				on t.pnc_stdy_smry_id = Items.tx_path_parent_key
-- 		)
--insert into @pnc_smry_ancstr
--select @jobExecId, nullParentKey, pnc_stdy_smry_id, realLevel
--from (
--  select validancestor.nullParentKey, validancestor.ancestorlevel, 
--  case 
--      when path.pnc_stdy_smry_id is not null then validancestor.ancestorlevel
--      when path.pnc_stdy_smry_id is null then 1000000000
--    end as realLevel,
--  validancestor.parentid, path.pnc_stdy_smry_id
--  from @pnc_smrypth_fltr path
--  right join
--  (select smry.tx_path_parent_key nullParentKey, nullParentAncestors.l ancestorLevel, nullParentAncestors.parent parentId from @pnc_smrypth_fltr smry
--    join
--    (SELECT pnc_stdy_smry_id, ancestor AS parent, lvl as l
--      FROM
--      (
--		select pnc_stdy_smry_id, ancestor, lvl from AncestryTree
--      ) t
--      WHERE t.ancestor <> t.pnc_stdy_smry_id
--      and t.pnc_stdy_smry_id in 
--      (select tx_path_parent_key from @pnc_smrypth_fltr where tx_path_parent_key not in (select PNC_STDY_SMRY_ID from @pnc_smrypth_fltr
--      	where job_execution_id = @jobExecId)
--      	and job_execution_id = @jobExecId)
--    ) nullParentAncestors
--    on smry.tx_path_parent_key = nullParentAncestors.pnc_stdy_smry_id
--    where smry.job_execution_id = @jobExecId) validAncestor
--  on path.pnc_stdy_smry_id = validAncestor.parentId
--  where path.job_execution_id = @jobExecId) a;
create table #replace_rcte
(
    pnc_stdy_smry_id INT,
    ancestor INT,
    lvl INT
);

INSERT INTO #replace_rcte
(pnc_stdy_smry_id, ancestor, lvl)
select 
pnc_stdy_smry_id as pnc_stdy_smry_id,
tx_path_parent_key as ancestor, 
0 as lvl
FROM 
@results_schema.pnc_study_summary_path
where
study_id = @studyId 
and tx_rslt_version = 2
and tx_path_parent_key is not null;

--pull this part into a dynamic sql generation part inserted from Java (too many duplicate sql here to loop from lvl "level" 0 ~ 100)
@insertReplaceCTE_1
--INSERT INTO #replace_rcte
--(pnc_stdy_smry_id, ancestor, lvl)
--select 
--N.pnc_stdy_smry_id as pnc_stdy_smry_id,
--X.ancestor as ancestor, 
--X.lvl + 1 as lvl
--from ohdsi.ohdsi.pnc_study_summary_path N
--join #replace_rcte X
--    on X.pnc_stdy_smry_id = N.tx_path_parent_key
--where X.lvl = 8;
-- will have to hard code lvl = from 0 to maximum 100 for this, run one by one increasing lvl by 1 each time

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
    (SELECT pnc_stdy_smry_id, ancestor AS parent, lvl as l
      FROM
      (
		select pnc_stdy_smry_id, ancestor, lvl from #replace_rcte
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
  where path.job_execution_id = @jobExecId) a;
  
IF OBJECT_ID('tempdb..#replace_rcte', 'U') IS NOT NULL
  DROP TABLE #replace_rcte;

--update null parent key in #_pnc_smrypth_fltr with valid ancestor id which exists in #_pnc_smrypth_fltr or null (null is from level set to 1000000 from table of #_pnc_smry_ancstr)
--refactor merge
--merge into @pnc_smrypth_fltr m
--using
--  (
--    select path.pnc_stdy_smry_id, updateParent.pnc_ancestor_id from @pnc_smrypth_fltr path,
--    (select pnc_stdy_parent_id, pnc_ancestor_id
--    	from (select pnc_stdy_parent_id, pnc_ancestor_id, reallevel, 
--    	row_number() over (partition by pnc_stdy_parent_id order by reallevel) rn
--    	from @pnc_smry_ancstr where job_execution_id = @jobExecId) inner1
--    where rn = 1) updateParent
--    where path.tx_path_parent_key = updateParent.pnc_stdy_parent_id
--    and path.job_execution_id = @jobExecId
--  ) m1
--  on
--  (
--     m.pnc_stdy_smry_id = m1.pnc_stdy_smry_id
--     and m.job_execution_id = @jobExecId
--  )
--  WHEN MATCHED then update set m.tx_path_parent_key = m1.pnc_ancestor_id;
update m
set m.tx_path_parent_key = m1.pnc_ancestor_id
from @pnc_smrypth_fltr m
join (
    select path.pnc_stdy_smry_id, updateParent.pnc_ancestor_id from @pnc_smrypth_fltr path,
    (select pnc_stdy_parent_id, pnc_ancestor_id
    	from (select pnc_stdy_parent_id, pnc_ancestor_id, reallevel, 
    	row_number() over (partition by pnc_stdy_parent_id order by reallevel) rn
    	from @pnc_smry_ancstr where job_execution_id = @jobExecId) inner1
    where rn = 1) updateParent
    where path.tx_path_parent_key = updateParent.pnc_stdy_parent_id
    and path.job_execution_id = @jobExecId
) m1
on 
  m.pnc_stdy_smry_id = m1.pnc_stdy_smry_id
  and m.job_execution_id = @jobExecId;

--update path (tx_stg_cmb_pth with modified_path in the recursive query) in #_pnc_smrypth_fltr
--refactor big hierarchical recursive CTE
--  WITH t1(job_execution_id, combo_id, current_path1, pnc_stdy_smry_id, parent_key, modified_path, Lvl, depthOrder) AS (
--        SELECT
--           job_execution_id						as job_execution_id
--          ,tx_stg_cmb                           as combo_id
--          ,tx_stg_cmb_pth                       as current_path1
--          ,pnc_stdy_smry_id                     as pnc_stdy_smry_id
--          ,tx_path_parent_key                   as parent_key
--          ,cast(tx_stg_cmb as varchar(max))                           as modified_path
--          ,1                                    as Lvl
--          ,cast(pnc_stdy_smry_id as varchar(max))+''                 as depthOrder
--          FROM   @pnc_smrypth_fltr
--        WHERE tx_path_parent_key is null
--        and job_execution_id = @jobExecId
--        UNION ALL
--        SELECT 
--           t2.job_execution_id 					   as job_execution_id
--		  ,t2.tx_stg_cmb                           as combo_id
--          ,t2.tx_stg_cmb_pth                       as current_path1
--          ,t2.pnc_stdy_smry_id                     as pnc_stdy_smry_id
--          ,t2.tx_path_parent_key                   as parent_key
--          ,modified_path+'>'+cast(t2.tx_stg_cmb as varchar(max))       as modified_path
--          ,lvl+1                                as Lvl
--          ,depthOrder+'.'+cast(t2.pnc_stdy_smry_id as varchar(max)) as depthOrder
--        FROM    @pnc_smrypth_fltr t2, t1
--        WHERE  t2.tx_path_parent_key = t1.pnc_stdy_smry_id
--        and t2.job_execution_id = @jobExecId
--      )
--merge into @pnc_smrypth_fltr m
--using
--(
--      SELECT row_number() over(order by depthOrder) as rnum, combo_id, modified_path, lvl, current_path1, pnc_stdy_smry_id, parent_key, depthOrder
--      FROM   t1
--) m1
--on
--(
--  m.pnc_stdy_smry_id = m1.pnc_stdy_smry_id
--  and m.job_execution_id = @jobExecId
--)
--WHEN MATCHED then update set m.tx_stg_cmb_pth = m1.modified_path;
create table #replace_rcte_1
(
    job_execution_id    BIGINT,
    combo_id VARCHAR(255),
    current_path1 VARCHAR(4000),
    pnc_stdy_smry_id INT,
    parent_key BIGINT,
    modified_path VARCHAR(4000),
    lvl INT,
    depthOrder VARCHAR(4000)
);

insert into #replace_rcte_1
(job_execution_id, combo_id, current_path1, pnc_stdy_smry_id,
parent_key, modified_path, lvl,depthOrder)
select 
job_execution_id, tx_stg_cmb, tx_stg_cmb_pth, pnc_stdy_smry_id,
tx_path_parent_key, cast(tx_stg_cmb as varchar(max)), 1,
cast(pnc_stdy_smry_id as varchar(max))+''
FROM @pnc_smrypth_fltr
WHERE tx_path_parent_key is null
and job_execution_id = @jobExecId;

--pull this part into a dynamic sql generation part inserted from Java (too many duplicate sql here to loop from lvl "level" 1 ~ 100)
@insertReplaceCTE_2
--insert into #replace_rcte_1
--(job_execution_id, combo_id, current_path1, pnc_stdy_smry_id,
--parent_key, modified_path, lvl,depthOrder)
--select
--N.job_execution_id, N.tx_stg_cmb, N.tx_stg_cmb_pth,
--N.pnc_stdy_smry_id, N.tx_path_parent_key, 
--X.modified_path+'>'+cast(N.tx_stg_cmb as varchar(max)), 
--X.lvl+1, 
--X.depthOrder+'.'+cast(N.pnc_stdy_smry_id as varchar(max))
--from ohdsi.ohdsi.pnc_tmp_smrypth_fltr N
--join #replace_rcte_1 X
--    on X.pnc_stdy_smry_id = N.tx_path_parent_key
--where X.lvl = 1;
-- will have to hard code lvl = from 1 to maximum 100 for this, run one by one increasing lvl by 1 each time

update  m
set m.tx_stg_cmb_pth = m1.modified_path
from @pnc_smrypth_fltr m
join #replace_rcte_1 m1
on
(
  m.pnc_stdy_smry_id = m1.pnc_stdy_smry_id
  and m.job_execution_id = @jobExecId
);

IF OBJECT_ID('tempdb..#replace_rcte_1', 'U') IS NOT NULL
  DROP TABLE #replace_rcte_1;
  
------------------try unique path here-----------------
--refactor big hierarchical recursive CTE
--WITH t1(combo_id, current_path1, pnc_stdy_smry_id, parent_key, modified_path, Lvl, depthOrder, job_execution_id) AS (
--        SELECT 
--          tx_stg_cmb                            as combo_id
--          ,tx_stg_cmb_pth                       as current_path1
--          ,pnc_stdy_smry_id                     as pnc_stdy_smry_id
--          ,tx_path_parent_key                   as parent_key
--          ,cast(tx_stg_cmb as varchar(max))                           as modified_path
--          ,1                                    as Lvl
--          ,cast(pnc_stdy_smry_id AS varchar(max))+''                 as depthOrder
--          , job_execution_id as job_execution_id
--          FROM   @pnc_smrypth_fltr
--  		WHERE pnc_stdy_smry_id in (select pnc_stdy_smry_id from @pnc_smrypth_fltr
--        where 
--	        study_id = @studyId
--        	and tx_rslt_version = 2
--        	and job_execution_id = @jobExecId
--	        and tx_path_parent_key is null)
--        UNION ALL
--        SELECT 
--          t2.tx_stg_cmb                           as combo_id
--          ,t2.tx_stg_cmb_pth                       as current_path1
--          ,t2.pnc_stdy_smry_id                     as pnc_stdy_smry_id
--          ,t2.tx_path_parent_key                   as parent_key
--          ,cast(t1.modified_path as varchar(max))+'>'+cast(t2.tx_stg_cmb as varchar(max))       as modified_path
--          ,lvl+1                                as Lvl
--          ,t1.depthOrder+'.'+cast(t2.pnc_stdy_smry_id AS varchar(max)) as depthOrder
--          ,t2.job_execution_id as job_execution_id
--        FROM    @pnc_smrypth_fltr t2, t1
--        WHERE  t2.tx_path_parent_key = t1.pnc_stdy_smry_id
--        and t2.job_execution_id = @jobExecId
--      )
--	 insert into @pnc_unq_trtmt(job_execution_id, rnum, pnc_stdy_smry_id, path_cmb_ids)
--	 select @jobExecId, row_number() over(order by depthOrder) as rnum, pnc_stdy_smry_id as pnc_stdy_smry_id, modified_path as modified_path
--	 from t1
--order by depthOrder;
--
--update path_unique_treatment for current path unit concpetIds
--WITH t1(combo_id, current_path1, pnc_stdy_smry_id, parent_key, modified_path, modified_concepts, Lvl, depthOrder, job_execution_id) AS (
--        SELECT 
--          rootPath.tx_stg_cmb                            as combo_id
--          ,tx_stg_cmb_pth                       as current_path1
--          ,pnc_stdy_smry_id                     as pnc_stdy_smry_id
--          ,tx_path_parent_key                   as parent_key
--          ,cast(tx_stg_cmb as varchar(max))                           as modified_path
--          ,cast(comb.concept_ids as varchar(max))                     as modified_concepts
--          ,1                                    as Lvl
--          ,cast(pnc_stdy_smry_id as varchar(max))+''                 as depthOrder
--          ,rootPath.job_execution_id as job_execution_id
--          FROM @pnc_smrypth_fltr rootPath
--          join @pnc_smry_msql_cmb comb
--          on rootPath.tx_stg_cmb = comb.pnc_tx_stg_cmb_id
--          and comb.job_execution_id = @jobExecId
--  		WHERE pnc_stdy_smry_id in (select pnc_stdy_smry_id from @pnc_smrypth_fltr
--        where 
--	        study_id = @studyId
--        	and tx_rslt_version = 2
--        	and job_execution_id = @jobExecId
--	        and tx_path_parent_key is null)
--	        and rootPath.job_execution_id = @jobExecId
--        UNION ALL
--        SELECT 
--          t2.tx_stg_cmb                           as combo_id
--          ,t2.tx_stg_cmb_pth                       as current_path1
--          ,t2.pnc_stdy_smry_id                     as pnc_stdy_smry_id
--          ,t2.tx_path_parent_key                   as parent_key
--          ,t1.modified_path+'>'+cast(t2.tx_stg_cmb as varchar(max))       as modified_path
----this case clause simplify caltulation of duplicate concept_ids by just assert if concept_ids string already in parents ids ',id1,id2,id3,'
----compare path too: if previous path contains current path combo '>combo_id>', no need to append again
----concepts_ids in #_pnc_smry_msql_cmb.concept_ids should help
--          ,
--          CASE 
----		     WHEN CHARINDEX(modified_concepts, ',' + comb.concept_ids + ',') > 0
--		     WHEN CHARINDEX(',' + comb.concept_ids + ',', modified_concepts) > 0
----    		 or CHARINDEX(modified_path, '>' + t2.tx_stg_cmb + '>') > 0
--    		 or CHARINDEX('>' + t2.tx_stg_cmb + '>', modified_path) > 0		     
--    		 THEN modified_concepts
--    		 ELSE modified_concepts+','+cast(comb.concept_ids as varchar(max))
--		  END
--												as modified_concepts
--          ,lvl+1                                as Lvl
--          ,depthOrder+'.'+cast(t2.pnc_stdy_smry_id as varchar(max)) as depthOrder
--          ,t2.job_execution_id as job_execution_id
--        FROM (@pnc_smrypth_fltr t2
--        join @pnc_smry_msql_cmb comb
--        on t2.tx_stg_cmb = comb.pnc_tx_stg_cmb_id
--        and comb.job_execution_id = @jobExecId
--        and t2.job_execution_id = @jobExecId), t1
--        WHERE  t2.tx_path_parent_key = t1.pnc_stdy_smry_id
--        and t1.job_execution_id = @jobExecId
--      )
--	  merge into @pnc_unq_trtmt m
--	  using
--	  (
--	  SELECT row_number() over(order by depthOrder) as rnum, combo_id, modified_path, modified_concepts, lvl, current_path1, pnc_stdy_smry_id, parent_key, depthOrder
--      FROM   t1
----sql server change: no order by here... need test...
----order by depthOrder
--	  ) m1
--on
--(
--  m1.pnc_stdy_smry_id = m.pnc_stdy_smry_id
--  and m.job_execution_id = @jobExecId
--)
--WHEN MATCHED then update set m.path_unique_treatment = m1.modified_concepts;
create table #replace_rcte_2
(
    pnc_stdy_smry_id INT,
    tx_path_parent_key BIGINT,
    tx_stg_cmb    VARCHAR(255),
    tx_stg_cmb_pth VARCHAR(4000),
    tx_seq         BIGINT,
    tx_stg_cnt      BIGINT,
    tx_stg_percentage numeric(5,2),
    tx_stg_avg_dr   INT,
    tx_stg_avg_gap  INT,
    tx_avg_frm_strt INT,
    lvl INT,
    parent_comb VARCHAR(255),
    modified_path varchar(800),
    path_unique_treatment varchar(4000)
);

INSERT INTO #replace_rcte_2
(pnc_stdy_smry_id, tx_path_parent_key, tx_stg_cmb, tx_stg_cmb_pth, 
tx_seq, tx_stg_cnt, tx_stg_percentage, tx_stg_avg_dr, tx_stg_avg_gap, 
tx_avg_frm_strt, lvl, parent_comb, modified_path, path_unique_treatment)
select pnc_stdy_smry_id, tx_path_parent_key, tx_stg_cmb, tx_stg_cmb_pth, 
tx_seq, tx_stg_cnt, tx_stg_percentage, tx_stg_avg_dr, tx_stg_avg_gap, 
tx_avg_frm_strt, 1 as Level, null as parent_comb,
tx_stg_cmb, comb.concept_ids
from @pnc_smrypth_fltr sp
join @pnc_smry_msql_cmb comb
on sp.tx_stg_cmb = comb.pnc_tx_stg_cmb_id
and comb.job_execution_id = @jobExecId
where 
sp.study_id = @studyId
and sp.tx_rslt_version = 2
and sp.job_execution_id = @jobExecId
and sp.tx_path_parent_key is null;

--pull this part into a dynamic sql generation part inserted from Java (too many duplicate sql here to loop from lvl "level" 1 ~ 100)
@insertReplaceCTE_3
--INSERT INTO #replace_rcte_2
--(pnc_stdy_smry_id, tx_path_parent_key, tx_stg_cmb, tx_stg_cmb_pth, 
--tx_seq, tx_stg_cnt, tx_stg_percentage, tx_stg_avg_dr, tx_stg_avg_gap, 
--tx_avg_frm_strt, lvl, parent_comb, modified_path, path_unique_treatment)
--select N.pnc_stdy_smry_id, N.tx_path_parent_key, N.tx_stg_cmb, N.tx_stg_cmb_pth, 
--N.tx_seq, N.tx_stg_cnt, N.tx_stg_percentage, N.tx_stg_avg_dr, N.tx_stg_avg_gap, 
--N.tx_avg_frm_strt, (X.lvl + 1) as lvl, X.tx_stg_cmb as parent_comb,
--X.modified_path+'>' + N.tx_stg_cmb as modified_path,
--CASE 
--  WHEN CHARINDEX(comb.concept_ids + ',', X.path_unique_treatment) > 0
--  or CHARINDEX(N.tx_stg_cmb + '>', X.modified_path) > 0
--  THEN X.path_unique_treatment
--  ELSE X.path_unique_treatment+','+comb.concept_ids
--END
--as path_unique_treatment
--from @pnc_smrypth_fltr N
--join @pnc_smry_msql_cmb comb
--on N.tx_stg_cmb = comb.pnc_tx_stg_cmb_id
--and comb.job_execution_id = @jobExecId
--join #replace_rcte_2 X
--on X.pnc_stdy_smry_id = N.tx_path_parent_key
--where X.lvl = 1;
-- will have to hard code lvl = from 1 to maximum 100 for this, run one by one increasing lvl by 1 each time

insert into @pnc_unq_trtmt 
(job_execution_id, rnum, pnc_stdy_smry_id, rslt_version, path_cmb_ids, path_unique_treatment)
select @jobExecId, 
  row_number() over(order by tx_stg_cmb_pth) as rnum, 
  pnc_stdy_smry_id as pnc_stdy_smry_id, 
  null as rslt_version, modified_path, path_unique_treatment
      FROM #replace_rcte_2
  order by rnum;

IF OBJECT_ID('tempdb..#replace_rcte_2', 'U') IS NOT NULL
  DROP TABLE #replace_rcte_2;

--split conceptIds with "," from #_pnc_unq_trtmt per smry_id and insert order by lastPos (which is used as the order of the concepts in the path)
--remove splitter_cte
--WITH splitter_cte(smry_id, origin, pos, lastPos) AS (
--      SELECT 
--        pnc_stdy_smry_id smry_id,
--        path_unique_treatment as origin,
--		CHARINDEX(',', path_unique_treatment) as pos, 
--        0 as lastPos
--      from @pnc_unq_trtmt
--      where job_execution_id = @jobExecId
--      UNION ALL
--      SELECT 
--        smry_id as smry_id,
--        origin as origin, 
--		CHARINDEX(',', origin, pos + 1) as pos, 
--        pos as lastPos
--      FROM splitter_cte
--      WHERE pos > 0
--    )
--insert into @pnc_unq_pth_id (job_execution_id, pnc_tx_smry_id, concept_id, concept_order)
--SELECT 
--	  @jobExecId,
--      smry_id, 
--      SUBSTRING(origin, lastPos + 1,
--        case when pos = 0 then 80000
--        else pos - lastPos -1 end) as ids,
--      lastPos
--    FROM splitter_cte
--    order by smry_id, ids;
create table #tmp_numbers(
ID int
);

-- stupid workaround for not using identiy/primary key/sequence in #tmp_numbers (dont think Sql Render support this in temp table creation!!!) 
insert into #tmp_numbers (ID)
select top 8000 row_number() over (order by concept_id) from @cdm_schema.concept;

create table #temp(
-- use auto incremental primary key is the best, but dont think Sql Render support this!!!
--ID int identity(1,1) primary key,
ID int,
StringValues varchar(8000),
pnc_stdy_smry_id bigint
);

insert into #temp(ID, StringValues, pnc_stdy_smry_id)
select ROW_NUMBER() OVER(ORDER BY (select NULL as noorder)), 
substring(',' + trtmt.path_unique_treatment,ID+1,charindex(',',',' + trtmt.path_unique_treatment,ID+1)-ID-1), 
trtmt.pnc_stdy_smry_id
from #tmp_numbers, @pnc_unq_trtmt trtmt where ID < len(',' + trtmt.path_unique_treatment)
and substring(',' + trtmt.path_unique_treatment,ID,1) = ','
and trtmt.job_execution_id = @jobExecId;

insert into @pnc_unq_pth_id (job_execution_id, pnc_tx_smry_id, concept_id, concept_order)
select @jobExecId, trtmt.pnc_stdy_smry_id, CONVERT(INT, tmp.StringValues), tmp.ID
from @pnc_unq_trtmt trtmt, #temp tmp
where trtmt.job_execution_id = @jobExecId 
and tmp.pnc_stdy_smry_id = trtmt.pnc_stdy_smry_id
order by trtmt.pnc_stdy_smry_id, tmp.ID;

drop table #temp;

drop table #tmp_numbers;

--sql server workaround.......
delete from @pnc_unq_pth_id 
where
 job_execution_id = @jobExecId
 and (concept_id = 0 or concept_id is null);
 
--delete duplicate concept_id per smry_id in the path if it's not the first on in the path by min(concept_order)
--refactor for removing %%physloc%%
--delete from @pnc_unq_pth_id 
--where %%physloc%% in (select conceptIds.%%physloc%% from @pnc_unq_pth_id conceptIds, 
--  (select pnc_tx_smry_id, concept_id, min(concept_order) as concept_order
--    from @pnc_unq_pth_id
--    where job_execution_id = @jobExecId
--    group by pnc_tx_smry_id, concept_id
--  ) uniqueIds
--where
--  conceptIds.pnc_tx_smry_id = uniqueIds.pnc_tx_smry_id
--  and conceptIds.concept_id = uniqueIds.concept_id
--  and conceptIds.concept_order != uniqueIds.concept_order
--  and conceptIds.job_execution_id = @jobExecId
--)
--and job_execution_id = @jobExecId;
delete from @pnc_unq_pth_id
where exists(
select * from
  (select pnc_tx_smry_id, concept_id, min(concept_order) as concept_order
    from @pnc_unq_pth_id
    where job_execution_id = @jobExecId
    group by pnc_tx_smry_id, concept_id
  ) uniqueIds
where
  @pnc_unq_pth_id.pnc_tx_smry_id = uniqueIds.pnc_tx_smry_id
  and @pnc_unq_pth_id.concept_id = uniqueIds.concept_id
  and @pnc_unq_pth_id.concept_order != uniqueIds.concept_order
  and @pnc_unq_pth_id.job_execution_id = @jobExecId
 );

--update conceptsArray and conceptName JSON by join concept table
-- remove "for xml path" and merge: sql render does not support!!
-- create a temp table #pnc_tmp_unq_pth_id_fix besically same as @pnc_unq_pth_id
-- then insert first concept by min(concept_order) per pnc_tx_smry_id&job_execution_id
-- then update #pnc_tmp_unq_pth_id_fix with all rest concepts with same pnc_tx_smry_id&job_execution_id
--merge into @pnc_unq_pth_id m
--using
--(
--	select path1.pnc_tx_smry_id,
--    '[' + STUFF((SELECT distinct ',{"innerConceptName":' + '"' + path2.concept_name + '"' +
--    ',"innerConceptId":' + convert(varchar, path2.concept_id) + '}'
--		from (select path.pnc_tx_smry_id as pnc_tx_smry_id, concepts.concept_id as concept_id, 
--			concepts.concept_name as concept_name
--			from @pnc_unq_pth_id path
--			join @cdm_schema.concept concepts
--			on path.concept_id = concepts.concept_id
--			where path.job_execution_id = @jobExecId
--		) path2
--         where path1.pnc_tx_smry_id = path2.pnc_tx_smry_id
--            FOR XML PATH(''), TYPE
--            ).value('.', 'NVARCHAR(MAX)') 
--        ,1,0,'') +  ']' as conceptsArray,
--	STUFF((SELECT distinct path2.concept_name + ','
--         from (select path.pnc_tx_smry_id as pnc_tx_smry_id, concepts.concept_id as concept_id, 
--			concepts.concept_name as concept_name
--			from @pnc_unq_pth_id path
--			join @cdm_schema.concept concepts
--			on path.concept_id = concepts.concept_id
--			where path.job_execution_id = @jobExecId
--		) path2
--         where path1.pnc_tx_smry_id = path2.pnc_tx_smry_id
--            FOR XML PATH(''), TYPE
--            ).value('.', 'NVARCHAR(MAX)')  
--        ,1,0,'') as conceptsName
--	from (select path.pnc_tx_smry_id as pnc_tx_smry_id, concepts.concept_id as concept_id, 
--		concepts.concept_name as concept_name
--		from @pnc_unq_pth_id path
--		join @cdm_schema.concept concepts
--		on path.concept_id = concepts.concept_id
--		where path.job_execution_id = @jobExecId
--	) path1
--	group by pnc_tx_smry_id
--) m1
--on
--(
--  m.pnc_tx_smry_id = m1.pnc_tx_smry_id
--  and m.job_execution_id = @jobExecId
--)
--WHEN MATCHED then update set m.conceptsArray = m1.conceptsArray,
-- m.conceptsName = m1.conceptsName;
--
--update @pnc_unq_pth_id 
--set conceptsArray = '[' + substring(conceptsArray, 3, len(conceptsArray))
--where job_execution_id = @jobExecId;
CREATE TABLE #pnc_tmp_unq_pth_id_fix
(
    job_execution_id BIGINT,
    pnc_tx_smry_id BIGINT,
    concept_id BIGINT,
    concept_order int,
    concept_count int,
    conceptsName varchar(1000),
    conceptsArray varchar(1500)
);

insert into #pnc_tmp_unq_pth_id_fix(job_execution_id, pnc_tx_smry_id, concept_id, concept_order, concept_count, conceptsName, conceptsArray)
select pId.job_execution_id, pId.pnc_tx_smry_id, pId.concept_id, pId.concept_order, pId.concept_count, 
concepts.concept_name, 
'[{"innerConceptName":' + '"' + concepts.concept_name + '"' + ',"innerConceptId":' + convert(varchar, concepts.concept_id) + '}'
from @pnc_unq_pth_id pId
join @cdm_schema.concept concepts
on pId.concept_id = concepts.concept_id,
(select pnc_tx_smry_id, min(concept_order) concept_order, job_execution_id  
from  @pnc_unq_pth_id
where job_execution_id = @jobExecId
and concept_id != 0
group by job_execution_id, pnc_tx_smry_id, job_execution_id) minPid
where 
pId.job_execution_id = minPid.job_execution_id
and pid.pnc_tx_smry_id = minPid.pnc_tx_smry_id
and pid.concept_order = minPid.concept_order;

update a
set a.conceptsName = a.conceptsName + allColPid.concatConceptName,
a.conceptsArray = a.conceptsArray + allColPid.concatConceptArray
from #pnc_tmp_unq_pth_id_fix a
join (
select pidFix.*, missPid.concatConceptName, missPid.concatConceptArray from #pnc_tmp_unq_pth_id_fix pidFix
join (select pth.*, 
',' + concepts.concept_name concatConceptName, 
',{"innerConceptName":' + '"' + concepts.concept_name + '"' + ',"innerConceptId":' + convert(varchar, concepts.concept_id) + '}' concatConceptArray
 from @pnc_unq_pth_id pth 
join @cdm_schema.concept concepts
on pth.concept_id = concepts.concept_id
where exists
(
  select * from #pnc_tmp_unq_pth_id_fix pthFix
  where 
  pth.job_execution_id = @jobExecId 
  and pth.job_execution_id = pthFix.job_execution_id 
  and pth.pnc_tx_smry_id = pthFix.pnc_tx_smry_id
  and pth.concept_order != pthFix.concept_order
)
and pth.job_execution_id = @jobExecId
and pth.concept_id != 0
) missPid
on pidFix.job_execution_id = missPid.job_execution_id
and pidFix.pnc_tx_smry_id= missPid.pnc_tx_smry_id
) allColPid
on a.job_execution_id = allColPid.job_execution_id
and a.pnc_tx_smry_id= allColPid.pnc_tx_smry_id;

update #pnc_tmp_unq_pth_id_fix
set conceptsArray = conceptsArray + ']';

update pId
set pId.conceptsName = fix.conceptsName,
pId.conceptsArray = fix.conceptsArray
from @pnc_unq_pth_id Pid
join #pnc_tmp_unq_pth_id_fix fix
on pId.job_execution_id = fix.job_execution_id
and pId.pnc_tx_smry_id = fix.pnc_tx_smry_id;

drop table #pnc_tmp_unq_pth_id_fix;

--refactor merge
--merge into @pnc_unq_pth_id m
--using
--(
--	select path1.pnc_tx_smry_id,
--    count(distinct concepts.concept_id) conceptCount
--    from @pnc_unq_pth_id path1
--    join @cdm_schema.concept concepts
--    on path1.concept_id = concepts.concept_id
--    where path1.job_execution_id = @jobExecId
--    group by path1.pnc_tx_smry_id
--) m1
--on
--(
--  m.pnc_tx_smry_id = m1.pnc_tx_smry_id
--  and m.job_execution_id = @jobExecId
--)
--WHEN MATCHED then update set m.concept_count = m1.conceptCount;
update m
set m.concept_count = m1.conceptCount
from @pnc_unq_pth_id m
join
(
    select path1.pnc_tx_smry_id,
    count(distinct concepts.concept_id) conceptCount
    from @pnc_unq_pth_id path1
    join @cdm_schema.concept concepts
    on path1.concept_id = concepts.concept_id
    where path1.job_execution_id = @jobExecId
    and path1.concept_id != 0
    group by path1.pnc_tx_smry_id
) m1
on
  m.pnc_tx_smry_id = m1.pnc_tx_smry_id
  and m.job_execution_id = @jobExecId;


--delete duplicat smry_id rows (now we have smry_id with it's unique concepts conceptsArray and conceptsName)
--refactor %%physloc%%
--delete from @pnc_unq_pth_id 
--where %%physloc%% in (select conceptIds.%%physloc%% from @pnc_unq_pth_id conceptIds, 
--  (select pnc_tx_smry_id, min(concept_order) as concept_order
--    from @pnc_unq_pth_id
--    where job_execution_id = @jobExecId
--    group by pnc_tx_smry_id
--  ) uniqueIds
--where
--  conceptIds.pnc_tx_smry_id = uniqueIds.pnc_tx_smry_id
--  and conceptIds.concept_order != uniqueIds.concept_order
--  and conceptIds.job_execution_id = @jobExecId
--)
--and job_execution_id = @jobExecId;
delete from @pnc_unq_pth_id 
where concept_order in (select conceptIds.concept_order from @pnc_unq_pth_id conceptIds, 
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

----------------------------------version 2 into temp table ------------------------------------------
delete from #pnc_tmp_smry;

--refactor big hierarchical recursive CTE
--WITH t1( pnc_stdy_smry_id, tx_path_parent_key, lvl, 
--	tx_stg_cmb, tx_stg_cmb_pth, tx_seq, tx_stg_avg_dr, tx_stg_cnt, 
--	tx_stg_percentage, tx_stg_avg_gap, depthOrder, parent_comb, daysFromStart) AS (
--	    SELECT 
--           pnc_stdy_smry_id as pnc_stdy_smry_id, tx_path_parent_key as tx_path_parent_key,
--           1 AS lvl,
--           tx_stg_cmb as tx_stg_cmb, tx_stg_cmb_pth as tx_stg_cmb_pth, tx_seq as tx_seq, tx_stg_avg_dr as tx_stg_avg_dr, tx_stg_cnt as tx_stg_cnt,
--           tx_stg_percentage as tx_stg_percentage
--           ,tx_stg_avg_gap as  tx_stg_avg_gap
--           ,CAST(pnc_stdy_smry_id AS varchar(max))+'' as depthOrder, 
--           CAST(null as varchar(255)) as parent_comb
--           ,tx_avg_frm_strt				  as daysFromStart
--          FROM @pnc_smrypth_fltr
--        WHERE pnc_stdy_smry_id in (select pnc_stdy_smry_id FROM @pnc_smrypth_fltr
--              where 
--              study_id = @studyId
--              and tx_rslt_version = 2
--              and tx_path_parent_key is null
--              and job_execution_id = @jobExecId)
--              and job_execution_id = @jobExecId
--        UNION ALL
--        SELECT 
--              t2.pnc_stdy_smry_id as pnc_stdy_smry_id, t2.tx_path_parent_key as tx_path_parent_key,
--              t1.lvl+1 as lvl,
--              t2.tx_stg_cmb as tx_stg_cmb, t2.tx_stg_cmb_pth as tx_stg_cmb_pth, t2.tx_seq as tx_seq, t2.tx_stg_avg_dr as tx_stg_avg_dr, 
--              t2.tx_stg_cnt as tx_stg_cnt, t2.tx_stg_percentage as tx_stg_percentage
--              ,t2.tx_stg_avg_gap as tx_stg_avg_gap 
--              ,t1.depthOrder+'.'+CAST(t2.pnc_stdy_smry_id AS varchar(max)) as depthOrder, ISNULL(t1.tx_stg_cmb,'') as parent_comb
--              ,t2.tx_avg_frm_strt				  as daysFromStart
--        FROM   @pnc_smrypth_fltr t2, t1
--        WHERE   t2.tx_path_parent_key = t1.pnc_stdy_smry_id
--        		and t2.job_execution_id = @jobExecId
--      )
--	  insert into #pnc_tmp_smry (rnum, combo_id, current_path, path_seq, avg_duration, pt_count, pt_percentage,	concept_names, 
--	  	combo_concepts, lvl, parent_concept_names, parent_combo_concepts,
--		avg_gap, gap_pcnt, uniqueConceptsName, uniqueConceptsArray, uniqueConceptCount, daysFromStart)
--      SELECT row_number() over(order by depthOrder) as rnum, 
--		tx_stg_cmb as combo_id, 
--		tx_stg_cmb_pth as current_path,
--		tx_seq as path_seq,
--		tx_stg_avg_dr as avg_duration,
--		tx_stg_cnt as pt_count,
--		tx_stg_percentage as pt_percentage,
--	    concepts.conceptsName as concept_names,
--		concepts.conceptsArray as combo_concepts,
--		lvl as lvl,
--	    parentConcepts.conceptsName as parent_concept_names,
--		parentConcepts.conceptsArray as parent_combo_concepts
--    	,tx_stg_avg_gap                       as avg_gap
--    	,isnull(ROUND(cast(tx_stg_avg_gap as float)/cast(tx_stg_avg_dr as float) * 100,2),0)   as gap_pcnt
--    	,uniqueConcepts.conceptsName		  as uniqueConceptsName
--    	,uniqueConcepts.conceptsArray		  as uniqueConceptsArray
--    	,uniqueConcepts.concept_count		  as uniqueConceptCount    
--		,daysFromStart	   					  as daysFromStart
--      FROM t1
--	  join @pnc_smry_msql_cmb concepts 
--	  on concepts.pnc_tx_stg_cmb_id = t1.tx_stg_cmb
--  	  and concepts.job_execution_id = @jobExecId
--  	  left join @pnc_smry_msql_cmb parentConcepts 
--  	  on parentConcepts.pnc_tx_stg_cmb_id = t1.parent_comb
--  	  and parentConcepts.job_execution_id = @jobExecId
--	  join (select pnc_tx_smry_id, conceptsName, conceptsArray, concept_count from @pnc_unq_pth_id where job_execution_id = @jobExecId) uniqueConcepts
--	  on uniqueConcepts.pnc_tx_smry_id = t1.pnc_stdy_smry_id  	  
--  	  order by depthOrder;

create table #replace_rcte_3
(
    pnc_stdy_smry_id INT,
    tx_path_parent_key BIGINT,
    tx_stg_cmb    VARCHAR(255),
    tx_stg_cmb_pth VARCHAR(4000),
    tx_seq         BIGINT,
    tx_stg_cnt      BIGINT,
    tx_stg_percentage numeric(5,2),
    tx_stg_avg_dr   INT,
    tx_stg_avg_gap  INT,
    tx_avg_frm_strt INT,
    lvl INT,
    parent_comb VARCHAR(255),
    gap_pcnt numeric(5, 2),
    uniqueConceptsName varchar(4000),
    uniqueConceptsArray varchar(4000),
    uniqueConceptCount int
);

INSERT INTO #replace_rcte_3
(pnc_stdy_smry_id, tx_path_parent_key, tx_stg_cmb, tx_stg_cmb_pth, 
tx_seq, tx_stg_cnt, tx_stg_percentage, tx_stg_avg_dr, tx_stg_avg_gap, 
tx_avg_frm_strt, lvl, parent_comb, 
gap_pcnt, uniqueConceptsName, uniqueConceptsArray, uniqueConceptCount)
select pnc_stdy_smry_id, tx_path_parent_key, tx_stg_cmb, tx_stg_cmb_pth, 
tx_seq, tx_stg_cnt, tx_stg_percentage, tx_stg_avg_dr, tx_stg_avg_gap, 
tx_avg_frm_strt, 1 as Level, null as parent_comb,
isnull(ROUND(cast(tx_stg_avg_gap as float)/cast(tx_stg_avg_dr as float) * 100,2),0), 
uniqueConcepts.conceptsName, 
uniqueConcepts.conceptsArray, uniqueConcepts.concept_count
from @pnc_smrypth_fltr sp
join @pnc_smry_msql_cmb comb
on sp.tx_stg_cmb = comb.pnc_tx_stg_cmb_id
and comb.job_execution_id = @jobExecId 
join (select pnc_tx_smry_id, conceptsName, conceptsArray, concept_count 
from @pnc_unq_pth_id where job_execution_id = @jobExecId) uniqueConcepts
on uniqueConcepts.pnc_tx_smry_id = sp.pnc_stdy_smry_id  	  
where 
sp.study_id = @studyId 
and sp.tx_rslt_version = 2
and sp.job_execution_id = @jobExecId
and sp.tx_path_parent_key is null;

--pull this part into a dynamic sql generation part inserted from Java (too many duplicate sql here to loop from lvl "level" 1 ~ 100)
@insertReplaceCTE_4
--INSERT INTO #replace_rcte_3
--(pnc_stdy_smry_id, tx_path_parent_key, tx_stg_cmb, tx_stg_cmb_pth, 
--tx_seq, tx_stg_cnt, tx_stg_percentage, tx_stg_avg_dr, tx_stg_avg_gap, 
--tx_avg_frm_strt, lvl, parent_comb, 
--gap_pcnt, uniqueConceptsName, uniqueConceptsArray, uniqueConceptCount)
--select N.pnc_stdy_smry_id, N.tx_path_parent_key, N.tx_stg_cmb, N.tx_stg_cmb_pth, 
--N.tx_seq, N.tx_stg_cnt, N.tx_stg_percentage, N.tx_stg_avg_dr, N.tx_stg_avg_gap, 
--N.tx_avg_frm_strt, (X.lvl + 1) as lvl, X.tx_stg_cmb as parent_comb,
--isnull(ROUND(cast(N.tx_stg_avg_gap as float)/cast(N.tx_stg_avg_dr as float) * 100,2),0), 
--uniqueConcepts.conceptsName, 
--uniqueConcepts.conceptsArray, uniqueConcepts.concept_count
--from @pnc_smrypth_fltr N
--join @pnc_smry_msql_cmb comb
--on N.tx_stg_cmb = comb.pnc_tx_stg_cmb_id
--and comb.job_execution_id = @jobExecId
--join #replace_rcte_3 X
--on X.pnc_stdy_smry_id = N.tx_path_parent_key
--join (select pnc_tx_smry_id, conceptsName, conceptsArray, concept_count 
--from @pnc_unq_pth_id where job_execution_id = @jobExecId) uniqueConcepts
--on uniqueConcepts.pnc_tx_smry_id = N.pnc_stdy_smry_id  	  
--where X.lvl = 1;
-- will have to hard code lvl = from 1 to maximum 100 for this, run one by one increasing lvl by 1 each time

insert into #pnc_tmp_smry 
(rnum, combo_id, current_path, path_seq, avg_duration, pt_count, pt_percentage,	concept_names, 
combo_concepts, lvl, parent_concept_names, parent_combo_concepts,
avg_gap, gap_pcnt, uniqueConceptsName, uniqueConceptsArray, uniqueConceptCount, daysFromStart)
SELECT row_number() over(order by tx_stg_cmb_pth) as rnum, 
		tx_stg_cmb as combo_id, 
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
    	,tx_stg_avg_gap                       as avg_gap
    	,gap_pcnt   as gap_pcnt
    	,uniqueConceptsName		  as uniqueConceptsName
    	,uniqueConceptsArray		  as uniqueConceptsArray
    	,uniqueConceptCount  as uniqueConceptCount    
		,tx_avg_frm_strt	   					  as daysFromStart
      FROM #replace_rcte_3 t1
  join @pnc_smry_msql_cmb concepts 
  on concepts.pnc_tx_stg_cmb_id = t1.tx_stg_cmb
  and concepts.job_execution_id = @jobExecId 
  left join @pnc_smry_msql_cmb parentConcepts 
  on parentConcepts.pnc_tx_stg_cmb_id = t1.parent_comb
  and parentConcepts.job_execution_id = @jobExecId
  order by rnum;

IF OBJECT_ID('tempdb..#replace_rcte_3', 'U') IS NOT NULL
  DROP TABLE #replace_rcte_3;

--refactor lag/lead/replicate function
insert into @pnc_indv_jsn(job_execution_id, rnum, table_row_id, JSON, lvl, leadLvl,lagLvl)
select @jobExecId, rnum, table_row_id, JSON, 
lvl, leadValueSubstitute, lagValueSubstitute
from
(
select allRoots.rnum as rnum, cast(1 as bigint) as table_row_id,
CASE 
--	WHEN rnum = 1 THEN '{"comboId": "root","children": [' + substring(JSON_SNIPPET, 2, len(JSON_SNIPPET))
--    ELSE JSON_SNIPPET
    WHEN rnum = 1 THEN '{"comboId": "root"' 
    + ',"totalCountFirstTherapy":'
    + (select cast(sum(tx_stg_cnt) as varchar(max)) from @pnc_smrypth_fltr 
        where tx_path_parent_key is null
        and tx_seq = 1
        and job_execution_id = @jobExecId)
    + ',"totalCohortCount":'
    + (select cast(count( distinct subject_id) as varchar(max)) from @ohdsi_schema.cohort
    		where cohort_definition_id = @cohort_definition_id)
--        where cohort_definition_id = (select cohort_definition_id from 
--        @results_schema.panacea_study 
--        where study_id = @studyId))
    + ',"firstTherapyPercentage":'
    + (select cast(isnull(ROUND(cast(firstCount.firstCount as float)/cast(cohortTotal.cohortTotal as float) * 100,2),0) as varchar(max)) firstTherrapyPercentage from 
        (select sum(tx_stg_cnt) as firstCount from @pnc_smrypth_fltr 
        where tx_path_parent_key is null
        and tx_seq = 1
        and job_execution_id = @jobExecId) firstCount,  
        (select count( distinct subject_id) as cohortTotal from @ohdsi_schema.cohort
--        where cohort_definition_id = (select cohort_definition_id from 
--        @results_schema.panacea_study 
--        where study_id = @studyId)
				where cohort_definition_id = @cohort_definition_id) 
			cohortTotal)
    +',"children": [' 
    + substring(JSON_SNIPPET, 2, len(JSON_SNIPPET))
    ELSE JSON_SNIPPET
END
as JSON
, lvl, leadValueSubstitute, lagValueSubstitute
from 
(
  select 
  connect_by_query.rnum as rnum,
  CASE 
    WHEN connect_by_query.Lvl = 1 THEN ',{'
--refactor lag/lead/replicate function
--    WHEN Lvl - LAG(Lvl) OVER (order by rnum) = 1 THEN ',"children" : [{'
		WHEN connect_by_query.Lvl - selfTmpSmryLag.lvl = 1 THEN ',"children" : [{' 
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
  + CASE WHEN connect_by_query.Lvl > 1 THEN    
        ',"parentConcept": { "parentConceptName": "' + parent_concept_names + '", '  
        + '"parentConcepts":' + parent_combo_concepts   + '}'
	ELSE ''
    END
--refactor lag/lead/replicate function
--   + CASE WHEN LEAD(Lvl, 1, 1) OVER (order by rnum) - Lvl <= 0
--     THEN '}' + replicate(']}', lvl - LEAD(Lvl, 1, 1) OVER (order by rnum))
--     ELSE ''
--  END 
  as JSON_SNIPPET
--refactor lag/lead/replicate function
	, connect_by_query.lvl as lvl
  , ISNULL(selfTmpSmryLead.lvl, 1) AS leadValueSubstitute
  , selfTmpSmryLag.lvl AS lagValueSubstitute
from #pnc_tmp_smry connect_by_query
--refactor lag/lead/replicate function
LEFT OUTER JOIN 
(select rnum, lvl, 
 row_number() OVER (order by rnum) as orderRnum from #pnc_tmp_smry) selfTmpSmryLead
ON selfTmpSmryLead.rnum = connect_by_query.rnum+1
--refactor lag/lead/replicate function
LEFT OUTER JOIN 
(select rnum, lvl, 
 row_number() OVER (order by rnum) as orderRnum from #pnc_tmp_smry) selfTmpSmryLag
ON selfTmpSmryLag.rnum = connect_by_query.rnum-1
) allRoots
union all
--refactor lag/lead/replicate function
select lastRow.rnum as rnum, lastRow.table_row_id as table_row_id, ']}' as JSON 
, 0, 0, 0 from (
	select distinct 1000000000 as rnum, 1 as table_row_id from @pnc_smrypth_fltr) lastRow
) allRootsAndLastPadding;
  	  
-------------------------------------version 2 into summary table-------------------------------------
-- workaround for sql server version and sql render not supporting aggregatig concatenation: use PanaceaInsertSummaryTasklet
--update @results_schema.pnc_study_summary set study_results_filtered = m1.json, last_update_time = CURRENT_TIMESTAMP
--FROM
--	(select distinct
--		  STUFF((SELECT '' + tab2.json
--         from @pnc_indv_jsn tab2
--         where 
--         tab2.job_execution_id = @jobExecId
--         and tab2.table_row_id = 1
--         order by rnum
--            FOR XML PATH(''), TYPE
--            ).value('.', 'NVARCHAR(MAX)') 
--        ,1,0,'') as JSON
--		FROM @pnc_indv_jsn tab1
--		where tab1.job_execution_id = @jobExecId
--		and tab1.table_row_id = 1
--		group by table_row_id
--	) m1
--WHERE study_id = @studyId ;
