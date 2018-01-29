select JSON from (
SELECT
   table_row_id,
   DBMS_XMLGEN.CONVERT (
     EXTRACT(
       xmltype('<?xml version="1.0"?><document>' ||
               XMLAGG(
                 XMLTYPE('<V>' || DBMS_XMLGEN.CONVERT(JSON)|| '</V>')
                 order by rnum).getclobval() || '</document>'),
               '/document/V/text()').getclobval(),1) AS JSON
FROM (select allRoots.rnum rnum, 1 table_row_id,
CASE 
    WHEN rnum = 1 THEN '{"combo_id": "root","children": [' || substr(JSON_SNIPPET, 2, length(JSON_SNIPPET))
    ELSE JSON_SNIPPET
END
as JSON
from 
(WITH connect_by_query as (
  SELECT 
     ROWNUM                               as rnum
    ,tx_stg_cmb                           as combo_id
    ,tx_stg_cmb_pth                       as current_path
    ,tx_seq                               as path_seq
    ,tx_stg_avg_dr                        as avg_duration
    ,tx_stg_cnt                           as pt_count
    ,concepts.conceptsName                as concept_names
    ,concepts.conceptsArray               as combo_concepts
    ,LEVEL                                as Lvl
  FROM pnc_study_summary_path smry
  join
  (select pnc_stdy_smry_id smry_id, 
    '[' || wm_concat('{"conceptName":' || '"' || concept.concept_name  || '"' || 
    ',"conceptId":' || concept.concept_id || '}') || ']' conceptsArray,
    wm_concat(concept.concept_name) conceptsName
    from pnc_study_summary_path sumPath
    join (select comb.pnc_tx_stg_cmb_id comb_id, combmap.concept_id concept_id, combmap.concept_name concept_name 
    from pnc_tx_stage_combination comb
    join pnc_tx_stage_combination_map combMap 
    on comb.pnc_tx_stg_cmb_id = combmap.pnc_tx_stg_cmb_id
    and comb.study_id = 18) concept
  on concept.comb_id = sumPath.tx_stg_cmb
  group by sumpath.pnc_stdy_smry_id
  ) concepts
  on concepts.smry_id = smry.pnc_stdy_smry_id
  START WITH pnc_stdy_smry_id in (select pnc_stdy_smry_id from pnc_study_summary_path
        where 
        study_id = 18
        and source_id = 2
        and tx_path_parent_key is null)
  CONNECT BY PRIOR pnc_stdy_smry_id = tx_path_parent_key
  ORDER SIBLINGS BY pnc_stdy_smry_id
)
select 
  rnum rnum,
  CASE 
    /* the top dog gets a left curly brace to start things off */
    WHEN Lvl = 1 THEN ',{'
    /* when the last level is lower (shallower) than the current level, start a "children" array */
    WHEN Lvl - LAG(Lvl) OVER (order by rnum) = 1 THEN ',"children" : [{' 
    ELSE ',{' 
  END 
  || ' "combo_id" : ' || combo_id || ' '
  || ' ,"concept_names" : "' || concept_names || '" '  
  || ' ,"patient_counts" : ' || pt_count || ' '
  || ' ,"average_duration" : ' || avg_duration || ' '
  || ',"concepts" : ' || combo_concepts 
  /* when the next level lower (shallower) than the current level, close a "children" array */
  || CASE WHEN LEAD(Lvl, 1, 1) OVER (order by rnum) - Lvl <= 0 
     THEN '}' || rpad( ' ', 1+ (-2 * (LEAD(Lvl, 1, 1) OVER (order by rnum) - Lvl)), ']}' )
     ELSE NULL 
  END as JSON_SNIPPET
from connect_by_query
order by rnum) allRoots
union all
select rnum as rnum, table_row_id as table_row_id, to_clob(']}') as JSON from (
  select distinct 1000000 as rnum, 1 as table_row_id from pnc_study_summary_path)
)
GROUP BY
   table_row_id);