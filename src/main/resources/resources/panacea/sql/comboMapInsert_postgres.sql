
with adding_concept (concept_id, concept_name) as
  (SELECT DISTINCT myConcept.concept_id concept_id, myConcept.concept_name concept_name FROM @cdm_schema.concept myConcept
    where myconcept.concept_id in (@allConceptIdsStr)
    and myConcept.concept_id NOT IN
--change to add all concepts into pnc_tx_stage_combination_map and pnc_tx_stage_combination instead of dynamically add not exising concepts in #_pnc_ptsq_ct, per Jon  
--  (SELECT DISTINCT ptsq.concept_id concept_id, ptsq.concept_name concept_name FROM #_pnc_ptsq_ct ptsq
--    WHERE ptsq.concept_id NOT IN 
--      (select distinct concept_id from #_pnc_sngl_cmb 
--      )
      (select distinct concept_id from 
        (select comb.pnc_tx_stg_cmb_id as comb_id, combmap.concept_id as concept_id, combmap.concept_name as concept_name from @results_schema.pnc_tx_stage_combination comb
          join @results_schema.pnc_tx_stage_combination_map combMap 
          on combmap.pnc_tx_stg_cmb_id = comb.pnc_tx_stg_cmb_id
          join 
          (select comb.pnc_tx_stg_cmb_id, count(*) from @results_schema.pnc_tx_stage_combination comb
          join @results_schema.pnc_tx_stage_combination_map combMap 
          on combmap.pnc_tx_stg_cmb_id = comb.pnc_tx_stg_cmb_id
          where comb.study_id = @studyId
          group by comb.pnc_tx_stg_cmb_id
          having count(*) = 1) multiple_ids_combo
          on multiple_ids_combo.pnc_tx_stg_cmb_id = comb.pnc_tx_stg_cmb_id
        ) innerA
      )
  ) 
INSERT into @results_schema.pnc_tx_stage_combination_map (PNC_TX_STG_CMB_MP_ID, PNC_TX_STG_CMB_ID, CONCEPT_ID, CONCEPT_NAME)
select NEXTVAL('@results_schema.seq_pnc_tx_stg_cmb_mp'), NEXTVAL('@results_schema.seq_pnc_tx_stg_cmb'), adding_concept.concept_id, adding_concept.concept_name
from adding_concept;

with adding_combo (pnc_tx_stg_cmb_id) as
  (
    SELECT combo_map.pnc_tx_stg_cmb_id pnc_tx_stg_cmb_id FROM @results_schema.pnc_tx_stage_combination_map combo_map
  )
insert into @results_schema.pnc_tx_stage_combination 
select adding_combo.pnc_tx_stg_cmb_id, @studyId
from adding_combo
where adding_combo.pnc_tx_stg_cmb_id not in (select pnc_tx_stg_cmb_id from @results_schema.pnc_tx_stage_combination);
