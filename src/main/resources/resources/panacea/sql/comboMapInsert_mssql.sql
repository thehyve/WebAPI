

--insert into @results_schema.pnc_tx_stage_combination_map (pnc_tx_stg_cmb_mp_id, pnc_tx_stg_cmb_id, concept_id, concept_name)
--select NEXT VALUE FOR @results_schema.seq_pnc_tx_stg_cmb_mp, NEXT VALUE FOR @results_schema.seq_pnc_tx_stg_cmb, concept_id, concept_name from
--  (SELECT DISTINCT myConcept.concept_id concept_id, myConcept.concept_name concept_name FROM @cdm_schema.concept myConcept
--    where myconcept.concept_id in (@allConceptIdsStr)
--    and myConcept.concept_id NOT IN
--      (select distinct concept_id from 
--        (select comb.pnc_tx_stg_cmb_id as comb_id, combmap.concept_id as concept_id, combmap.concept_name as concept_name from @results_schema.pnc_tx_stage_combination comb
--          join @results_schema.pnc_tx_stage_combination_map combMap 
--          on combmap.pnc_tx_stg_cmb_id = comb.pnc_tx_stg_cmb_id
--          join 
--          (select comb.pnc_tx_stg_cmb_id pnc_tx_stg_cmb_id, count(*) cnt from @results_schema.pnc_tx_stage_combination comb
--          join @results_schema.pnc_tx_stage_combination_map combMap 
--          on combmap.pnc_tx_stg_cmb_id = comb.pnc_tx_stg_cmb_id
--          where comb.study_id = @studyId
--          group by comb.pnc_tx_stg_cmb_id
--          having count(*) = 1) multiple_ids_combo
--          on multiple_ids_combo.pnc_tx_stg_cmb_id = comb.pnc_tx_stg_cmb_id
--        ) distinctConcept
--      )
--  ) adding_concept;


-- Ahh quick workaround to remove sequences!!!

insert into @results_schema.pnc_tx_stage_combination (study_id, concept_id, concept_name)
select @studyId, concept_id, concept_name from
  (SELECT DISTINCT myConcept.concept_id concept_id, myConcept.concept_name concept_name FROM @cdm_schema.concept myConcept
    where myconcept.concept_id in (@allConceptIdsStr)
    and myConcept.concept_id NOT IN
      (select distinct concept_id from 
        (select comb.pnc_tx_stg_cmb_id as comb_id, combmap.concept_id as concept_id, combmap.concept_name as concept_name from @results_schema.pnc_tx_stage_combination comb
          join @results_schema.pnc_tx_stage_combination_map combMap 
          on combmap.pnc_tx_stg_cmb_id = comb.pnc_tx_stg_cmb_id
          join 
          (select comb.pnc_tx_stg_cmb_id pnc_tx_stg_cmb_id, count(*) cnt from @results_schema.pnc_tx_stage_combination comb
          join @results_schema.pnc_tx_stage_combination_map combMap 
          on combmap.pnc_tx_stg_cmb_id = comb.pnc_tx_stg_cmb_id
          where comb.study_id = @studyId
          group by comb.pnc_tx_stg_cmb_id
          having count(*) = 1) multiple_ids_combo
          on multiple_ids_combo.pnc_tx_stg_cmb_id = comb.pnc_tx_stg_cmb_id
        ) distinctConcept
      )
  ) adding_concept;

MERGE INTO @results_schema.pnc_tx_stage_combination_map comb_map
USING
  (
    SELECT combo.pnc_tx_stg_cmb_id pnc_tx_stg_cmb_id, combo.concept_id concept_id, combo.concept_name concept_name FROM @results_schema.pnc_tx_stage_combination combo
  ) adding_map
  ON
  (
    comb_map.pnc_tx_stg_cmb_id = adding_map.pnc_tx_stg_cmb_id
  )
WHEN NOT MATCHED THEN INSERT (pnc_tx_stg_cmb_id, concept_id, concept_name)
VALUES (adding_map.pnc_tx_stg_cmb_id, adding_map.concept_id, adding_map.concept_name);

-- Ahh quick workaround to remove sequences!!!
update @results_schema.pnc_tx_stage_combination set concept_id = null, concept_name = null;
