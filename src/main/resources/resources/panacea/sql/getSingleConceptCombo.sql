select min(comb.pnc_tx_stg_cmb_id) as combo_id, combmap.concept_id as concept_id, combmap.concept_name as concept_name 
from @results_schema.pnc_tx_stage_combination comb
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