delete from @results_schema.pnc_tmp_pt_sq_ct where study_id = @study_id;

insert into @results_schema.pnc_tmp_pt_sq_ct 
	(pnc_pt_sq_ct_id, study_id, person_id, concept_id, concept_name, idx_start_date, idx_end_date, duration_days)
  select seq_pnc_tmp_pt_sq_ct.nextval, 2, innerView.person_id, innerView.drug_concept_id, innerView.concept_name, 
  innerView.drug_era_start_date, innerView.drug_era_end_date, innerView.drug_era_end_date - innerView.drug_era_start_date
  from 
  (select distinct era.person_id person_id, era.drug_concept_id drug_concept_id, myConcept.concept_name concept_name, 
      era.drug_era_start_date drug_era_start_date, era.drug_era_end_date drug_era_end_date
      from 
          omopv5_de.drug_era era, 
          (select  distinct subject_id, COHORT_START_DATE, cohort_end_date from cohort where COHORT_DEFINITION_ID = 915) myCohort,
          (select concept_name, concept_id from omopv5_de.concept) myConcept
      where 
        myCohort.cohort_start_date < era.drug_era_start_date
        and era.drug_era_start_date < myCohort.cohort_end_date
        and myCohort.cohort_start_date > to_date('01/01/2009', 'MM/DD/YYYY')
        and myCohort.cohort_start_date < to_date('01/01/2013', 'MM/DD/YYYY')
        and myCohort.cohort_end_date < to_date('01/01/2013', 'MM/DD/YYYY')
        and era.drug_era_start_date > to_date('01/01/2009', 'MM/DD/YYYY')
        and era.drug_era_start_date < to_date('01/01/2013', 'MM/DD/YYYY')
        and era.drug_era_end_date < (era.drug_era_start_date + 1095)
        and myCohort.subject_id = era.person_id
        and drug_concept_id in (1301025,1328165,1771162,19058274,918906,923645,933724,1310149,1125315)
        and era.drug_concept_id = myConcept.concept_id
      order by person_id, drug_era_start_date) innerView;
