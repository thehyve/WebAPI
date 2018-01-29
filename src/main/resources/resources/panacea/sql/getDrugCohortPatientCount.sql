SELECT DISTINCT era.person_id person_id, era.drug_concept_id drug_concept_id, myConcept.concept_name concept_name, 
      era.drug_era_start_date drug_era_start_date, era.drug_era_end_date drug_era_end_date, study.study_id study_id, era.drug_era_end_date - era.drug_era_start_date duration
      FROM 
          @cds_schema.drug_era era, 
          (SELECT  DISTINCT subject_id, COHORT_START_DATE, cohort_end_date FROM @ohdsi_schema.cohort WHERE COHORT_DEFINITION_ID = @cohortDefId AND subject_id = 2000000030415658) myCohort,
          (SELECT concept_name, concept_id FROM @cds_schema.concept) myConcept,
          (SELECT cohort_definition_id, study_duration, start_date, end_date, study_id FROM @ohdsi_schema.panacea_study WHERE study_id = @studyId) study
      WHERE 
        myCohort.cohort_start_date < era.drug_era_start_date
        AND era.drug_era_start_date < myCohort.cohort_end_date
        AND myCohort.cohort_start_date > study.start_date
        AND myCohort.cohort_start_date < study.end_date
        AND myCohort.cohort_end_date < study.end_date
        AND era.drug_era_start_date > study.start_date
        AND era.drug_era_start_date < study.end_date
        AND era.drug_era_end_date < (era.drug_era_start_date + study.study_duration)
        AND myCohort.subject_id = era.person_id
        AND drug_concept_id in (@drugConceptId)
        AND era.drug_concept_id = myConcept.concept_id
      ORDER BY person_id, drug_era_start_date