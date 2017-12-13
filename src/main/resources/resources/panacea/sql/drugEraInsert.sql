
--from drug_era
INSERT INTO @pnc_ptsq_ct (
	job_execution_id
	, study_id
	, person_id
	, source_id
	, concept_id
	, concept_name
	, idx_start_date
	, idx_end_date
	, duration_days
	, tx_seq)
SELECT DISTINCT 
	@jobExecId as job_execution_id
	, @studyId AS study_id
	, myCohort.person_id AS person_id
	, @sourceId AS source_id
	, era.drug_concept_id
	, myConcept.concept_name
	, era.drug_era_start_date
	, era.drug_era_end_date
	, DATEDIFF(DAY, era.drug_era_start_date, era.drug_era_end_date) + 1
  , rank() OVER (PARTITION BY myCohort.person_id ORDER BY myCohort.person_id, era.drug_era_start_date, era.drug_era_end_date, era.drug_concept_id) real_tx_seq
from (
	SELECT DISTINCT COHORT_DEFINITION_ID COHORT_DEFINITION_ID, subject_id person_id, COHORT_START_DATE cohort_start_date, cohort_end_date cohort_end_date 
	FROM @ohdsi_schema.cohort
  WHERE COHORT_DEFINITION_ID = @cohortDefId
)  myCohort
INNER JOIN @cdm_schema.drug_era era   
  ON myCohort.person_id = era.person_id
  AND era.drug_concept_id in (@drugConceptId)
  AND (era.DRUG_ERA_START_DATE > myCohort.COHORT_START_DATE OR era.DRUG_ERA_START_DATE = myCohort.COHORT_START_DATE) 
  AND (era.DRUG_ERA_START_DATE < DATEADD(day, @STUDY_DURATION, myCohort.COHORT_START_DATE) OR era.DRUG_ERA_START_DATE = DATEADD(day, @STUDY_DURATION, myCohort.COHORT_START_DATE)) 
  @drugEraStudyOptionalDateConstraint
INNER JOIN @cdm_schema.concept myConcept
	ON era.drug_concept_id = myConcept.concept_id
WHERE myCohort.COHORT_DEFINITION_ID = @COHORT_DEFINITION_ID
;