SELECT DISTINCT ptstg.person_id AS person_id
FROM @pnc_ptstg_ct ptstg
WHERE job_execution_id = @jobExecId