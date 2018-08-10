CREATE OR REPLACE FUNCTION PATIENTS_STRONG_CHECK
  (
    currentJobID NUMERIC DEFAULT -1
  )
  RETURNS NUMERIC AS
$BODY$
DECLARE
  badPatients   VARCHAR(2000);
  rtnCd         INTEGER;
  jobID         NUMERIC(18, 0);
  stepCt        NUMERIC(18, 0);
  databaseName  VARCHAR(100);
  newJobFlag    INTEGER;
  procedureName VARCHAR(100);
BEGIN

  jobID := currentJobID;
  databaseName := current_schema();
  procedureName := 'I2B2_LOAD_SAMPLES';


  IF (jobID IS NULL OR jobID < 1)
  THEN
    newJobFlag := 1; -- True
    SELECT cz_start_audit(procedureName, databaseName)
    INTO jobID;
  END IF;

  stepCt := 1;
  SELECT cz_write_audit(jobId, databaseName, procedureName, 'Starting PATIENTS_STRONG_CHECK jobID '||jobID, 0, stepCt, 'Done')
  INTO rtnCd;

  SELECT string_agg(t.usubjid, ', ')
  INTO badPatients
  FROM
    wt_subject_info t INNER JOIN i2b2demodata.patient_dimension pd
      ON t.usubjid = pd.sourcesystem_cd
  WHERE
    t.sex_cd <> pd.sex_cd OR
    t.age_in_years_num <> pd.age_in_years_num OR
    t.race_cd <> pd.race_cd;

  IF badPatients IS NOT NULL
  THEN
    stepCt := stepCt + 1;
    SELECT cz_write_audit(jobId, databaseName, procedureName, 'New patients set (' || badPatients || ') contain different values from exist in DB', 0, stepCt, 'Done')
    INTO rtnCd;

    SELECT cz_end_audit(jobID, 'FAIL')
    INTO rtnCd;

    RETURN -16;
  END IF;

  stepCt := stepCt + 1;
  SELECT cz_write_audit(jobId, databaseName, procedureName, 'End PATIENTS_STRONG_CHECK', 0, stepCt, 'Done')
  INTO rtnCd;

  IF newJobFlag = 1
  THEN
    SELECT cz_end_audit(jobID, 'SUCCESS')
    INTO rtnCd;
  END IF;

  RETURN 1;

END;

$BODY$
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path FROM CURRENT
COST 100;

