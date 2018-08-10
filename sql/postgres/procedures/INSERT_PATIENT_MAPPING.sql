CREATE OR REPLACE FUNCTION INSERT_PATIENT_MAPPING
  (
    shared_patients      CHARACTER VARYING DEFAULT NULL,
    currentJobID NUMERIC DEFAULT -1
  )
  RETURNS NUMERIC AS
$BODY$
DECLARE
  rtnCd         INTEGER;
  jobID         NUMERIC(18, 0);
  stepCt        NUMERIC(18, 0);
  databaseName  VARCHAR(100);
  newJobFlag    INTEGER;
  procedureName VARCHAR(100);
  rowCt			numeric(18,0);
  errorNumber		character varying;
  errorMessage	character varying;

BEGIN

  jobID := currentJobID;
  databaseName := current_schema();
  procedureName := 'INSERT_PATIENT_MAPPING';


  IF (jobID IS NULL OR jobID < 1)
  THEN
    newJobFlag := 1; -- True
    SELECT cz_start_audit(procedureName, databaseName)
    INTO jobID;
  END IF;

  stepCt := 1;
  SELECT cz_write_audit(jobId, databaseName, procedureName, 'Starting INSERT_PATIENT_MAPPING', 0, stepCt, 'Done')
  INTO rtnCd;

  IF shared_patients IS NOT NULL
  THEN
    BEGIN

      INSERT INTO i2b2demodata.patient_mapping (
        patient_ide,
        patient_ide_source,
        patient_num,
        patient_ide_status
      ) SELECT
          sourcesystem_cd,
          'SUBJ_ID',
          patient_num,
          'ACTIVE'
        FROM i2b2demodata.patient_dimension pd
        WHERE
          sourcesystem_cd LIKE shared_patients || ':%'
          AND NOT exists(SELECT 1
                         FROM i2b2demodata.patient_mapping pd2
                         WHERE pd.sourcesystem_cd = pd2.patient_ide);

      GET DIAGNOSTICS rowCt := ROW_COUNT;
      EXCEPTION
      WHEN OTHERS
        THEN
          errorNumber := SQLSTATE;
          errorMessage := SQLERRM;
          --Handle errors.
          SELECT cz_error_handler(jobID, procedureName, errorNumber, errorMessage)
          INTO rtnCd;
          --End Proc
          SELECT cz_end_audit(jobID, 'FAIL')
          INTO rtnCd;
          RETURN -16;
    END;
    stepCt := stepCt + 1;
    SELECT cz_write_audit(jobId, databaseName, procedureName, 'Insert new subjects into patient_mapping', rowCt, stepCt,
                          'Done')
    INTO rtnCd;
  END IF;

  stepCt := stepCt + 1;
  SELECT cz_write_audit(jobId, databaseName, procedureName, 'End INSERT_PATIENT_MAPPING', 0, stepCt, 'Done')
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

