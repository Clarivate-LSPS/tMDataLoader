CREATE OR REPLACE
PROCEDURE "I2B2_DELETE_CROSS_DATA"
  (
    path_string        VARCHAR2 := NULL,
    is_delete_concepts INT := 0,
    currentJobID       NUMBER := NULL
  )
AS

  isDeleteConcepts BOOLEAN;
  pCount           INT;
  pathString       VARCHAR2(4000);

  --Audit variables
  newJobFlag       INTEGER(1);
  databaseName     VARCHAR(100);
  procedureName    VARCHAR(100);
  jobID            NUMBER(18, 0);
  stepCt           NUMBER(18, 0);

    exists_observation EXCEPTION;
    path_not_found EXCEPTION;

  BEGIN
    --Set Audit Parameters
    newJobFlag := 0; -- False (Default)
    jobID := currentJobID;
    stepCt := 0;
    pCount := 0;

    SELECT sys_context('USERENV', 'CURRENT_SCHEMA')
    INTO databaseName
    FROM dual;
    procedureName := $$PLSQL_UNIT;

    --Audit JOB Initialization
    --If Job ID does not exist, then this is a single procedure run and we need to create it
    IF (jobID IS NULL OR jobID < 1)
    THEN
      newJobFlag := 1; -- True
      cz_start_audit(procedureName, databaseName, jobID);
    END IF;

    IF (path_string IS NULL)
    THEN
      RAISE path_not_found;
    ELSE
      pathString := REGEXP_REPLACE('\' || path_string || '\', '(\\){2,}', '\');
      isDeleteConcepts := is_delete_concepts = 1;
    END IF;

    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Starting ' || procedureName || ' for ' || pathString, 0, stepCt,
                   'Done');

    SELECT count(*)
    INTO pCount
    FROM i2b2demodata.observation_fact
    WHERE concept_cd IN (
      SELECT concept_cd
      FROM i2b2demodata.concept_dimension
      WHERE concept_path LIKE pathString || '%' ESCAPE '`');

    IF (pCount > 0)
    THEN
      RAISE exists_observation;
    END IF;

    DELETE FROM i2b2metadata.i2b2
    WHERE C_FULLNAME LIKE pathString || '%' ESCAPE '`';
    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Delete data for trial from I2B2METADATA i2b2', SQL%ROWCOUNT,
                   stepCt, 'Done');
    COMMIT;

    DELETE FROM i2b2metadata.i2b2_secure
    WHERE C_FULLNAME LIKE pathString || '%' ESCAPE '`';
    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Delete data for trial from I2B2METADATA i2b2_secure',
                   SQL%ROWCOUNT, stepCt, 'Done');
    COMMIT;

    DELETE FROM i2b2metadata.table_access
    WHERE C_FULLNAME LIKE pathString || '%' ESCAPE '`';
    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Delete data I2B2METADATA table_access', SQL%ROWCOUNT, stepCt,
                   'Done');
    COMMIT;

    IF (isDeleteConcepts)
    THEN
      DELETE FROM i2b2demodata.CONCEPT_DIMENSION
      WHERE CONCEPT_PATH LIKE pathString || '%' ESCAPE '`';

      stepCt := stepCt + 1;
      cz_write_audit(jobId, databaseName, procedureName, 'Delete data for trial from I2B2DEMODATA concept_dimension',
                     SQL%ROWCOUNT, stepCt, 'Done');
      COMMIT;
    END IF;

    ---Cleanup OVERALL JOB if this proc is being run standalone
    IF newJobFlag = 1
    THEN
      cz_end_audit(jobID, 'SUCCESS');
    END IF;

    EXCEPTION
    WHEN exists_observation
    THEN
      cz_write_audit(jobId, databasename, procedurename, 'Exists not deleting study', 1, stepCt, 'ERROR');
      cz_error_handler(jobid, procedurename);
      cz_end_audit(jobId, 'FAIL');
    WHEN path_not_found
    THEN
      cz_write_audit(jobId, databasename, procedurename, 'Path was not found for this trial id', 1, stepCt, 'ERROR');
      cz_error_handler(jobid, procedurename);
      cz_end_audit(jobId, 'FAIL');
    WHEN OTHERS
    THEN
      --Handle errors.
      cz_error_handler(jobID, procedureName);
      --End Proc
      cz_end_audit(jobID, 'FAIL');

  END;
/
