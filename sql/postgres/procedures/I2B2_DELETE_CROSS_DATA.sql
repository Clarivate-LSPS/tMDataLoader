CREATE OR REPLACE FUNCTION I2B2_DELETE_CROSS_DATA
  (
    path_string        VARCHAR,
    is_delete_concepts INTEGER,
    currentJobID       NUMERIC DEFAULT -1
  )
  RETURNS NUMERIC AS
$BODY$
DECLARE
  pathString       VARCHAR(700);
  pCount           INTEGER;
  isDeleteConcepts BOOLEAN;

  --Audit variables
  rowCt            NUMERIC(18, 0);
  databaseName     VARCHAR(100);
  procedureName    VARCHAR(100);
  jobID            NUMERIC(18, 0);
  stepCt           NUMERIC(18, 0);
  rtnCd            INTEGER;
  errorNumber      CHARACTER VARYING;
  errorMessage CHARACTER VARYING;

BEGIN
  databaseName := current_schema();
  procedureName := 'I2B2_DELETE_CROSS_DATA';

  SELECT CASE WHEN coalesce(currentjobid, -1) < 1
    THEN cz_start_audit(procedureName, databaseName)
         ELSE currentjobid END
  INTO jobId;
  stepCt := 0;

  IF (path_string IS NULL)
  THEN
    stepCt := stepCt + 1;
    SELECT cz_write_audit(jobId, databasename, procedurename, 'Path string and study id are null', 1, stepCt, 'ERROR')
    INTO rtnCd;
    SELECT cz_error_handler(jobid, procedurename, '-1', 'Application raised error')
    INTO rtnCd;
    SELECT cz_end_audit(jobId, 'FAIL')
    INTO rtnCd;
    RETURN -16;
  END IF;

  pathString := path_string;
  pathString := REGEXP_REPLACE('\' || pathString || '\', '(\\){2,}', '\', 'g');
  isDeleteConcepts := is_delete_concepts = 1;

  stepCt := stepCt + 1;
  SELECT cz_write_audit(jobId, databaseName, procedureName,
                        'Starting ' || procedureName || ' for ''' || pathString || '''',
                        0, stepCt, 'Done')
  INTO rtnCd;
  IF (is_delete_concepts = 1)
  THEN
    stepCt := stepCt + 1;
    SELECT cz_write_audit(jobId, databaseName, procedureName,
                          'Remove concept too',
                          0, stepCt, 'Done')
    INTO rtnCd;
    ELSE
      stepCt := stepCt + 1;
      SELECT cz_write_audit(jobId, databaseName, procedureName,
                            'Without remove concept ',
                            0, stepCt, 'Done')
      INTO rtnCd;
  END IF;

  --Checking exists observation_fact
  SELECT count(*)
  INTO pCount
  FROM i2b2demodata.observation_fact
  WHERE concept_cd IN (
    SELECT concept_cd
    FROM i2b2demodata.concept_dimension
    WHERE concept_path LIKE pathString || '%' ESCAPE '`');

  IF (pCount > 0)
  THEN
    stepCt := stepCt + 1;
    SELECT cz_write_audit(jobId, databasename, procedurename, 'Exists not deleting study', 1, stepCt, 'ERROR')
    INTO rtnCd;
    SELECT cz_error_handler(jobid, procedurename, '-1', 'Application raised error')
    INTO rtnCd;
    SELECT cz_end_audit(jobId, 'FAIL')
    INTO rtnCd;
    RETURN -16;
  END IF;

  BEGIN
    DELETE FROM i2b2metadata.i2b2
    WHERE C_FULLNAME LIKE pathString || '%' ESCAPE '`';
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
  GET DIAGNOSTICS rowCt := ROW_COUNT;
  SELECT cz_write_audit(jobId, databaseName, procedureName, 'Delete data for trial from I2B2METADATA i2b2', rowCt,
                        stepCt, 'Done')
  INTO rtnCd;

  BEGIN
    DELETE FROM i2b2metadata.i2b2_secure
    WHERE C_FULLNAME LIKE pathString || '%' ESCAPE '`';
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
  GET DIAGNOSTICS rowCt := ROW_COUNT;
  SELECT
    cz_write_audit(jobId, databaseName, procedureName, 'Delete data for trial from I2B2METADATA i2b2_secure', rowCt,
                   stepCt, 'Done')
  INTO rtnCd;

  BEGIN
    DELETE FROM i2b2metadata.table_access
    WHERE C_FULLNAME LIKE pathString || '%' ESCAPE '`';
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
  GET DIAGNOSTICS rowCt := ROW_COUNT;
  SELECT cz_write_audit(jobId, databaseName, procedureName, 'Delete data I2B2METADATA table_access', rowCt,
                        stepCt, 'Done')
  INTO rtnCd;

  IF (isDeleteConcepts)
  THEN
    BEGIN
      DELETE FROM i2b2demodata.CONCEPT_DIMENSION
      WHERE CONCEPT_PATH LIKE pathString || '%' ESCAPE '`';
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
    SELECT
      cz_write_audit(jobId, databaseName, procedureName, 'Delete data for trial from I2B2DEMODATA concept_dimension',
                     rowCt, stepCt, 'Done')
    INTO rtnCd;

  END IF;

  PERFORM cz_end_audit(jobID, 'SUCCESS')
  WHERE coalesce(currentJobId, -1) <> jobId;

  RETURN 1;
END;
$BODY$
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path FROM CURRENT
COST 100;
