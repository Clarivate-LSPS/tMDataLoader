CREATE OR REPLACE FUNCTION I2B2_DELETE_CROSS_DATA
  (
    path_string        VARCHAR,
    concept_cd_inp     VARCHAR,
    is_delete_concepts INTEGER,
    currentJobID       NUMERIC DEFAULT -1
  )
  RETURNS NUMERIC AS
$BODY$
DECLARE
  pathString       VARCHAR(700);
  pCount           INTEGER;
  isDeleteConcepts BOOLEAN;
  upNode           VARCHAR(700);

  --Audit variables
  rowCt            NUMERIC(18, 0);
  databaseName     VARCHAR(100);
  procedureName    VARCHAR(100);
  jobID            NUMERIC(18, 0);
  stepCt           NUMERIC(18, 0);
  rtnCd            INTEGER;
  errorNumber  CHARACTER VARYING;
  errorMessage CHARACTER VARYING;

BEGIN
  databaseName := current_schema();
  procedureName := 'I2B2_DELETE_CROSS_DATA';

  SELECT CASE WHEN coalesce(currentjobid, -1) < 1
    THEN cz_start_audit(procedureName, databaseName)
         ELSE currentjobid END
  INTO jobId;
  stepCt := 1;

  SELECT cz_write_audit(jobId, databaseName, procedureName,
                        'Starting ' || procedureName || ' for ''' || coalesce(path_string, '<null>') ||
                        ''' and concept_cd ''' || coalesce(concept_cd_inp, '<null>') || '''',
                        0, stepCt, 'Done')
  INTO rtnCd;

  IF (path_string IS NULL)
  THEN
    IF concept_cd_inp IS NULL
    THEN
      stepCt := stepCt + 1;
      SELECT
        cz_write_audit(jobId, databasename, procedurename, 'Path string and concept_cd are null', 1, stepCt, 'ERROR')
      INTO rtnCd;
      SELECT cz_error_handler(jobid, procedurename, '-1', 'Application raised error')
      INTO rtnCd;
      SELECT cz_end_audit(jobId, 'FAIL')
      INTO rtnCd;
      RETURN -16;
    ELSE
      SELECT concept_path
      INTO pathString
      FROM i2b2demodata.concept_dimension
      WHERE concept_cd = concept_cd_inp;

      IF pathString IS NULL
      THEN
        stepCt := stepCt + 1;
        SELECT cz_write_audit(jobId, databasename, procedurename,
                              'Path for concept_cd ' || concept_cd_inp || ' does not find',
                              1, stepCt, 'ERROR')
        INTO rtnCd;
        SELECT cz_error_handler(jobid, procedurename, '-1', 'Application raised error')
        INTO rtnCd;
        SELECT cz_end_audit(jobId, 'FAIL')
        INTO rtnCd;
        RETURN -16;
      END IF;
    END IF;
  ELSE
    pathString := path_string;
  END IF;

  pathString := REGEXP_REPLACE('\' || pathString || '\', '(\\){2,}', '\', 'g');
  isDeleteConcepts := is_delete_concepts = 1;

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

  upNode := substring(pathString FROM 1 FOR (length(pathString) - position('\' IN reverse(rtrim(pathString, '\')))));

  IF isDeleteConcepts
  THEN
    SELECT count(*)
    INTO pCount
    FROM i2b2demodata.concept_dimension
    WHERE concept_path like upNode || '%' escape '`';

    IF pCount = 0
    THEN
      SELECT count(*)
      INTO pCount
      FROM i2b2metadata.table_access
      WHERE c_fullname LIKE upNode || '%' ESCAPE '`';
    END IF;
  ELSE
    SELECT count(*)
    INTO pCount
    FROM i2b2metadata.i2b2
    WHERE c_fullname like upNode || '%' escape '`';
  END IF;

  stepCt := stepCt + 1;
  SELECT cz_write_audit(jobId, databaseName, procedureName,
                        'Check if need to remove top node ' || upNode || CASE WHEN pCount = 1
                          THEN ' YES'
                                                                         ELSE ' NO' END,
                        0, stepCt, 'Done')
  INTO rtnCd;

  IF pCount = 1
  THEN
    SELECT I2B2_DELETE_CROSS_DATA(upNode, NULL, is_delete_concepts, jobID)
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
