CREATE OR REPLACE
PROCEDURE "I2B2_DELETE_CROSS_DATA"
  (
    path_string        VARCHAR2 := NULL,
    concept_cd_inp     VARCHAR2 := NULL,
    is_delete_concepts INT := 0,
    currentJobID       NUMBER := NULL
  )
AS

  isDeleteConcepts BOOLEAN;
  pCount           INT;
  pathString       VARCHAR2(4000);
  upNode           VARCHAR2(700);

  --Audit variables
  newJobFlag       INTEGER(1);
  databaseName     VARCHAR(100);
  procedureName    VARCHAR(100);
  jobID            NUMBER(18, 0);
  stepCt           NUMBER(18, 0);

    exists_observation EXCEPTION;
    path_not_found EXCEPTION;
    path_for_concept_not_found EXCEPTION;

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

    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName,
                   'Starting ' || procedureName || ' for ''' || coalesce(path_string, '<null>') ||
                   ''' and concept_cd ''' || coalesce(concept_cd_inp, '<null>') || '''', 0, stepCt,
                   'Done');

    --Audit JOB Initialization
    --If Job ID does not exist, then this is a single procedure run and we need to create it
    IF (jobID IS NULL OR jobID < 1)
    THEN
      newJobFlag := 1; -- True
      cz_start_audit(procedureName, databaseName, jobID);
    END IF;

    IF (path_string IS NULL)
    THEN
      IF concept_cd_inp IS NULL
      THEN
        RAISE path_not_found;
      ELSE
        SELECT concept_path
        INTO pathString
        FROM i2b2demodata.concept_dimension
        WHERE concept_cd = concept_cd_inp;

        IF pathString IS NULL
        THEN
          RAISE path_for_concept_not_found;
        END IF;
      END IF;
    ELSE
      pathString := path_string;
    END IF;

    pathString := REGEXP_REPLACE('\' || pathString || '\', '(\\){2,}', '\');
    isDeleteConcepts := is_delete_concepts = 1;

    IF (is_delete_concepts = 1)
    THEN
      stepCt := stepCt + 1;
      cz_write_audit(jobId, databaseName, procedureName,
                     'Remove concept too',
                     0, stepCt, 'Done');
    ELSE
      stepCt := stepCt + 1;
      cz_write_audit(jobId, databaseName, procedureName,
                     'Without remove concept ',
                     0, stepCt, 'Done');
    END IF;

    SELECT count(*)
    INTO pCount
    FROM i2b2demodata.observation_fact
    WHERE concept_cd IN (
      SELECT concept_cd
      FROM i2b2demodata.concept_dimension
      WHERE concept_path LIKE pathString || '%');

    IF (pCount > 0)
    THEN
      RAISE exists_observation;
    END IF;

    DELETE FROM i2b2metadata.i2b2
    WHERE C_FULLNAME LIKE pathString || '%';
    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Delete data for trial from I2B2METADATA i2b2', SQL%ROWCOUNT,
                   stepCt, 'Done');
    COMMIT;

    DELETE FROM i2b2metadata.i2b2_secure
    WHERE C_FULLNAME LIKE pathString || '%';
    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Delete data for trial from I2B2METADATA i2b2_secure',
                   SQL%ROWCOUNT, stepCt, 'Done');
    COMMIT;

    DELETE FROM i2b2metadata.table_access
    WHERE C_FULLNAME LIKE pathString || '%';
    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Delete data I2B2METADATA table_access', SQL%ROWCOUNT, stepCt,
                   'Done');
    COMMIT;

    IF (isDeleteConcepts)
    THEN
      DELETE FROM i2b2demodata.CONCEPT_DIMENSION
      WHERE CONCEPT_PATH LIKE pathString || '%';

      stepCt := stepCt + 1;
      cz_write_audit(jobId, databaseName, procedureName, 'Delete data for trial from I2B2DEMODATA concept_dimension',
                     SQL%ROWCOUNT, stepCt, 'Done');
      COMMIT;
    END IF;

    upNode := substr(pathString, 1, INSTR(rtrim(pathString, '\'), '\', -1));

    IF isDeleteConcepts
    THEN
      SELECT count(*)
      INTO pCount
      FROM i2b2demodata.concept_dimension
      WHERE concept_path LIKE upNode || '%';

      -- Check i2b2 for remove top node, if necessary
      IF pCount = 0
      THEN
        SELECT count(*)
        INTO pCount
        FROM i2b2metadata.i2b2
        WHERE c_fullname LIKE upNode || '%' AND sourcesystem_cd IS NULL;
      END IF;
    ELSE
      SELECT count(*)
      INTO pCount
      FROM i2b2metadata.i2b2
      WHERE c_fullname LIKE upNode || '%';
    END IF;

    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName,
                   'Check if need to remove top node ' || upNode || CASE WHEN pCount = 1
                     THEN ' YES'
                                                                    ELSE ' NO' END,
                   0, stepCt, 'Done');

    IF pCount = 1
    THEN
      I2B2_DELETE_CROSS_DATA(upNode, NULL, is_delete_concepts, jobID);
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
      cz_write_audit(jobId, databasename, procedurename, 'Path string and concept_cd are null', 1, stepCt, 'ERROR');
      cz_error_handler(jobid, procedurename);
      cz_end_audit(jobId, 'FAIL');
    WHEN path_for_concept_not_found
    THEN
      cz_write_audit(jobId, databasename, procedurename, 'Path for concept_cd ' || concept_cd_inp || ' does not find',
                     1, stepCt, 'ERROR');
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
