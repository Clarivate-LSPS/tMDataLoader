CREATE OR REPLACE PROCEDURE COPY_SECURITY_FROM_OTHER_STUDY(studyid      VARCHAR2, studyidfrom VARCHAR2,
                                                          currentjobid NUMERIC := NULL)
AS

  bioDataId      NUMBER(18, 0);
  secureObjectId NUMBER(18, 0);

  newJobFlag     INTEGER;
  databaseName   VARCHAR(100);
  procedureName  VARCHAR(100);
  jobID          NUMERIC(18, 0);
  stepCt         NUMERIC(18, 0);
  rtnCode        INT;

  BEGIN

    stepCt := 0;

    newJobFlag := 0; -- False (Default)
    jobID := currentJobID;

    SELECT sys_context('USERENV', 'CURRENT_SCHEMA')
    INTO databaseName
    FROM dual;
    procedureName := $$PLSQL_UNIT;

    IF (jobID IS NULL OR jobID < 1)
    THEN
      newJobFlag := 1; -- True
      cz_start_audit(procedureName, databaseName, jobID);
    END IF;

    cz_write_audit(jobId, databaseName, procedureName, 'Start '|| procedureName || ' ' || studyid || ' ' || studyidfrom, 0,
                   stepCt, 'Done');


    SELECT bio_experiment_id
    INTO bioDataId
    FROM biomart.bio_experiment
    WHERE accession = studyid;

    SELECT search_secure_object_id
    INTO secureObjectId
    FROM searchapp.search_secure_object
    WHERE bio_data_unique_id = (SELECT unique_id
                                FROM biomart.bio_data_uid
                                WHERE bio_data_id = bioDataId);

    INSERT INTO searchapp.search_auth_sec_object_access
    (auth_sec_obj_access_id, auth_principal_id, secure_object_id, secure_access_level_id)
      SELECT
        SEQ_SEARCH_DATA_ID.nextval,
        auth_principal_id,
        secureObjectId,
        secure_access_level_id
      FROM searchapp.search_auth_sec_object_access
      WHERE secure_object_id = (SELECT search_secure_object_id
                                FROM searchapp.search_secure_object
                                WHERE bio_data_unique_id = 'EXP:' || studyidfrom);
    COMMIT;
    cz_write_audit(jobId, databaseName, procedureName, 'Inserted into search_auth_sec_object_access', SQL%ROWCOUNT,
                   stepCt, 'Done');

    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'End i2b2_load_clinical_data', 0, stepCt, 'Done');

    IF newJobFlag = 1
    THEN
      cz_end_audit(jobID, 'SUCCESS');
    END IF;

    rtnCode := 0;

    EXCEPTION
    WHEN OTHERS THEN
    --Handle errors.
    cz_error_handler(jobID, procedureName);
    --End Proc
    cz_end_audit(jobID, 'FAIL');

  END;
/
