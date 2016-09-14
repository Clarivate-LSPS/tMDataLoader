CREATE OR REPLACE FUNCTION COPY_SECURITY_FROM_DIFF_STUDY(studyid      CHARACTER VARYING, studyidfrom CHARACTER VARYING,
                                                        currentjobid NUMERIC)
  RETURNS INTEGER
SET search_path FROM CURRENT
AS $BODY$
DECLARE
  bioDataId      BIGINT;
  secureObjectId BIGINT;

  newJobFlag     INTEGER;
  databaseName   VARCHAR(100);
  procedureName  VARCHAR(100);
  jobID          NUMERIC(18, 0);
  stepCt         NUMERIC(18, 0);
  rowCt          NUMERIC(18, 0);
  rtnCD          NUMERIC(18, 0);
  errorNumber    CHARACTER VARYING;
  errorMessage   CHARACTER VARYING;

BEGIN

  stepCt := 0;

  newJobFlag := 0; -- False (Default)
  jobID := currentJobID;

  databaseName := current_schema();
  procedureName := 'COPY_SECURITY_FROM_DIFF_STUDY';

  IF (jobID IS NULL OR jobID < 1)
  THEN
    newJobFlag := 1; -- True
    SELECT cz_start_audit(procedureName, databaseName)
    INTO jobId;
  END IF;

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
      nextval('searchapp.seq_search_data_id'),
      auth_principal_id,
      secureObjectId,
      secure_access_level_id
    FROM searchapp.search_auth_sec_object_access
    WHERE secure_object_id = (SELECT search_secure_object_id
                              FROM searchapp.search_secure_object
                              WHERE bio_data_unique_id = 'EXP:' || studyidfrom);

  IF newJobFlag = 1
  THEN
    SELECT cz_end_audit(jobID, 'SUCCESS')
    INTO rtnCD;
  END IF;

  RETURN 1;
END;

$BODY$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path FROM CURRENT
COST 100;

ALTER FUNCTION COPY_SECURITY_FROM_DIFF_STUDY( CHARACTER VARYING, CHARACTER VARYING, NUMERIC )
OWNER TO postgres;