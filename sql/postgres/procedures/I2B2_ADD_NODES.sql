CREATE OR REPLACE FUNCTION I2B2_ADD_NODES(trialid CHARACTER VARYING,new_paths TEXT[], currentjobid NUMERIC)
RETURNS NUMERIC
SET search_path FROM CURRENT
AS $BODY$
  DECLARE
    -- audit variables
    rtnCd			    INTEGER;
    newJobFlag		INTEGER;
    databaseName 	VARCHAR(100);
    procedureName VARCHAR(100);
    jobID 			  NUMERIC(18,0);
    errorNumber		CHARACTER VARYING;
    errorMessage	CHARACTER VARYING;
    stepCt 			  NUMERIC(18,0);
    rowCt			    NUMERIC(18,0);
    headNode VARCHAR(4000);
  BEGIN

    -- audit init
    newJobFlag := 0; -- False (Default)
    jobID := currentJobID;

    databaseName := current_schema();
    procedureName := 'I2B2_ADD_NODES';

    --If Job ID does not exist, then this is a single procedure run and we need to create it
    IF(jobID IS NULL or jobID < 1)
    THEN
      newJobFlag := 1; -- True
      select cz_start_audit (procedureName, databaseName) into jobId;
    END IF;

    stepCt := 0;
    stepCt := stepCt + 1;

    IF array_length(new_paths, 1) = 0 THEN
      select cz_write_audit(jobId, databaseName, procedureName, 'Paths array is empty', 0,stepCt, 'Done') into rtnCd;
      RETURN 1;
    END IF;

    -- Delete existing node
    DELETE FROM i2b2demodata.OBSERVATION_FACT fact
      USING i2b2metadata.I2B2 i2b2
      WHERE i2b2.c_fullname = ANY(new_paths)
            AND fact.concept_cd = i2b2.c_basecode;

    DELETE FROM i2b2demodata.CONCEPT_DIMENSION
      WHERE concept_path = ANY(new_paths);

    SELECT c_fullname INTO headNode FROM i2b2metadata.i2b2
    WHERE c_fullname = ANY(new_paths)
        AND c_visualattributes = 'FAS';

    select cz_write_audit(jobId, databaseName, procedureName, 'headNode ' || headNode, 0,stepCt, 'Done') into rtnCd;
    DELETE FROM i2b2metadata.I2B2
      WHERE c_fullname = ANY(new_paths);

    -- Insert new node
    INSERT INTO i2b2demodata.CONCEPT_DIMENSION
    (CONCEPT_CD,
     CONCEPT_PATH,
     NAME_CHAR,
     UPDATE_DATE,
     DOWNLOAD_DATE,
     IMPORT_DATE,
     SOURCESYSTEM_CD)
    SELECT cast(nextval('i2b2demodata.concept_id') as varchar),
      path,
      trim(both '\' from substring(path, '\\[^\\]+\\$')),
      current_timestamp,
      current_timestamp,
      current_timestamp,
      trialid
    FROM unnest(new_paths) path;

    INSERT INTO i2b2metadata.I2B2
    (c_hlevel, C_FULLNAME, C_NAME, C_VISUALATTRIBUTES, c_synonym_cd, C_FACTTABLECOLUMN, C_TABLENAME,
     C_COLUMNNAME,C_DIMCODE, C_TOOLTIP, UPDATE_DATE, DOWNLOAD_DATE, IMPORT_DATE, SOURCESYSTEM_CD,
     c_basecode, C_OPERATOR, c_columndatatype, c_comment, m_applied_path)
    SELECT
      i2b2_get_hlevel(concept_path),
      CONCEPT_PATH,
      NAME_CHAR,
      case CONCEPT_PATH when headNode then 'FAS' else 'FA' END,
      'N',
      'CONCEPT_CD',
      'CONCEPT_DIMENSION',
      'CONCEPT_PATH',
      CONCEPT_PATH,
      CONCEPT_PATH,
      current_timestamp,
      current_timestamp,
      current_timestamp,
      SOURCESYSTEM_CD,
      CONCEPT_CD,
      'LIKE',
      'T',
      case when trialid is null then null else 'trial:' || trialid end,
      '@'
    FROM i2b2demodata.CONCEPT_DIMENSION
    WHERE
      CONCEPT_PATH = ANY(new_paths);

    ---Cleanup OVERALL JOB if this proc is being run standalone
    IF newJobFlag = 1
    THEN
      select cz_end_audit (jobID, 'SUCCESS') into rtnCD;
    END IF;

    RETURN 1;

    EXCEPTION
    WHEN OTHERS THEN
      errorNumber := SQLSTATE;
      errorMessage := SQLERRM;
      --Handle errors.
      select cz_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
      --End Proc
      select cz_end_audit (jobID, 'FAIL') into rtnCd;
      return -16;

  END;
$BODY$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path FROM CURRENT
COST 100;

ALTER FUNCTION I2B2_ADD_NODES(CHARACTER VARYING, TEXT[], NUMERIC) OWNER TO postgres;
