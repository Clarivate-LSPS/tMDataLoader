-- Function: i2b2_delete_all_nodes(character varying, numeric)

-- DROP FUNCTION i2b2_delete_all_nodes(character varying, numeric);

CREATE OR REPLACE FUNCTION i2b2_delete_all_nodes(
  path         CHARACTER VARYING,
  currentjobid NUMERIC DEFAULT '-1' :: INTEGER)
  RETURNS NUMERIC AS
$BODY$
/*************************************************************************
* Copyright 2008-2012 Janssen Research & Development, LLC.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
******************************************************************/
DECLARE

  --Audit variables
  newJobFlag    INTEGER;
  databaseName  VARCHAR(100);
  procedureName VARCHAR(100);
  jobID         NUMERIC(18, 0);
  stepCt        NUMERIC(18, 0);
  rowCt         NUMERIC(18, 0);
  errorNumber   CHARACTER VARYING;
  errorMessage  CHARACTER VARYING;
  rtnCd         NUMERIC;
  trialVisitNum NUMERIC;
  trialVisitNumN NUMERIC;
BEGIN

  --Set Audit Parameters
  newJobFlag := 0; -- False (Default)
  jobID := currentJobID;

  databaseName := current_schema();
  procedureName := 'I2B2_DELETE_ALL_NODES';

  --Audit JOB Initialization
  --If Job ID does not exist, then this is a single procedure run and we need to create it
  IF (jobID IS NULL OR jobID < 1)
  THEN
    newJobFlag := 1; -- True
    SELECT cz_start_audit(procedureName, databaseName)
    INTO jobID;
  END IF;

  IF path != '' AND path != '%'
  THEN
    -- observation_fact
    SELECT count(DISTINCT trial_visit_num)
    INTO trialVisitNumN
    FROM i2b2demodata.OBSERVATION_FACT
    WHERE concept_cd IN (
      SELECT concept_cd
      FROM i2b2demodata.CONCEPT_DIMENSION
      WHERE concept_path LIKE path || '%' ESCAPE '`' AND sourcesystem_cd IS NOT NULL
    );
    IF (trialVisitNumN != 1)
    THEN
      stepCt := stepCt + 1;
      SELECT cz_write_audit(jobId, databaseName, procedureName, 'You can only specify a path for a single study.', rowCt, stepCt, 'Warning')
      INTO rtnCd;
    ELSE
      SELECT DISTINCT trial_visit_num
      INTO trialVisitNum
      FROM i2b2demodata.OBSERVATION_FACT
      WHERE concept_cd IN (
        SELECT concept_cd
        FROM i2b2demodata.CONCEPT_DIMENSION
        WHERE concept_path LIKE PATH || '%' ESCAPE '`' AND sourcesystem_cd IS NOT NULL
      );
      stepCt := stepCt + 1;
      SELECT cz_write_audit(jobId, databaseName, procedureName, 'Found trial_visit_num', rowCt, stepCt, 'Done')
      INTO rtnCd;
      BEGIN
        DELETE FROM i2b2demodata.OBSERVATION_FACT
        WHERE
          concept_cd IN (SELECT C_BASECODE
                         FROM i2b2metadata.I2B2
                         WHERE C_FULLNAME LIKE PATH || '%' ESCAPE '`' AND sourcesystem_cd IS NOT NULL)
          AND trial_visit_num = trialVisitNum;
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
        cz_write_audit(jobId, databaseName, procedureName, 'Delete data for trial from I2B2DEMODATA observation_fact',
                       rowCt, stepCt, 'Done')
      INTO rtnCd;
    END IF;

    --CONCEPT DIMENSION
    BEGIN
      DELETE FROM i2b2demodata.CONCEPT_DIMENSION cd
      WHERE CONCEPT_PATH LIKE path || '%' ESCAPE '`' AND (cd.sourcesystem_cd IS NOT NULL
                                                          OR exists(SELECT 1
                                                                    FROM i2b2metadata.i2b2
                                                                    WHERE c_fullname = cd.concept_path AND
                                                                          char_length(c_visualattributes) > 2));
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

    --I2B2
    BEGIN
      DELETE FROM i2b2metadata.i2b2
      WHERE C_FULLNAME LIKE PATH || '%' ESCAPE '`';
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
      cz_write_audit(jobId, databaseName, procedureName, 'Delete data for trial from I2B2METADATA i2b2', rowCt, stepCt,
                     'Done')
    INTO rtnCd;

    --i2b2_secure
    BEGIN
      DELETE FROM i2b2metadata.i2b2_secure
      WHERE C_FULLNAME LIKE PATH || '%' ESCAPE '`';
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

    --concept_counts
    BEGIN
      DELETE FROM i2b2demodata.concept_counts
      WHERE concept_path LIKE PATH || '%' ESCAPE '`';
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
    SELECT cz_write_audit(jobId, databaseName, procedureName, 'Delete data for trial from I2B2DEMODATA concept_counts',
                          rowCt, stepCt, 'Done')
    INTO rtnCd;

  END IF;

  RETURN 1;
END;

$BODY$
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
COST 100;
