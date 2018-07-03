--
-- Type: PROCEDURE; Owner: TM_DATALOADER; Name: I2B2_DELETE_1_NODE
--
CREATE OR REPLACE PROCEDURE "I2B2_DELETE_1_NODE"
  (
      path         VARCHAR2
    , currentJobID NUMBER := -1
  ) AUTHID CURRENT_USER
AS
  /*************************************************************************
  * Copyright 2008-2012 Janssen Research + Development, LLC.
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

  --Audit variables
  newJobFlag    INTEGER(1);
  databaseName  VARCHAR(100);
  procedureName VARCHAR(100);
  jobID         NUMBER(18, 0);
  stepCt        NUMBER(18, 0);
  trialId       VARCHAR2(100);
  srcSysCD      VARCHAR2(100);

    has_cross_node EXCEPTION;
begin
  --Set Audit Parameters
  newJobFlag := 0; -- False (Default)
  jobID := currentJobID;

  SELECT sys_context('USERENV', 'CURRENT_SCHEMA') INTO databaseName FROM dual;
  procedureName := $$PLSQL_UNIT;

  --Audit JOB Initialization
  --If Job ID does not exist, then this is a single procedure run and we need to create it
  IF(jobID IS NULL or jobID < 1)
  THEN
    newJobFlag := 1; -- True
    cz_start_audit (procedureName, databaseName, jobID);
  END IF;

  stepCt := 0;
  if coalesce(path,'') = ''  or path = '%'
	then
		cz_write_audit(jobId,databaseName,procedureName,'Path missing or invalid',0,stepCt,'Done');
  else
    SELECT sourcesystem_cd
    INTO trialId
    FROM i2b2metadata.i2b2
    WHERE c_fullname = path;

    IF (trialId IS NOT NULL)
    THEN
      DELETE FROM i2b2demodata.observation_fact f
      WHERE f.concept_cd IN (SELECT c_basecode
                             FROM i2b2metadata.i2b2
                             WHERE c_fullname = path) AND f.sourcesystem_cd = trialId;
    ELSE
      BEGIN
        SELECT f.sourcesystem_cd
        INTO srcSysCD
        FROM i2b2demodata.observation_fact f
        WHERE f.concept_cd IN (SELECT c_basecode
                               FROM i2b2metadata.i2b2
                               WHERE c_fullname = path);
        EXCEPTION
        WHEN NO_DATA_FOUND
        THEN
          srcSysCd := NULL;
      END;
      IF (srcSysCD IS NOT NULL)
      THEN
        RAISE has_cross_node;
      END IF;
    END IF;

    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Delete data for node from I2B2DEMODATA observation_fact',
                    SQL%ROWCOUNT, stepCt, 'Done');
    COMMIT;

      --CONCEPT DIMENSION
    IF (trialId IS NOT NULL)
    THEN
      DELETE FROM i2b2demodata.concept_dimension c
      WHERE c.CONCEPT_PATH = path AND c.sourcesystem_cd = trialId;
    END IF;
    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Delete data for node from I2B2DEMODATA concept_dimension',
                    SQL%ROWCOUNT, stepCt, 'Done');
    COMMIT;

    --I2B2
    DELETE
    FROM i2b2
    WHERE
      C_FULLNAME = PATH;
    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Delete data for node from I2B2METADATA i2b2', SQL%ROWCOUNT,
                    stepCt, 'Done');
    COMMIT;

  --i2b2_secure
    DELETE
    FROM i2b2_secure
    WHERE
      C_FULLNAME = PATH;
    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Delete data for node from I2B2METADATA i2b2_secure',
                    SQL%ROWCOUNT, stepCt, 'Done');
    COMMIT;

    --concept_counts
    DELETE
    FROM concept_counts
    WHERE
      concept_path = PATH;
    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Delete data for node from I2B2DEMODATA concept_counts',
                    SQL%ROWCOUNT, stepCt, 'Done');
    COMMIT;

  END IF;

    ---Cleanup OVERALL JOB if this proc is being run standalone
  IF newJobFlag = 1
  THEN
    cz_end_audit (jobID, 'SUCCESS');
  END IF;

  EXCEPTION
  WHEN has_cross_node
  THEN
    cz_write_audit(jobId, databasename, procedurename,
                   'Cross-trial node ' || path || ' has data from study ' || coalesce(srcSysCD, 'null'), 1, stepCt,
                   'ERROR');
    cz_error_handler(jobid, procedurename);
    cz_end_audit(jobId, 'FAIL');
  WHEN OTHERS THEN
    --Handle errors.
    czx_error_handler (jobID, procedureName);
    --End Proc
    czx_end_audit (jobID, 'FAIL');
END;


/
 
