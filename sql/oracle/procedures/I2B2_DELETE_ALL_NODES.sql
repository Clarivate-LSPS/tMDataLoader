create or replace PROCEDURE                     "I2B2_DELETE_ALL_NODES" 
(
  path VARCHAR2
 ,currentJobID NUMBER := null
)
AS
      
  --Audit variables
  newJobFlag INTEGER(1);
  databaseName VARCHAR(100);
  procedureName VARCHAR(100);
  jobID number(18,0);
  stepCt number(18,0);
  INDEX_NOT_EXISTS EXCEPTION;
  PRAGMA EXCEPTION_INIT(index_not_exists, -1418);

  trialVisitNum INTEGER(8);
  trialVisitNumN INTEGER(8);
Begin

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
  -------------------------------------------------------------
  -- Delete a tree node in I2b2
  -- Not handling Observation Fact. It will take too long. 
  -- KCR@20090404 - First Rev
  -- JEA@20100106 - Added auditing
  -------------------------------------------------------------
  
  
    -- WA optimization 2013_08_14  
    begin
      execute immediate('ALTER INDEX i2b2demodata.IDX_OB_FACT_1 unusable');
        exception
          when index_not_exists then null;
    end;
    begin
      execute immediate('ALTER INDEX i2b2demodata.IDX_OB_FACT_2 unusable');
        exception
          when index_not_exists then null;
    end;
    begin  
      execute immediate('ALTER INDEX i2b2demodata.IDX_OB_FACT_4 unusable');
        exception
          when index_not_exists then null;
    end;
    -- WA optimization 2013_08_14  
  
  
  if path != ''  or path != '%'
  then 
    --observation_fact
    SELECT count(DISTINCT trial_visit_num)
    INTO trialVisitNumN
    FROM i2b2demodata.OBSERVATION_FACT
    WHERE concept_cd IN (
      SELECT concept_cd
      FROM i2b2demodata.CONCEPT_DIMENSION
      WHERE concept_path LIKE PATH || '%' AND sourcesystem_cd IS NOT NULL
    );
    stepCt := stepCt + 1;
    cz_write_audit(jobId,databaseName,procedureName,'trialVisitNumN ' || trialVisitNumN,SQL%ROWCOUNT,stepCt,'Done');
    IF (trialVisitNumN != 1)
    THEN
      stepCt := stepCt + 1;
      cz_write_audit(jobId,databaseName,procedureName,'You can only specify a path for a single study.',SQL%ROWCOUNT,stepCt,'Done');
    ELSE
      SELECT DISTINCT trial_visit_num
      INTO trialVisitNum
      FROM i2b2demodata.OBSERVATION_FACT
      WHERE concept_cd IN (
        SELECT concept_cd
        FROM i2b2demodata.CONCEPT_DIMENSION
        WHERE concept_path LIKE PATH || '%' AND sourcesystem_cd IS NOT NULL
      );
      cz_write_audit(jobId, databaseName, procedureName, 'Found trial_visit_num', SQL%ROWCOUNT, stepCt, 'Done');
      DELETE
      FROM OBSERVATION_FACT
      WHERE
        concept_cd IN (SELECT C_BASECODE FROM I2B2 WHERE C_FULLNAME LIKE PATH || '%'  AND sourcesystem_cd IS NOT NULL)
      AND trial_visit_num = trialVisitNum;
      stepCt := stepCt + 1;
      cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from I2B2DEMODATA observation_fact',SQL%ROWCOUNT,stepCt,'Done');
      COMMIT;
    END IF;

      --CONCEPT DIMENSION
    DELETE
    FROM CONCEPT_DIMENSION cd
    WHERE
      CONCEPT_PATH LIKE path || '%' AND (sourcesystem_cd IS NOT NULL OR
                                         exists(SELECT 1
                                                FROM i2b2metadata.i2b2
                                                WHERE c_fullname = cd.concept_path AND
                                                      length(c_visualattributes) > 2)
      );
	  stepCt := stepCt + 1;
	  cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from I2B2DEMODATA concept_dimension',SQL%ROWCOUNT,stepCt,'Done');
    COMMIT;
    
      --I2B2
      DELETE
        FROM i2b2
      WHERE 
        C_FULLNAME LIKE PATH || '%' AND sourcesystem_cd IS NOT NULL;
	  stepCt := stepCt + 1;
	  cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from I2B2METADATA i2b2',SQL%ROWCOUNT,stepCt,'Done');
    COMMIT;
  END IF;
  
  -- WA optimization 2013_08_14    
  begin
    execute immediate('ALTER INDEX i2b2demodata.IDX_OB_FACT_1 rebuild');
      exception
        when index_not_exists then null;
  end;
  begin
    execute immediate('ALTER INDEX i2b2demodata.IDX_OB_FACT_2 rebuild');
      exception
        when index_not_exists then null;
  end;
  begin
    execute immediate('ALTER INDEX i2b2demodata.IDX_OB_FACT_4 rebuild');
      exception
        when index_not_exists then null;
  end;
  -- WA optimization 2013_08_14  
  
  
  --i2b2_secure
      DELETE
        FROM i2b2_secure
      WHERE 
        C_FULLNAME LIKE PATH || '%' AND sourcesystem_cd IS NOT NULL;
	  stepCt := stepCt + 1;
	  cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from I2B2METADATA i2b2_secure',SQL%ROWCOUNT,stepCt,'Done');
    COMMIT;

  --concept_counts
      DELETE
        FROM concept_counts
      WHERE 
        concept_path LIKE PATH || '%';
	  stepCt := stepCt + 1;
	  cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from I2B2DEMODATA concept_counts',SQL%ROWCOUNT,stepCt,'Done');
    COMMIT;
  
  
END;
/
