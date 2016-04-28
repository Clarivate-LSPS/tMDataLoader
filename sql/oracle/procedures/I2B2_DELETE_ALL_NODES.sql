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
    --I2B2
    DELETE 
      FROM OBSERVATION_FACT 
    WHERE 
      concept_cd IN (SELECT C_BASECODE FROM I2B2 WHERE C_FULLNAME LIKE PATH || '%');
	  stepCt := stepCt + 1;
	  cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from I2B2DEMODATA observation_fact',SQL%ROWCOUNT,stepCt,'Done');
    COMMIT;
	

      --CONCEPT DIMENSION
    DELETE 
      FROM CONCEPT_DIMENSION
    WHERE 
      CONCEPT_PATH LIKE path || '%';
	  stepCt := stepCt + 1;
	  cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from I2B2DEMODATA concept_dimension',SQL%ROWCOUNT,stepCt,'Done');
    COMMIT;
    
      --I2B2
      DELETE
        FROM i2b2
      WHERE 
        C_FULLNAME LIKE PATH || '%';
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
        C_FULLNAME LIKE PATH || '%';
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
