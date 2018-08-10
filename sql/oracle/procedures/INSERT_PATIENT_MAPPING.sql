CREATE OR REPLACE FUNCTION "INSERT_PATIENT_MAPPING"(
  shared_patients      IN VARCHAR := NULL,
  currentjobid NUMBER := 1
)
  RETURN VARCHAR2 IS
  rtnCd         INTEGER;
  jobID         NUMBER(18, 0);
  stepCt        NUMBER(18, 0);
  databaseName  VARCHAR2(100);
  newJobFlag    INTEGER;
  procedureName VARCHAR2(100);
  rowCt			numeric(18,0);

  BEGIN
    --Set Audit Parameters
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

    stepCt := 0;
    cz_write_audit(jobId, databaseName, procedureName, 'Starting ' || procedureName, 0, stepCt, 'Done');

    IF shared_patients IS NOT NULL
    THEN
        INSERT INTO i2b2demodata.patient_mapping (
          patient_ide,
          patient_ide_source,
          patient_num,
          patient_ide_status
        ) SELECT
            sourcesystem_cd,
            'SUBJ_ID',
            patient_num,
            'ACTIVE'
          FROM i2b2demodata.patient_dimension pd
          WHERE
            sourcesystem_cd LIKE shared_patients || ':%'
            AND NOT exists(SELECT 1
                           FROM i2b2demodata.patient_mapping pd2
                           WHERE pd.sourcesystem_cd = pd2.patient_ide);
      COMMIT ;
      stepCt := stepCt + 1;
      cz_write_audit(jobId, databaseName, procedureName, 'Insert new subjects into patient_mapping', SQL%ROWCOUNT, stepCt,
                            'Done');
    END IF;

    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'End INSERT_PATIENT_MAPPING', 0, stepCt, 'Done');

    IF newJobFlag = 1
    THEN
      cz_end_audit(jobID, 'SUCCESS');
    END IF;

    RETURN 1;
  END;
  /