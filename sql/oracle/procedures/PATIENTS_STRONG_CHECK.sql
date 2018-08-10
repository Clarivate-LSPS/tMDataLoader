CREATE OR REPLACE FUNCTION "PATIENTS_STRONG_CHECK"(
  currentjobid NUMBER := 1
)
  RETURN VARCHAR2 IS
  databaseName  VARCHAR(100);
  procedureName VARCHAR(100);
  --Audit variables
  newJobFlag    INTEGER;
  jobID         NUMBER(18, 0);
  stepCt        NUMBER(18, 0);
  rowCt         NUMBER(18, 0);
  errorNumber   NUMBER(18, 0);
  errorMessage  VARCHAR2(1000);
  badPatients   VARCHAR2(2000);

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

    SELECT LISTAGG(t.usubjid, ', ')
    WITHIN GROUP (
      ORDER BY t.usubjid)
    INTO badPatients
    FROM
      tmp_subject_info t INNER JOIN i2b2demodata.patient_dimension pd
        ON t.usubjid = pd.sourcesystem_cd
    WHERE
      t.sex_cd <> pd.sex_cd OR
      t.age_in_years_num <> pd.age_in_years_num OR
      t.race_cd <> pd.race_cd;

    IF badPatients IS NOT NULL
    THEN
      stepCt := stepCt + 1;
      cz_write_audit(jobId, databaseName, procedureName,
                     'New patients set (' || badPatients || ') contain different values from exist in DB', 0, stepCt,
                     'Done');

      RETURN -16;
    END IF;

    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'End ' || procedureName, 0, stepCt, 'Done');

    IF newJobFlag = 1
    THEN
      cz_end_audit(jobID, 'SUCCESS');
    END IF;

    RETURN 1;
  END;
  /