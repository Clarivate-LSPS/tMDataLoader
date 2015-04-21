CREATE OR REPLACE PROCEDURE "I2B2_ADD_PLATFORM"(
  gpl_id       varchar2,
  name varchar2,
  organism varchar2,
  marker_type varchar2,
  genome_build varchar2 := NULL,
  release_nbr varchar2 := NULL,
  currentjobid NUMBER := -1)
AS
    newJobFlag 	INTEGER(1);

    databaseName 	VARCHAR(100);
    procedureName VARCHAR(100);
    jobID 		number(18,0);
    stepCt 		number(18,0);

BEGIN
    stepCt := 0;

    --Set Audit Parameters
    newJobFlag := 0; -- False (Default)
    jobID := currentjobid;
    SELECT sys_context('USERENV', 'CURRENT_SCHEMA') INTO databaseName FROM dual;
    procedureName := $$PLSQL_UNIT;

    stepCt := 0;

    IF(jobID IS NULL or jobID < 1)
    THEN
      newJobFlag := 1; -- True
      cz_start_audit (procedureName, databaseName, jobID);
    END IF;

    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Starting I2B2_ADD_PLATFORM', 0, stepCt, 'Done');
    COMMIT;

    INSERT INTO deapp.de_gpl_info (platform, title, organism, annotation_date, marker_type, genome_build, release_nbr)
    VALUES
      (gpl_id, name, organism, current_timestamp, marker_type, genome_build, release_nbr);
    COMMIT;

    cz_write_audit(jobId, databaseName, procedureName, 'Add platform ' || gpl_id, SQL%ROWCOUNT, stepCt, 'Done');

    IF newJobFlag = 1
    THEN
    cz_end_audit(jobID, 'SUCCESS');
    END IF;

    EXCEPTION
    WHEN OTHERS THEN
--Handle errors.
    cz_error_handler (jobID, procedureName);
--End Proc
    cz_end_audit (jobID, 'FAIL');
END;
/
