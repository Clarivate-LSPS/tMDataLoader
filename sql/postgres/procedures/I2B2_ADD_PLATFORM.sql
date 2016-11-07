CREATE OR REPLACE FUNCTION I2B2_ADD_PLATFORM(
  gpl_id       CHARACTER VARYING,
  name CHARACTER VARYING,
  organism CHARACTER VARYING,
  marker_type CHARACTER VARYING,
  genome_build CHARACTER VARYING DEFAULT NULL,
  release_nbr CHARACTER VARYING DEFAULT NULL,
  currentjobid NUMERIC DEFAULT -1)
  RETURNS INTEGER AS
  $BODY$
  DECLARE
    jobID         NUMERIC(18, 0);
    databaseName  VARCHAR(100);
    procedureName VARCHAR(100);
    rowCt         NUMERIC(18, 0);
    stepCt        NUMERIC(18, 0);
    rtnCd         NUMERIC;

  BEGIN
    databaseName := current_schema();
    procedureName := 'I2B2_ADD_PLATFORM';

    stepCt := 0;

    SELECT CASE
      WHEN COALESCE(currentjobid, -1) < 1
      THEN cz_start_audit(procedureName, databaseName)
      ELSE currentjobid END
      INTO jobID;

    stepCt := stepCt + 1;
    PERFORM cz_write_audit(jobId, databaseName, procedureName, 'Starting ' || procedureName || ' for ' || gpl_id, 0, stepCt, 'Done');

    INSERT INTO deapp.de_gpl_info (platform, title, organism, annotation_date, marker_type, genome_build, release_nbr)
    VALUES
      (gpl_id, "name", organism, current_timestamp, marker_type, genome_build, release_nbr);
    get diagnostics rowCt := ROW_COUNT;

    stepCt := stepCt + 1;
    PERFORM cz_write_audit(jobId, databaseName, procedureName, 'Add platform ' || gpl_id, rowCt, stepCt, 'Done');

    stepCt := stepCt + 1;
    PERFORM cz_write_audit(jobId, databaseName, procedureName, 'End ' || procedureName, 0, stepCt, 'Done');

    PERFORM cz_end_audit(jobID, 'SUCCESS') WHERE COALESCE(currentjobid, -1) <> jobID;

    RETURN 1;

    EXCEPTION
    WHEN OTHERS THEN
      SELECT cz_write_error(jobId, SQLSTATE, SQLERRM, NULL, NULL)
      INTO rtnCd;
      RETURN -16;
  END;
  $BODY$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path FROM CURRENT
COST 100;
