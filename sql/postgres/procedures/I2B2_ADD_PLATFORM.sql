CREATE OR REPLACE FUNCTION I2B2_ADD_PLATFORM(
  gpl_id       CHARACTER VARYING, name CHARACTER VARYING, organism CHARACTER VARYING, marker_type CHARACTER VARYING,
  genome_build CHARACTER VARYING DEFAULT NULL, release_nbr CHARACTER VARYING DEFAULT NULL,
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
    jobID := currentjobid;
    databaseName := current_schema();
    procedureName := 'I2B2_ADD_PLATFORM';

    stepCt := 0;
    stepCt := stepCt + 1;
    PERFORM cz_write_audit(jobId, databaseName, procedureName, 'Starting i2b2_load_samples', 0, stepCt, 'Done');

    SELECT cz_start_audit(procedureName, databaseName)
    INTO jobID
    WHERE jobID IS NULL OR jobID < 1;


    INSERT INTO deapp.de_gpl_info (platform, title, organism, annotation_date, marker_type, genome_build, release_nbr)
    VALUES
      (gpl_id, "name", organism, current_timestamp, marker_type, genome_build, release_nbr);

    PERFORM cz_end_audit(jobID, 'SUCCESS')
    WHERE currentjobid IS NULL OR currentjobid <> jobID;

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


ALTER FUNCTION I2B2_ADD_PLATFORM( CHARACTER VARYING, CHARACTER VARYING, CHARACTER VARYING, CHARACTER VARYING, CHARACTER VARYING, CHARACTER VARYING, NUMERIC )
OWNER TO postgres;