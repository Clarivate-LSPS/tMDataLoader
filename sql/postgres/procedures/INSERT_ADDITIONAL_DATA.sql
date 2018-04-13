CREATE OR REPLACE FUNCTION INSERT_ADDITIONAL_DATA(
  trial_id       CHARACTER VARYING,
  rel_time_label CHARACTER VARYING DEFAULT 'Default',
  secure_study character varying DEFAULT 'N'::character varying,
  currentjobid   NUMERIC DEFAULT '-1' :: INTEGER)
  RETURNS NUMERIC AS
$BODY$
DECLARE

  --Audit variables
  newJobFlag          INTEGER;
  databaseName        VARCHAR(100);
  procedureName       VARCHAR(100);
  jobID               NUMERIC(18, 0);
  stepCt              NUMERIC(18, 0);
  rowCt               NUMERIC(18, 0);
  errorNumber         CHARACTER VARYING;
  errorMessage        CHARACTER VARYING;
  rtnCd               NUMERIC;
  studyNum            NUMERIC(18, 0);
  trialVisitNum       NUMERIC(18, 0);

  TrialID             VARCHAR(100);
  securedStudy        VARCHAR(5);
  pExists             INTEGER;
  v_bio_experiment_id NUMERIC(18, 0);
  relTimeLabel        VARCHAR(900);

BEGIN

  TrialID := trial_id;
  securedStudy := upper(secure_study);
  relTimeLabel := rel_time_label;

  --Set Audit Parameters
  newJobFlag := 0; -- False (Default)
  jobID := currentJobID;

  databaseName := current_schema();
  procedureName := 'INSERT_ADDITIONAL_DATA';

  IF (jobID IS NULL OR jobID < 1)
  THEN
    newJobFlag := 1; -- True
    SELECT cz_start_audit(procedureName, databaseName)
    INTO jobId;
  END IF;

  stepCt := 0;

  -- Insert into bio_experiment
  SELECT count(*)
  INTO pExists
  FROM biomart.bio_experiment
  WHERE accession = TrialId;

  IF pExists = 0
  THEN
    BEGIN
      INSERT INTO biomart.bio_experiment
      (title, accession, etl_id)
        SELECT
          'Metadata not available',
          TrialId,
          'METADATA:' || TrialId;
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
    SELECT cz_write_audit(jobId, databaseName, procedureName, 'Insert trial/study into biomart.bio_experiment', rowCt,
                          stepCt, 'Done')
    INTO rtnCd;
  END IF;

  SELECT count(*)
  INTO pExists
  FROM i2b2demodata.study
  WHERE study_id = TrialId;
  --Insert into study
  IF pExists = 0
  THEN
    BEGIN
      SELECT bio_experiment_id
      INTO v_bio_experiment_id
      FROM biomart.bio_experiment
      WHERE accession = TrialId;

      INSERT INTO i2b2demodata.study (
        study_num,
        bio_experiment_id,
        study_id,
        secure_obj_token)
      VALUES (
        nextval('i2b2demodata.study_num_seq'),
        v_bio_experiment_id,
        TrialId,
        CASE WHEN securedStudy = 'N'
          THEN 'PUBLIC'
        ELSE 'EXP:' || TrialId
        END);

      stepCt := stepCt + 1;
      SELECT cz_write_audit(jobId, databaseName, procedureName, 'Add study to STUDY table', rowCt, stepCt, 'Done')
      INTO rtnCd;
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
  END IF;

  --Insert into trial_visit_dimenstion
  SELECT study_num
  INTO studyNum
  FROM i2b2demodata.study
  WHERE study_id = TrialId;

  SELECT count(*)
  INTO pExists
  FROM i2b2demodata.trial_visit_dimension tvd
  WHERE
    tvd.study_num = studyNum AND tvd.rel_time_label = relTimeLabel;

  IF pExists = 0
  THEN
    BEGIN
      trialVisitNum := nextval('i2b2demodata.trial_visit_num_seq');

      INSERT INTO i2b2demodata.trial_visit_dimension (
        trial_visit_num,
        study_num,
        rel_time_label)
        SELECT
          trialVisitNum,
          studyNum,
          relTimeLabel;
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
    SELECT cz_write_audit(jobId, databaseName, procedureName, 'Inserting new trial visits', rowCt, stepCt, 'Done')
    INTO rtnCd;
  ELSE
    SELECT trial_visit_num
    INTO trialVisitNum
    FROM i2b2demodata.trial_visit_dimension tvd
    WHERE tvd.study_num = studyNum AND tvd.rel_time_label = relTimeLabel;
  END IF;

  RETURN trialVisitNum;

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

$BODY$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path FROM CURRENT
COST 100;