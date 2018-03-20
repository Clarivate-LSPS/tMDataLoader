create or replace PROCEDURE INSERT_ADDITIONAL_DATA
  (
    trial_id       VARCHAR2,
    rel_time_label VARCHAR2,
    currentjobid   NUMBER := 1,
    trialVisitNum OUT NUMBER
  )
AS

  --Audit variables
  newJobFlag          INTEGER;
  jobID               NUMBER(18, 0);
  stepCt              NUMBER(18, 0);
  rowCt               NUMBER(18, 0);
  errorNumber         NUMBER(18, 0);
  errorMessage        VARCHAR2(1000);
  studyNum            NUMBER(18, 0);

  TrialID             VARCHAR2(100);
  securedStudy        VARCHAR2(5);
  pExists             INTEGER;
  v_bio_experiment_id NUMBER(18, 0);
  relTimeLabel        VARCHAR2(900);
  databaseName        VARCHAR(100);
  procedureName       VARCHAR(100);

  BEGIN

    TrialID := trial_id;
    relTimeLabel := rel_time_label;

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

    -- Insert into bio_experiment
    SELECT count(*)
    INTO pExists
    FROM biomart.bio_experiment
    WHERE accession = TrialId;

    IF pExists = 0
    THEN
      INSERT INTO biomart.bio_experiment
      (title, accession, etl_id)
        SELECT
          'Metadata not available',
          TrialId,
          'METADATA:' || TrialId
        FROM dual;

      stepCt := stepCt + 1;
      cz_write_audit(jobId, databaseName, procedureName, 'Insert trial/study into biomart.bio_experiment', SQL%ROWCOUNT,
                     stepCt, 'Done');
      COMMIT;
    END IF;

    SELECT count(*)
    INTO pExists
    FROM i2b2demodata.study
    WHERE study_id = TrialId;
    --Insert into study
    IF pExists = 0
    THEN
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
        i2b2demodata.study_num_seq.nextval,
        v_bio_experiment_id,
        TrialId,
        CASE WHEN securedStudy = 'N'
          THEN 'PUBLIC'
        ELSE 'EXP:' || TrialId
        END);

      stepCt := stepCt + 1;
      cz_write_audit(jobId, databaseName, procedureName, 'Add study to STUDY table', SQL%ROWCOUNT, stepCt, 'Done');
      COMMIT;
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
      trialVisitNum := i2b2demodata.trial_visit_num_seq.nextval;

      INSERT INTO i2b2demodata.trial_visit_dimension (
        trial_visit_num,
        study_num,
        rel_time_label)
        SELECT
          trialVisitNum,
          studyNum,
          relTimeLabel
        FROM dual;

      stepCt := stepCt + 1;
      cz_write_audit(jobId, databaseName, procedureName, 'Inserting new trial visits', SQL%ROWCOUNT, stepCt, 'Done');
    ELSE
      SELECT trial_visit_num
      INTO trialVisitNum
      FROM i2b2demodata.trial_visit_dimension tvd
      WHERE tvd.study_num = studyNum AND tvd.rel_time_label = relTimeLabel;
    END IF;

    EXCEPTION
    WHEN OTHERS THEN
    --Handle errors.
    cz_error_handler(jobID, procedureName);
    --End Proc
    cz_end_audit(jobID, 'FAIL');
  END;