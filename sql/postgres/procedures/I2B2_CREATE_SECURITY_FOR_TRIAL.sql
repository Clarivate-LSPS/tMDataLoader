-- Function: i2b2_create_security_for_trial(character varying, character varying, numeric)

-- DROP FUNCTION i2b2_create_security_for_trial(character varying, character varying, numeric);

CREATE OR REPLACE FUNCTION i2b2_create_security_for_trial(
  trial_id character varying,
  secured_study character varying DEFAULT 'N'::character varying,
  currentjobid numeric DEFAULT '-1'::integer)
  RETURNS numeric AS
$BODY$
/*************************************************************************
* Copyright 2008-2012 Janssen Research & Development, LLC.
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
Declare

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

BEGIN
  TrialID := trial_id;
  securedStudy := secured_study;

  --Set Audit Parameters
  newJobFlag := 0; -- False (Default)
  jobID := currentJobID;

  databaseName := current_schema();
  procedureName := 'I2B2_CREATE_SECURITY_FOR_TRIAL';

  --Audit JOB Initialization
  --If Job ID does not exist, then this is a single procedure run and we need to create it
  IF(jobID IS NULL or jobID < 1)
  THEN
    newJobFlag := 1; -- True
    select cz_start_audit (procedureName, databaseName) into jobId;
  END IF;

  stepCt := 0;

  begin
    delete from i2b2demodata.observation_fact
    where sourcesystem_cd = TrialID
          and concept_cd = 'SECURITY';
    get diagnostics rowCt := ROW_COUNT;
    exception
    when others then
      errorNumber := SQLSTATE;
      errorMessage := SQLERRM;
      --Handle errors.
      select cz_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
      --End Proc
      select cz_end_audit (jobID, 'FAIL') into rtnCd;
      return -16;
  end;
  stepCt := stepCt + 1;
  select cz_write_audit(jobId,databaseName,procedureName,'Delete security records for trial from I2B2DEMODATA observation_fact',rowCt,stepCt,'Done') into rtnCd;

  begin
    insert into i2b2demodata.observation_fact
    (encounter_num
      ,patient_num
      ,concept_cd
      ,start_date
      ,provider_id
      ,modifier_cd
      ,valtype_cd
      ,tval_char
      ,valueflag_cd
      ,location_cd
      ,update_date
      ,download_date
      ,import_date
      ,sourcesystem_cd
      ,instance_num
    )
      select patient_num
        ,patient_num
        ,'SECURITY'
        ,current_timestamp
        ,'@'
        ,TrialId
        ,'T'
        ,case when securedStudy = 'N' then 'EXP:PUBLIC' else 'EXP:' || trialID end
        ,'@'
        ,'@'
        ,current_timestamp
        ,current_timestamp
        ,current_timestamp
        --,sourcesystem_cd
        ,TrialId
        ,1
      from i2b2demodata.patient_dimension
      where sourcesystem_cd like TrialID || ':%';
    get diagnostics rowCt := ROW_COUNT;
    exception
    when others then
      errorNumber := SQLSTATE;
      errorMessage := SQLERRM;
      --Handle errors.
      select cz_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
      --End Proc
      select cz_end_audit (jobID, 'FAIL') into rtnCd;
      return -16;
  end;
  stepCt := stepCt + 1;
  select cz_write_audit(jobId,databaseName,procedureName,'Insert security records for trial from I2B2DEMODATA observation_fact',rowCt,stepCt,'Done') into rtnCd;

  --	insert patients to patient_trial table

  begin
    delete from i2b2demodata.patient_trial
    where trial  = TrialID;
    get diagnostics rowCt := ROW_COUNT;
    exception
    when others then
      errorNumber := SQLSTATE;
      errorMessage := SQLERRM;
      --Handle errors.
      select cz_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
      --End Proc
      select cz_end_audit (jobID, 'FAIL') into rtnCd;
      return -16;
  end;
  stepCt := stepCt + 1;
  select cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from I2B2DEMODATA patient_trial',rowCt,stepCt,'Done') into rtnCd;

  begin
    insert into i2b2demodata.patient_trial
    (patient_num
      ,trial
      ,secure_obj_token
    )
      select patient_num,
        TrialID,
        case when securedStudy = 'N' then 'EXP:PUBLIC' else 'EXP:' || trialID end
      from i2b2demodata.patient_dimension
      where sourcesystem_cd like TrialID || ':%';
    get diagnostics rowCt := ROW_COUNT;
    exception
    when others then
      errorNumber := SQLSTATE;
      errorMessage := SQLERRM;
      --Handle errors.
      select cz_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
      --End Proc
      select cz_end_audit (jobID, 'FAIL') into rtnCd;
      return -16;
  end;
  stepCt := stepCt + 1;
  select cz_write_audit(jobId,databaseName,procedureName,'Insert data for trial into I2B2DEMODATA patient_trial',rowCt,stepCt,'Done') into rtnCd;

  --	We always create bio_experiment record, because we need add relationship between study and dimenstion_description
  -- if secure study, then insert to search_secured_object

  select count(*) into pExists
  from biomart.bio_experiment
  where accession = TrialId;

  if pExists = 0 then
    begin
      insert into biomart.bio_experiment
      (title, accession, etl_id)
        select 'Metadata not available'
          ,TrialId
          ,'METADATA:' || TrialId;
      get diagnostics rowCt := ROW_COUNT;
      exception
      when others then
        errorNumber := SQLSTATE;
        errorMessage := SQLERRM;
        --Handle errors.
        select cz_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
        --End Proc
        select cz_end_audit (jobID, 'FAIL') into rtnCd;
        return -16;
    end;
    stepCt := stepCt + 1;
    select cz_write_audit(jobId,databaseName,procedureName,'Insert trial/study into biomart.bio_experiment',rowCt,stepCt,'Done') into rtnCd;
  end if;

  select count(*) into pExists
  from searchapp.search_secure_object sso
  where bio_data_unique_id = 'EXP:' || TrialId;

  if pExists = 0 then
    --	if securedStudy = Y, add trial to searchapp.search_secured_object
    if securedStudy = 'Y' then

      select bio_experiment_id into v_bio_experiment_id
      from biomart.bio_experiment
      where accession = TrialId;

      begin
        insert into searchapp.search_secure_object
        (bio_data_id
          ,display_name
          ,data_type
          ,bio_data_unique_id
        )
          select v_bio_experiment_id
            ,parse_nth_value(md.c_fullname,2,'\') || ' - ' || md.c_name as display_name
            ,'BIO_CLINICAL_TRIAL' as data_type
            ,'EXP:' || TrialId as bio_data_unique_id
          from i2b2metadata.i2b2 md
          where md.sourcesystem_cd = TrialId
                and md.c_hlevel =
                    (select min(x.c_hlevel) from i2b2metadata.i2b2 x
                    where x.sourcesystem_cd = TrialId)
                and not exists
          (select 1 from searchapp.search_secure_object so
              where v_bio_experiment_id = so.bio_data_id);
        get diagnostics rowCt := ROW_COUNT;
        exception
        when others then
          errorNumber := SQLSTATE;
          errorMessage := SQLERRM;
          --Handle errors.
          select cz_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
          --End Proc
          select cz_end_audit (jobID, 'FAIL') into rtnCd;
          return -16;
      end;
      stepCt := stepCt + 1;
      select cz_write_audit(jobId,databaseName,procedureName,'Inserted trial/study into SEARCHAPP search_secure_object',rowCt,stepCt,'Done') into rtnCd;
    end if;
  else
    --	if securedStudy = N, delete entry from searchapp.search_secure_object
    if securedStudy = 'N' then
      begin
        delete from searchapp.search_secure_object
        where bio_data_unique_id = 'EXP:' || TrialId;
        get diagnostics rowCt := ROW_COUNT;
        exception
        when others then
          errorNumber := SQLSTATE;
          errorMessage := SQLERRM;
          --Handle errors.
          select cz_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
          --End Proc
          select cz_end_audit (jobID, 'FAIL') into rtnCd;
          return -16;
      end;
      stepCt := stepCt + 1;
      select cz_write_audit(jobId,databaseName,procedureName,'Deleted trial/study from SEARCHAPP search_secure_object',rowCt,stepCt,'Done') into rtnCd;
    end if;
  end if;

  SELECT count(*)
  INTO pExists
  FROM i2b2demodata.study
  WHERE study_id = TrialId;

  IF pExists = 0
  THEN
    BEGIN
      select bio_experiment_id into v_bio_experiment_id
      from biomart.bio_experiment
      where accession = TrialId;

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
      get diagnostics rowCt := ROW_COUNT;
      exception
      when others then
        errorNumber := SQLSTATE;
        errorMessage := SQLERRM;
        --Handle errors.
        select cz_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
        --End Proc
        select cz_end_audit (jobID, 'FAIL') into rtnCd;
        return -16;
    end;
  END IF;

  SELECT count(*)
  INTO pExists
  FROM i2b2demodata.study s, i2b2metadata.study_dimension_descriptions sdd
  WHERE
    s.study_num = sdd.study_id AND
    s.study_id = TrialId;

  IF pExists = 0
  THEN
    select study_num into studyNum
    from i2b2demodata.study
    where study_id = TrialId;

    BEGIN
        INSERT INTO i2b2metadata.study_dimension_descriptions (
          dimension_description_id,
          study_id)
          SELECT id, studyNum FROM i2b2metadata.dimension_description
          WHERE
            name in ('study',
              'concept',
              'patient',
              'visit',
              'start time',
              'end time',
              'location',
              'trial visit',
              'provider',
              'biomarker',
              'assay',
              'projection');
      get diagnostics rowCt := ROW_COUNT;
      exception
      when others then
        errorNumber := SQLSTATE;
        errorMessage := SQLERRM;
        --Handle errors.
        select cz_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
        --End Proc
        select cz_end_audit (jobID, 'FAIL') into rtnCd;
        return -16;
    END;

    stepCt := stepCt + 1;
    select cz_write_audit(jobId, databaseName, procedureName, 'Add study dimension', rowCt, stepCt, 'Done') into rtnCd;
  END IF;

  ---Cleanup OVERALL JOB if this proc is being run standalone
  IF newJobFlag = 1
  THEN
    select cz_end_audit (jobID, 'SUCCESS') into rtnCd;
  END IF;

  return 1;

  EXCEPTION
  WHEN OTHERS THEN
    errorNumber := SQLSTATE;
    errorMessage := SQLERRM;
    --Handle errors.
    select cz_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
    --End Proc
    select cz_end_audit (jobID, 'FAIL') into rtnCd;
    return -16;

END;

$BODY$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path FROM CURRENT
COST 100;