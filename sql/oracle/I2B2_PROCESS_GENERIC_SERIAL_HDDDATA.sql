CREATE OR REPLACE PROCEDURE "I2B2_PROCESS_GENERIC_HDDDATA"
(
  studyIdInput  VARCHAR2
 ,platform VARCHAR2
 ,mapTable VARCHAR2
 ,currentJobID 	NUMBER := null
 ,rtn_code		  OUT	NUMBER
)
AS

  studyId		varchar2(100);
  --Audit variables
  newJobFlag INTEGER(1);
  databaseName VARCHAR(100);
  procedureName VARCHAR(100);
  jobID number(18,0);
  stepCt number(18,0);
  query VARCHAR2(1000);

BEGIN
	studyId := upper(studyIdInput);

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
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Starting i2b2_process_generic_hdd_data. Type: ' || platform ,0,stepCt,'Done');

  update lt_src_mrna_xml_data
  set study_id=upper(study_id);

  stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Uppercase study_id in lt_src_mrna_xml_data',SQL%ROWCOUNT,stepCt,'Done');
	commit;

  query := 'update i2b2metadata.i2b2 ib set c_metadataxml = ' ||
  '(select c_metadataxml from lt_src_mrna_xml_data mxd ' ||
  'where ib.c_name = mxd.category_cd and ib.sourcesystem_cd = mxd.study_id) ' ||
  'where ib.sourcesystem_cd = :1 and ib.c_basecode in ' ||
  '(select sm.concept_code from deapp.de_subject_sample_mapping sm ' ||
  'inner join ' || mapTable || ' lsm on lsm.sample_cd = sm.sample_cd where sm.trial_name = :1 ' ||
  'and sm.platform = :2)';

  EXECUTE IMMEDIATE query USING studyId, studyId, platform;

  stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Update records in i2b2metadata.i2b2',SQL%ROWCOUNT,stepCt,'Done');
	commit;

  query := 'insert into i2b2demodata.sample_dimension (sample_cd)' ||
  'select distinct sample_cd from ' ||  mapTable || ' ssm where ssm.trial_name = :1' ||
  'and not exists (select sample_cd from i2b2demodata.sample_dimension sd where sd.sample_cd = ssm.sample_cd)';

  EXECUTE IMMEDIATE query USING studyId;

  stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Insert records into i2b2demodata.sample_dimension',SQL%ROWCOUNT,stepCt,'Done');
	commit;

  query := 'update i2b2demodata.observation_fact obf set sample_cd = ' ||
  '(select distinct sm.sample_cd from deapp.de_subject_sample_mapping sm ' ||
  'where obf.patient_num = sm.patient_id and obf.sourcesystem_cd = sm.trial_name ' ||
  'and obf.concept_cd = sm.concept_code and sm.platform = :1) ' ||
  'where obf.sourcesystem_cd = :2 and obf.concept_cd in ' ||
  '(select sm.concept_code from deapp.de_subject_sample_mapping sm ' ||
  'inner join ' || mapTable || ' lsm on lsm.sample_cd = sm.sample_cd ' ||
  'where sm.trial_name = :2 and sm.platform = :1)';

  EXECUTE IMMEDIATE query USING platform, studyId, studyId, platform;

  stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Update sample codes in i2b2demodata.observation_fact',SQL%ROWCOUNT,stepCt,'Done');
	commit;

  ---Cleanup OVERALL JOB if this proc is being run standalone
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'End i2b2_process_generic_hdd_data',0,stepCt,'Done');

	IF newJobFlag = 1
	THEN
		cz_end_audit (jobID, 'SUCCESS');
	END IF;

	select 0 into rtn_code from dual;

	EXCEPTION
	WHEN OTHERS THEN
		--Handle errors.
		cz_error_handler (jobID, procedureName);
		--End Proc
		cz_end_audit (jobID, 'FAIL');
		select 16 into rtn_code from dual;
END;
/
