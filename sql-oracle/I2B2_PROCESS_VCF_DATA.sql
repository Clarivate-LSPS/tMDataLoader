-- Function: tm_cz.i2b2_process_vcf_data(varchar2, varchar2, varchar2, varchar2, numeric)

-- DROP FUNCTION tm_cz.i2b2_process_vcf_data(varchar2, varchar2, varchar2, varchar2, numeric);

CREATE OR REPLACE PROCEDURE tm_cz.i2b2_process_vcf_data
( trial_id 		varchar2
 ,top_node		varchar2
 ,source_cd		varchar2 default 'STD'		--	default source_cd = 'STD'
 ,secure_study	varchar2	default 'N'		--	security setting if new patients added to patient_dimension
 ,currentJobID 	number default -1
) as

	--Audit variables
	newJobFlag		integer;
	databaseName 	VARCHAR(100);
	procedureName 	VARCHAR(100);
	jobID number(18,0);
	stepCt number(18,0);
	rowCt	number(18,0);
	errorNumber		varchar2(500);
	errorMessage	varchar2(500);
	rtnCd			integer;

	TrialID			varchar(100);
	sourceCd		varchar(50);

	res numeric;
  resnull exception;
BEGIN
	TrialID := upper(trial_id);

	--Set Audit Parameters
	newJobFlag := 0; -- False (Default)
	jobID := currentJobID;
	databaseName := 'TM_CZ';
	procedureName := 'i2b2_process_vcf_data';

	--Audit JOB Initialization
	--If Job ID does not exist, then this is a single procedure run and we need to create it

	IF(jobID IS NULL or jobID < 1)
	THEN
		newJobFlag := 1; -- True
		tm_cz.cz_start_audit (procedureName, databaseName);
	END IF;

	stepCt := 0;
	stepCt := stepCt + 1;
	tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Starting i2b2_process_vcf_data',0,stepCt,'Done');

	select upper(nvl(source_cd,'STD')) into sourceCd from dual;

	tm_cz.I2B2_LOAD_SAMPLES(trial_id, top_node, 'VCF', sourceCd, secure_study, jobID);
	if res < 0 then
	  raise resnull;
	end if;

	stepCt := stepCt + 1;
	tm_cz.cz_write_audit(jobId,databaseName,procedureName,'End i2b2_process_vcf_data',0,stepCt,'Done');

	---Cleanup OVERALL JOB if this proc is being run standalone
	IF newJobFlag = 1
	THEN
		tm_cz.cz_end_audit (jobID, 'SUCCESS');
	END IF;

 EXCEPTION
  WHEN resnull then 
	cz_write_audit(jobId,databasename,procedurename,'res < 0 ',1,stepCt,'ERROR');
	cz_error_handler(jobid,procedurename);
	cz_end_audit (jobId,'FAIL');

  WHEN OTHERS THEN
    cz_error_handler (jobID, procedureName);
    cz_end_audit (jobID, 'FAIL');

END;
