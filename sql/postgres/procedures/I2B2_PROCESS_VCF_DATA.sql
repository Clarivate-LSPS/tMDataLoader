-- Function: i2b2_process_vcf_data(character varying, character varying, character varying, character varying, numeric)

-- DROP FUNCTION i2b2_process_vcf_data(character varying, character varying, character varying, character varying, numeric);

CREATE OR REPLACE FUNCTION i2b2_process_vcf_data
(   trial_id             CHARACTER VARYING
	, top_node             CHARACTER VARYING
	, source_cd            CHARACTER VARYING DEFAULT 'STD'    --	default source_cd = 'STD'
	, secure_study         CHARACTER VARYING DEFAULT 'N'    --	security setting if new patients added to patient_dimension
	, currentJobID         NUMERIC DEFAULT -1
	, shared_patients      CHARACTER VARYING DEFAULT NULL
	, strong_patient_check CHARACTER VARYING DEFAULT 'N' :: CHARACTER VARYING
) RETURNS numeric AS
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
	newJobFlag		integer;
	databaseName 	VARCHAR(100);
	procedureName 	VARCHAR(100);
	jobID 			numeric(18,0);
	stepCt 			numeric(18,0);
	rowCt			numeric(18,0);
	errorNumber		character varying;
	errorMessage	character varying;
	rtnCd			integer;

	TrialID			varchar(100);
	sourceCd		varchar(50);
	datasetId		varchar(160);

	res numeric;
BEGIN
	TrialID := upper(trial_id);
	sourceCd := upper(coalesce(source_cd,'STD'));
	datasetId := trial_id || ':' || sourceCd;

	--Set Audit Parameters
	newJobFlag := 0; -- False (Default)
	jobID := currentJobID;
	databaseName := current_schema();
	procedureName := 'i2b2_process_vcf_data';


	--Audit JOB Initialization
	--If Job ID does not exist, then this is a single procedure run and we need to create it

	IF(jobID IS NULL or jobID < 1)
	THEN
		newJobFlag := 1; -- True
		select cz_start_audit (procedureName, databaseName) into jobID;
	END IF;

	stepCt := 0;
	stepCt := stepCt + 1;
	select cz_write_audit(jobId,databaseName,procedureName,'Starting i2b2_process_vcf_data',0,stepCt,'Done') into rtnCd;

	select I2B2_LOAD_SAMPLES(trial_id, top_node, 'VCF', sourceCd, secure_study, jobID, shared_patients, strong_patient_check) into res;
	if res < 0 then
		SELECT cz_error_handler(jobID, procedureName, res,
														'I2B2_LOAD_SAMPLES error')
		INTO rtnCd;

		SELECT cz_end_audit(jobID, 'FAIL')
		INTO rtnCd;

		return res;
	end if;

	update deapp.de_variant_subject_summary v
	set assay_id = sm.assay_id
	from deapp.de_subject_sample_mapping sm
	where sm.platform = 'VCF' and sm.trial_name = TrialID and sm.source_cd = sourceCd
		and v.dataset_id = datasetId
		and sm.sample_cd = v.subject_id;

  get diagnostics rowCt := ROW_COUNT;
	stepCt := stepCt + 1;
	select cz_write_audit(jobId,databaseName,procedureName,'Associate deapp.de_subject_sample_mapping with deapp.de_variant_subject_summary',rowCt,stepCt,'Done') into rtnCd;

	stepCt := stepCt + 1;
	select cz_write_audit(jobId,databaseName,procedureName,'End i2b2_process_vcf_data',0,stepCt,'Done') into rtnCd;

	---Cleanup OVERALL JOB if this proc is being run standalone
	IF newJobFlag = 1
	THEN
		select cz_end_audit (jobID, 'SUCCESS') into rtnCd;
	END IF;

	return 1;

END;

$BODY$
  LANGUAGE plpgsql VOLATILE SECURITY DEFINER
  SET search_path FROM CURRENT
  COST 100;

