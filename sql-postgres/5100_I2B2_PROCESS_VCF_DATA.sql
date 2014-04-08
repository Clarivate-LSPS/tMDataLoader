-- Function: tm_cz.i2b2_process_vcf_data(character varying, character varying, character varying, character varying, numeric)

-- DROP FUNCTION tm_cz.i2b2_process_vcf_data(character varying, character varying, character varying, character varying, numeric);

CREATE OR REPLACE FUNCTION tm_cz.i2b2_process_vcf_data
( trial_id 		character varying
 ,top_node		character varying
 ,source_cd		character varying default 'STD'		--	default source_cd = 'STD'
 ,secure_study	character varying	default 'N'		--	security setting if new patients added to patient_dimension
 ,currentJobID 	numeric default -1
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

	res numeric;
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
		select tm_cz.cz_start_audit (procedureName, databaseName) into jobID;
	END IF;

	stepCt := 0;
	stepCt := stepCt + 1;
	select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Starting i2b2_process_vcf_data',0,stepCt,'Done') into rtnCd;

	sourceCd := upper(coalesce(source_cd,'STD'));

	select tm_cz.I2B2_LOAD_SAMPLES(trial_id, top_node, 'VCF', sourceCd, secure_study, jobID) into res;
	if res < 0 then
	  return res;
	end if;

	update deapp.de_variant_subject_summary v
	set assay_id = sm.assay_id
	from deapp.de_subject_sample_mapping sm
	where sm.trial_name = TrialID and sm.sample_cd = v.subject_id;

  get diagnostics rowCt := ROW_COUNT;
	stepCt := stepCt + 1;
	select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Associate deapp.de_subject_sample_mapping with deapp.de_variant_subject_summary',rowCt,stepCt,'Done') into rtnCd;

	stepCt := stepCt + 1;
	select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'End i2b2_process_vcf_data',0,stepCt,'Done') into rtnCd;

	---Cleanup OVERALL JOB if this proc is being run standalone
	IF newJobFlag = 1
	THEN
		select tm_cz.cz_end_audit (jobID, 'SUCCESS') into rtnCd;
	END IF;

	return 1;

END;

$BODY$
  LANGUAGE plpgsql VOLATILE SECURITY DEFINER
  COST 100;
ALTER FUNCTION tm_cz.i2b2_process_vcf_data(character varying, character varying, character varying, character varying, numeric) SET search_path=tm_cz, tm_lz, tm_wz, deapp, i2b2demodata, pg_temp;

ALTER FUNCTION tm_cz.i2b2_process_vcf_data(character varying, character varying, character varying, character varying, numeric)
  OWNER TO postgres;
