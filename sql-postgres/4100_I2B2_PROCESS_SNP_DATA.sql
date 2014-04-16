-- Function: tm_cz.i2b2_process_snp_data(character varying, character varying, character varying, character varying, numeric, character varying, numeric)

-- DROP FUNCTION tm_cz.i2b2_process_snp_data(character varying, character varying, character varying, character varying, numeric, character varying, numeric);

CREATE OR REPLACE FUNCTION tm_cz.i2b2_process_snp_data
( trial_id 		character varying
 ,top_node		character varying
 ,data_type		character varying default 'R'		--	R = raw data, do zscore calc, T = transformed data, load raw values as zscore,
													--	L = log intensity data, skip log step in zscore calc
 ,source_cd		character varying default 'STD'		--	default source_cd = 'STD'
 ,log_base		numeric default 2					--	log base value for conversion back to raw
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

	sqlText			varchar(1000);
	pCount			integer;
	res numeric;
BEGIN
	TrialID := upper(trial_id);

	--Set Audit Parameters
	newJobFlag := 0; -- False (Default)
	jobID := currentJobID;
	databaseName := 'TM_CZ';
	procedureName := 'I2B2_PROCESS_SNP_DATA';

	--Audit JOB Initialization
	--If Job ID does not exist, then this is a single procedure run and we need to create it

	IF(jobID IS NULL or jobID < 1)
	THEN
		newJobFlag := 1; -- True
		select tm_cz.cz_start_audit (procedureName, databaseName) into jobID;
	END IF;

	stepCt := 0;
	stepCt := stepCt + 1;
	select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Starting i2b2_process_snp_data',0,stepCt,'Done') into rtnCd;

	sourceCd := upper(coalesce(source_cd,'STD'));

	select tm_cz.I2B2_LOAD_SAMPLES(trial_id, top_node, 'SNP', sourceCd, secure_study, jobID) into res;
	if res < 0 then
	  return res;
	end if;

	  -- Load SNP data from temp tables
  delete from deapp.de_snp_calls_by_gsm
  where patient_num in (
    select sm.omic_patient_id
    from deapp.de_subject_sample_mapping sm, tm_lz.lt_src_mrna_subj_samp_map tsm
    where sm.trial_name = TrialID
      and sm.source_cd = sourceCD
		  and coalesce(sm.site_id, '') = coalesce(tsm.site_id, '')
		  and sm.subject_id = tsm.subject_id and sm.sample_cd = tsm.sample_cd
		  and sm.platform = 'SNP'
  );

  get diagnostics rowCt := ROW_COUNT;
  stepCt := stepCt + 1;
	select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Cleanup de_snp_calls_by_gsm',rowCt,stepCt,'Done') into rtnCd;

  -- Load SNP calls
  insert into deapp.de_snp_calls_by_gsm
  (gsm_num, patient_num, snp_name, snp_calls)
  select sm.sample_cd as gsm_num, sm.omic_patient_id as patient_num, tmp.snp_name as snp_name, tmp.snp_calls as snp_calls
  from tm_lz.lt_snp_calls_by_gsm tmp
  inner join deapp.de_subject_sample_mapping sm
  on sm.sample_cd = tmp.gsm_num
  where sm.trial_name = TrialID;

  get diagnostics rowCt := ROW_COUNT;
  stepCt := stepCt + 1;
	select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Fill de_snp_calls_by_gsm',rowCt,stepCt,'Done') into rtnCd;

  delete from deapp.de_snp_copy_number
  where patient_num in (
    select sm.omic_patient_id
    from deapp.de_subject_sample_mapping sm, tm_lz.lt_src_mrna_subj_samp_map tsm
    where sm.trial_name = TrialID
      and sm.source_cd = sourceCD
		  and coalesce(sm.site_id, '') = coalesce(tsm.site_id, '')
		  and sm.subject_id = tsm.subject_id and sm.sample_cd = tsm.sample_cd
		  and sm.platform = 'SNP'
  );

  get diagnostics rowCt := ROW_COUNT;
  stepCt := stepCt + 1;
	select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Cleanup de_snp_copy_number',rowCt,stepCt,'Done') into rtnCd;

  insert into deapp.de_snp_copy_number
  (patient_num, snp_name, chrom, chrom_pos, copy_number)
  select sm.omic_patient_id as patient_num, tmp.snp_name as snp_name, tmp.chrom, tmp.chrom_pos, power(2::double precision, tmp.copy_number::double precision)
  from tm_lz.lt_snp_copy_number tmp
  inner join deapp.de_subject_sample_mapping sm
  on sm.sample_cd = tmp.gsm_num
  where sm.trial_name = TrialID;

  get diagnostics rowCt := ROW_COUNT;
  stepCt := stepCt + 1;
	select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Fill de_snp_copy_number',rowCt,stepCt,'Done') into rtnCd;

	stepCt := stepCt + 1;
	select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'End i2b2_process_snp_data',0,stepCt,'Done') into rtnCd;

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
ALTER FUNCTION tm_cz.i2b2_process_snp_data(character varying, character varying, character varying, character varying, numeric, character varying, numeric) SET search_path=tm_cz, tm_lz, tm_wz, deapp, i2b2demodata, pg_temp;

ALTER FUNCTION tm_cz.i2b2_process_snp_data(character varying, character varying, character varying, character varying, numeric, character varying, numeric)
  OWNER TO postgres;
