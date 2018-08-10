-- Function: i2b2_process_mrna_data(character varying, character varying, character varying, character varying, numeric, character varying, numeric)

-- DROP FUNCTION i2b2_process_mrna_data(character varying, character varying, character varying, character varying, numeric, character varying, numeric);

CREATE OR REPLACE FUNCTION i2b2_process_mrna_data
(   trial_id             CHARACTER VARYING
	, top_node             CHARACTER VARYING
	,	data_type            CHARACTER VARYING DEFAULT 'R'    --	R = raw data, do zscore calc, T = transformed data, load raw values as zscore,
																													--	L = log intensity data, skip log step in zscore calc
	, source_cd            CHARACTER VARYING DEFAULT 'STD'    --	default source_cd = 'STD'
	, log_base             NUMERIC DEFAULT 2          --	log base value for conversion back to raw
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

	dataType		varchar(10);
	sqlText			varchar(1000);
	pExists			numeric;
	partExists 		numeric;
	logBase			numeric;
	pCount			integer;
	partitioniD		numeric(18,0);
	partitionName	varchar(100);
	partitionIndx	varchar(100);
	res numeric;
BEGIN
	TrialID := upper(trial_id);

	--Set Audit Parameters
	newJobFlag := 0; -- False (Default)
	jobID := currentJobID;
	databaseName := current_schema();
	procedureName := 'I2B2_PROCESS_MRNA_DATA';

	--Audit JOB Initialization
	--If Job ID does not exist, then this is a single procedure run and we need to create it

	IF(jobID IS NULL or jobID < 1)
	THEN
		newJobFlag := 1; -- True
		select cz_start_audit (procedureName, databaseName) into jobID;
	END IF;

	stepCt := 0;
	stepCt := stepCt + 1;
	select cz_write_audit(jobId,databaseName,procedureName,'Starting i2b2_process_mrna_data',0,stepCt,'Done') into rtnCd;

	if data_type is null then
		dataType := 'R';
	else
		if data_type in ('R','T','L','Z') then
			dataType := data_type;
		else
			dataType := 'R';
		end if;
	end if;

	logBase := coalesce(log_base, 2);
	sourceCd := upper(coalesce(source_cd,'STD'));

	--	check if platform exists in probeset_deapp .  If not, abort run.
	select count(*) into pCount
	from probeset_deapp
	where platform in (select distinct m.platform from lt_src_mrna_subj_samp_map m);

	if pCount = 0 then
		select cz_write_audit(jobId,databaseName,procedureName,'No Gene Expression platforms in deapp.de_mrna_annotation',0,pCount,'Done') into rtnCd;
		select cz_error_handler (jobID, procedureName, '-1', 'Application raised error') into rtnCd;
		select cz_end_audit (jobID, 'FAIL') into rtnCd;
		return -16;
	end if;

	select I2B2_LOAD_SAMPLES(trial_id, top_node, 'MRNA_AFFYMETRIX', sourceCd, secure_study, jobID, shared_patients, strong_patient_check) into res;
	if res < 0 then
		SELECT cz_error_handler(jobID, procedureName, res,
														'I2B2_LOAD_SAMPLES error')
		INTO rtnCd;

		SELECT cz_end_audit(jobID, 'FAIL')
		INTO rtnCd;

		return res;
	end if;

	--	check if trial/source_cd already loaded, if yes, get existing partition_id else get new one
	select count(*) into partExists
	from deapp.de_subject_sample_mapping sm
	where sm.trial_name = TrialId
	  and coalesce(sm.source_cd,'STD') = sourceCd
	  and sm.platform = 'MRNA_AFFYMETRIX'
	  and sm.partition_id is not null;

	if partExists = 0 then
		select nextval('deapp.seq_mrna_partition_id') into partitionId;
	else
		select distinct partition_id into partitionId
		from deapp.de_subject_sample_mapping sm
		where sm.trial_name = TrialId
		  and coalesce(sm.source_cd,'STD') = sourceCd
		  and sm.platform = 'MRNA_AFFYMETRIX';
	end if;

	partitionName := 'deapp.de_subject_microarray_data_' || partitionId::text;
	partitionIndx := 'de_subject_microarray_data_' || partitionId::text;

	--	tag data with probeset_id from reference.probeset_deapp

	execute ('truncate table wt_subject_mrna_probeset');

	--	note: assay_id represents a unique subject/site/sample

	begin
	insert into wt_subject_mrna_probeset
	(probeset_id
	,intensity_value
	,assay_id
	,trial_name
	)
	select gs.probeset_id
		  ,avg(md.intensity_value::double precision)
		  ,sd.assay_id
		  ,TrialId
	from
	  lt_src_mrna_data md
      inner join deapp.de_subject_sample_mapping sd
        inner join probeset_deapp gs
        on sd.gpl_id = gs.platform
      on md.expr_id = sd.sample_cd and md.probeset = gs.probeset
	where sd.platform = 'MRNA_AFFYMETRIX'
	  and sd.trial_name = TrialId
	  and sd.source_cd = sourceCd
	  and sd.gpl_id = (select distinct platform from lt_src_mrna_subj_samp_map)
	  and case when dataType = 'R'
			   then case when md.intensity_value::double precision > 0 then 1 else 0 end
			   else 1 end = 1         --	take only >0 for dataType R
	group by gs.probeset_id
		  ,sd.assay_id;
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
	select cz_write_audit(jobId,databaseName,procedureName,'Insert into DEAPP wt_subject_mrna_probeset',rowCt,stepCt,'Done') into rtnCd;

	if rowCt = 0 then
		select cz_write_audit(jobId,databaseName,procedureName,'Unable to match probesets to platform in probeset_deapp',0,rowCt,'Done') into rtnCd;
		select cz_error_handler (jobID, procedureName, '-1', 'Application raised error') into rtnCd;
		select cz_end_audit (jobID, 'FAIL') into rtnCd;
		return -16;
	end if;
	
	--	add partition if it doesn't exist, drop indexes and truncate if it does (reload)

	select count(*) into pExists
	from information_schema.tables
	where table_name = partitionindx;
	
	if pExists = 0 then
		sqlText := 'create table ' || partitionName || ' ( constraint mrna_' || partitionId::text || '_check check ( partition_id = ' || partitionId::text ||
					')) inherits (deapp.de_subject_microarray_data)';
		raise notice 'sqlText= %', sqlText;
		execute sqlText;
		stepCt := stepCt + 1;
		select cz_write_audit(jobId,databaseName,procedureName,'Create partition ' || partitionName,1,stepCt,'Done') into rtnCd;
	else
        -- Keep this statement for backward compatibility
		sqlText := 'drop index if exists ' || partitionIndx || '_idx1';
		raise notice 'sqlText= %', sqlText;
		execute sqlText;
		sqlText := 'drop index if exists ' || partitionIndx || '_idx2';
		raise notice 'sqlText= %', sqlText;
		execute sqlText;
		sqlText := 'drop index if exists ' || partitionIndx || '_idx3';
		raise notice 'sqlText= %', sqlText;
		execute sqlText;
		sqlText := 'drop index if exists ' || partitionIndx || '_idx4';
		raise notice 'sqlText= %', sqlText;
		execute sqlText;
		stepCt := stepCt + 1;
		select cz_write_audit(jobId,databaseName,procedureName,'Drop indexes on ' || partitionName,1,stepCt,'Done') into rtnCd;

    sqlText := 'delete from ' || partitionName || ' where assay_id in (' ||
     'select sm.assay_id from deapp.de_subject_sample_mapping sm, lt_src_mrna_subj_samp_map tsm'
     || ' where sm.trial_name = ''' || TrialID || ''' and sm.source_cd = '''|| sourceCD || ''''
     || ' and coalesce(sm.site_id, '''') = coalesce(tsm.site_id, '''') and sm.subject_id = tsm.subject_id '
     || 'and sm.sample_cd = tsm.sample_cd and sm.gpl_id = tsm.platform)';
    raise notice 'sqlText= %', sqlText;
    execute sqlText;
		stepCt := stepCt + 1;
		select cz_write_audit(jobId,databaseName,procedureName,'Truncate ' || partitionName,1,stepCt,'Done') into rtnCd;
	end if;

	--	insert into de_subject_microarray_data when dataType is T (transformed)

	if dataType = 'T' or dataType = 'Z' then -- Z is for compatibility with TR ETL default settings
		sqlText := 'insert into ' || partitionName || ' (partition_id, trial_name, probeset_id, assay_id, log_intensity, zscore) ' ||
				   'select ' || partitionId::text || ', trial_name, probeset_id, assay_id, intensity_value, ' ||
				   'case when intensity_value < -2.5 then -2.5 when intensity_value > 2.5 then 2.5 else intensity_value end ' ||
				   'from wt_subject_mrna_probeset';
		raise notice 'sqlText= %', sqlText;
		execute sqlText;
		get diagnostics rowCt := ROW_COUNT;
		stepCt := stepCt + 1;
		select cz_write_audit(jobId,databaseName,procedureName,'Inserted data into ' || partitionName,rowCt,stepCt,'Done') into rtnCd;
	else
		--	calculate zscore and insert to partition

		execute ('drop index if exists wt_subject_mrna_logs_i1');
		execute ('drop index if exists wt_subject_mrna_calcs_i1');
		execute ('truncate table wt_subject_microarray_logs');
		execute ('truncate table wt_subject_microarray_calcs');
		execute ('truncate table wt_subject_microarray_med');
		stepCt := stepCt + 1;
		select cz_write_audit(jobId,databaseName,procedureName,'Drop indexes and truncate zscore work tables',1,stepCt,'Done') into rtnCd;

		begin
		insert into wt_subject_microarray_logs
		(probeset_id
		,assay_id
		,raw_intensity
		,log_intensity
		,trial_name
		)
		select probeset_id
			  ,assay_id
			  ,case when dataType = 'R' then intensity_value else 
				    case when logBase = -1 then 0 else power(logBase::double precision, intensity_value::double precision) end
			   end
			  ,case when dataType = 'L' then intensity_value else ln(intensity_value::double precision) / ln(logBase::double precision) end
			  ,trial_name
		from wt_subject_mrna_probeset;
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
		select cz_write_audit(jobId,databaseName,procedureName,'Loaded data for trial in wt_subject_microarray_logs',rowCt,stepCt,'Done') into rtnCd;

		execute ('create index wt_subject_mrna_logs_i1 on wt_subject_microarray_logs (probeset_id) tablespace "indx"');
		stepCt := stepCt + 1;
		select cz_write_audit(jobId,databaseName,procedureName,'Create index on wt_subject_microarray_logs',0,stepCt,'Done') into rtnCd;

		--	calculate mean_intensity, median_intensity, and stddev_intensity per probeset

		begin
		insert into wt_subject_microarray_calcs
		(probeset_id
		,mean_intensity
		,median_intensity
		,stddev_intensity
		,trial_name
		)
		select d.probeset_id
			  ,avg(log_intensity)
			  ,median(log_intensity::double precision)
			  ,stddev(log_intensity)
			  ,TrialID
		from wt_subject_microarray_logs d
		group by d.probeset_id;
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
		select cz_write_audit(jobId,databaseName,procedureName,'Calculate intensities for trial in wt_subject_microarray_calcs',rowCt,stepCt,'Done') into rtnCd;

		execute ('create index wt_subject_mrna_calcs_i1 on wt_subject_microarray_calcs (probeset_id) tablespace "indx"');
		stepCt := stepCt + 1;
		select cz_write_audit(jobId,databaseName,procedureName,'Create index on wt_subject_microarray_calcs',0,stepCt,'Done') into rtnCd;

		-- calculate zscore and insert into partition

		sqlText := 'insert into ' || partitionName || ' (partition_id, trial_name, probeset_id, assay_id, raw_intensity, log_intensity, zscore) ' ||
				   'select ' || partitionId::text || ', d.trial_name, d.probeset_id, d.assay_id, d.raw_intensity, d.log_intensity, ' ||
				   'case when c.stddev_intensity = 0 then 0 else ' ||
				   'case when (d.log_intensity - c.median_intensity ) / c.stddev_intensity < -2.5 then -2.5 ' ||
				   'when (d.log_intensity - c.median_intensity ) / c.stddev_intensity > 2.5 then 2.5 else ' ||
				   '(d.log_intensity - c.median_intensity ) / c.stddev_intensity end end ' ||
				   'from wt_subject_microarray_logs d ' ||
				   ',wt_subject_microarray_calcs c ' ||
				   'where d.probeset_id = c.probeset_id';
		raise notice 'sqlText= %', sqlText;
		execute sqlText;
		get diagnostics rowCt := ROW_COUNT;
		stepCt := stepCt + 1;
		select cz_write_audit(jobId,databaseName,procedureName,'Calculate Z-Score and insert into ' || partitionName,rowCt,stepCt,'Done') into rtnCd;
	end if;

	--	create indexes on partition
	sqlText := ' create index ' || partitionIndx || '_idx2 on ' || partitionName || ' using btree (assay_id) tablespace indx';
	raise notice 'sqlText= %', sqlText;
	execute sqlText;
	sqlText := ' create index ' || partitionIndx || '_idx3 on ' || partitionName || ' using btree (probeset_id) tablespace indx';
	raise notice 'sqlText= %', sqlText;
	execute sqlText;
	sqlText := ' create index ' || partitionIndx || '_idx4 on ' || partitionName || ' using btree (assay_id, probeset_id) tablespace indx';
	raise notice 'sqlText= %', sqlText;
	execute sqlText;
    ---Cleanup OVERALL JOB if this proc is being run standalone

	stepCt := stepCt + 1;
	select cz_write_audit(jobId,databaseName,procedureName,'End i2b2_process_mrna_data',0,stepCt,'Done') into rtnCd;

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

