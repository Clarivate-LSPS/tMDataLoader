--
-- Name: i2b2_rbm_zscore_calc_new(character varying, character varying, character varying, numeric, character varying, bigint, character varying, bigint, character varying); Type: FUNCTION; Schema: tm_cz; Owner: -
--
CREATE OR REPLACE FUNCTION i2b2_rbm_zscore_calc_new(trial_id character varying, partition_name character varying, partition_indx character varying, partitionid numeric, run_type character varying DEFAULT 'L'::character varying, currentjobid bigint DEFAULT 0, data_type character varying DEFAULT 'R'::character varying, log_base bigint DEFAULT 2, source_cd character varying DEFAULT NULL::character varying) RETURNS void
    LANGUAGE plpgsql VOLATILE SECURITY DEFINER
    SET search_path FROM CURRENT
    AS $$
DECLARE

/*************************************************************************
This Stored Procedure is used in ETL load RBM data
Date:12/04/2013
******************************************************************/

  TrialID varchar(50);
  sourceCD	varchar(50);
  sqlText varchar(2000);
  runType varchar(10);
  dataType varchar(10);
  partitionName varchar(200);
  partitionindx varchar(200);
  stgTrial varchar(50);
  idxExists bigint;
  pExists	bigint;
  nbrRecs bigint;
  logBase bigint;
   
  --Audit variables
  newJobFlag integer;
  databaseName varchar(100);
  procedureName varchar(100);
  jobID bigint;
  stepCt bigint;
  rowCt			bigint;
  rtnCd			integer;  
  errorNumber		character varying;
  errorMessage	character varying;

BEGIN

	TrialId := trial_id;
	runType := run_type;
	dataType := data_type;
	logBase := log_base;
	sourceCd := source_cd;
	
	partitionindx := partition_indx;
	partitionName := partition_name;
	  
  --Set Audit Parameters
  newJobFlag := 0; -- False (Default)
  jobID := currentJobID;

	databaseName := current_schema();
	procedureName := 'i2b2_rbm_zscore_calc_new';

  --Audit JOB Initialization
  --If Job ID does not exist, then this is a single procedure run and we need to create it
  IF(coalesce(jobID::text, '') = '' or jobID < 1)
  THEN
    newJobFlag := 1; -- True
    select cz_start_audit (procedureName, databaseName, jobID) into rtnCd;
  END IF;
   
  stepCt := 0;
  
  	select count(*) into pExists
	from information_schema.tables
	where table_name = partitionindx;

	if pExists = 0 then
	sqlText := 'create table ' || partitionName || ' ( constraint rbm_' || partitionId::text || '_check check ( partition_id = ' || partitionId::text ||
	')) inherits (deapp.de_subject_rbm_data)';
	raise notice 'sqlText= %', sqlText;
	execute sqlText;
	stepCt := stepCt + 1;
	select cz_write_audit(jobId,databaseName,procedureName,'Create partition ' || partitionName,1,stepCt,'Done') into rtnCd;
	else
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
	sqlText := 'truncate table ' || partitionName;
	raise notice 'sqlText= %', sqlText;
	execute sqlText;
	stepCt := stepCt + 1;
	select cz_write_audit(jobId,databaseName,procedureName,'Truncate ' || partitionName,1,stepCt,'Done') into rtnCd;
	end if;
  
	stepCt := stepCt + 1;
	select cz_write_audit(jobId,databaseName,procedureName,'Starting zscore calc for ' || TrialId || ' RunType: ' || runType || ' dataType: ' || dataType,0,stepCt,'Done') into rtnCd;
  
	if runType != 'L' then
		stepCt := stepCt + 1;
		select cz_write_audit(jobId,databaseName,procedureName,'Invalid runType passed - procedure exiting'
,0,stepCt,'Done') into rtnCd;
		select cz_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
		select cz_end_audit (jobId,'FAIL') into rtnCd;
		return;
	end if;
  
--	For Load, make sure that the TrialId passed as parameter is the same as the trial in stg_subject_mrna_data
--	If not, raise exception

	if runType = 'L' then
		select distinct trial_name into stgTrial
		from WT_SUBJECT_RBM_PROBESET;
		
		if stgTrial != TrialId then
			stepCt := stepCt + 1;
			select cz_write_audit(jobId,databaseName,procedureName,'TrialId not the same as trial in WT_SUBJECT_RBM_PROBESET - procedure exiting'
,0,stepCt,'Done') into rtnCd;
			select cz_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			select cz_end_audit (jobId,'FAIL') into rtnCd;
			return;
		end if;
	end if;

	--remove Reload processing
--	For Reload, make sure that the TrialId passed as parameter has data in de_subject_rbm_data
--	If not, raise exception

	if runType = 'R' then 
		select count(*) into idxExists
		from deapp.de_subject_rbm_data
		where trial_name = TrialId;
		
		if idxExists = 0 then
			stepCt := stepCt + 1;
			select cz_write_audit(jobId,databaseName,procedureName,'No data for TrialId in de_subject_rbm_data - procedure exiting'
,0,stepCt,'Done') into rtnCd;
		select cz_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
		select cz_end_audit (jobId,'FAIL') into rtnCd;
		return;
		end if;
	end if;

   
--	truncate tmp tables

	EXECUTE('truncate table wt_subject_rbm_logs');
	EXECUTE('truncate table wt_subject_rbm_calcs');
	EXECUTE('truncate table wt_subject_rbm_med');

	EXECUTE('drop index if exists wt_subject_rbm_logs_i1');
		
	EXECUTE('drop index if exists wt_subject_rbm_calcs_i1');
	
	stepCt := stepCt + 1;
	select cz_write_audit(jobId,databaseName,procedureName,'Truncate work tables in TM_WZ',0,stepCt,'Done') into rtnCd;
	
	--	if dataType = L, use intensity_value as log_intensity
	--	if dataType = R, always use intensity_value

	begin
		if dataType = 'L' then
			insert into wt_subject_rbm_logs
				(probeset_id
				,intensity_value
				,assay_id
				,log_intensity
				,patient_id
				)
				select probeset
					  ,intensity_value  
					  ,assay_id 
					  ,intensity_value
					  ,patient_id
				from wt_subject_rbm_probeset
				where trial_name = TrialId;
			   
			--end if;
		else	
			insert into wt_subject_rbm_logs
				(probeset_id
				,intensity_value
				,assay_id
				,log_intensity
				,patient_id
				)
				select probeset
					  ,intensity_value 
					  ,assay_id 
					  ,CASE WHEN intensity_value <= 0 THEN log(2,(intensity_value + 0.001)) ELSE log(2,intensity_value) END
					  ,patient_id
				from wt_subject_rbm_probeset
				where trial_name = TrialId;
		end if;
	get diagnostics rowCt := ROW_COUNT;
	exception
	when others then
		errorNumber := SQLSTATE;
		errorMessage := SQLERRM;
		--Handle errors.
		select cz_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
		--End Proc
		select cz_end_audit (jobID, 'FAIL') into rtnCd;
		return;
	end;
	stepCt := stepCt + 1;
	select cz_write_audit(jobId,databaseName,procedureName,'Loaded data for trial in TM_WZ wt_subject_rbm_logs',rowCt,stepCt,'Done') into rtnCd;
    
	EXECUTE('create index wt_subject_rbm_logs_i1 on wt_subject_rbm_logs (trial_name, probeset_id) tablespace "indx"');
	stepCt := stepCt + 1;
	select cz_write_audit(jobId,databaseName,procedureName,'Create index on TM_WZ wt_subject_rbm_logs',0,stepCt,'Done') into rtnCd;
		
--	calculate mean_intensity, median_intensity, and stddev_intensity per experiment, probe
	begin
		insert into wt_subject_rbm_calcs
		(trial_name
		,probeset_id
		,mean_intensity
		,median_intensity
		,stddev_intensity
		)
		select d.trial_name 
			  ,d.probeset_id
			  ,avg(log_intensity)
			  ,median(log_intensity)
			  ,stddev(log_intensity)
		from wt_subject_rbm_logs d
		group by d.trial_name 
				,d.probeset_id;
	get diagnostics rowCt := ROW_COUNT;
	exception
	when others then
		errorNumber := SQLSTATE;
		errorMessage := SQLERRM;
		--Handle errors.
		select cz_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
		--End Proc
		select cz_end_audit (jobID, 'FAIL') into rtnCd;
		return;
	end;
	stepCt := stepCt + 1;
	select cz_write_audit(jobId,databaseName,procedureName,'Calculate intensities for trial in TM_WZ wt_subject_rbm_calcs',rowCt,stepCt,'Done') into rtnCd;


	EXECUTE('create index wt_subject_rbm_calcs_i1 on wt_subject_rbm_calcs (trial_name, probeset_id) tablespace "indx"');
	stepCt := stepCt + 1;
	select cz_write_audit(jobId,databaseName,procedureName,'Create index on TM_WZ wt_subject_rbm_calcs',0,stepCt,'Done') into rtnCd;
		
-- calculate zscore
	begin
		insert into wt_subject_rbm_med
		(probeset_id
		,intensity_value
		,log_intensity
		,assay_id
		,mean_intensity
		,stddev_intensity
		,median_intensity
		,zscore
		,patient_id
		)
		select d.probeset_id
			  ,d.intensity_value 
			  ,d.log_intensity 
			  ,d.assay_id  
			  ,c.mean_intensity 
			  ,c.stddev_intensity 
			  ,c.median_intensity 
			  ,(CASE WHEN stddev_intensity=0 THEN 0 ELSE (log_intensity - median_intensity ) / stddev_intensity END)
			  ,d.patient_id
		from wt_subject_rbm_logs d
			,wt_subject_rbm_calcs c
		where d.probeset_id = c.probeset_id;
	get diagnostics rowCt := ROW_COUNT;
	exception
	when others then
		errorNumber := SQLSTATE;
		errorMessage := SQLERRM;
		--Handle errors.
		select cz_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
		--End Proc
		select cz_end_audit (jobID, 'FAIL') into rtnCd;
		return;
	end;
	stepCt := stepCt + 1;
	select cz_write_audit(jobId,databaseName,procedureName,'Calculate Z-Score for trial in TM_WZ wt_subject_rbm_med',rowCt,stepCt,'Done') into rtnCd;

	
	-- insert into de_subject_rbm_data when dataType is T (transformed)

  begin
	sqlText := 'insert into ' || partitionName || 
	'(partition_id, trial_name, antigen_name, patient_id, gene_symbol, gene_id, assay_id ' ||
        ',concept_cd, value, normalized_value, unit, zscore, id) ' ||
	'select ' || partitioniD::text || ', ''' || TrialId || '''' ||
              ',(CASE WHEN position(''('' in m.probeset_id) <> 0 THEN trim(substr(m.probeset_id,1,instr(m.probeset_id,''('')-1)) ELSE m.probeset_id END)' ||
              ',m.patient_id ' ||
              ',a.gene_symbol  ' ||
              ',a.gene_id::integer  ' ||
              ',m.assay_id ' ||
              ',d.concept_code ' ||
              ',m.intensity_value ' ||
              ',round(case when ''' || dataType || ''' = ''R'' then m.intensity_value::numeric ' ||
				'when ''' || dataType || ''' = ''L''  ' ||
				'then case when ''' || logBase || ''' = -1 then null else power( ''' || logBase || ''' , m.log_intensity)::numeric end ' ||
				'else null ' ||
				'end,4) as normalized_value ' ||
              ',(CASE WHEN position(''('' in m.probeset_id) <> 0 THEN trim(substr(m.probeset_id ,instr(m.probeset_id ,''('',-1,1),length(m.probeset_id ))) ELSE m.probeset_id END) ' ||
              ',(CASE WHEN m.zscore < -2.5 THEN -2.5 WHEN m.zscore >  2.5 THEN  2.5 ELSE m.zscore END) ' ||
			  ',nextval(''deapp.RBM_ANNOTATION_ID'') '||
	'from wt_subject_rbm_med m ' ||
        ',wt_subject_rbm_probeset p ' ||
        ',deapp.DE_RBM_ANNOTATION a ' ||
        ',deapp.de_subject_sample_mapping d ' ||
        'where  ' ||
        '(CASE WHEN position(''('' in p.probeset) <> 0 THEN trim(substr(p.probeset,1,instr(p.probeset,''('')-1)) ELSE p.probeset END) = trim(a.antigen_name)' ||
      'and   d.subject_id=p.subject_id ' ||
        'and p.platform=a.gpl_id ' ||
        'and m.assay_id=p.assay_id ' ||
        'and d.gpl_id=p.platform ' ||
        'and d.patient_id=p.patient_id ' ||
        'and d.concept_code in (select concept_cd from  i2b2demodata.concept_dimension where concept_cd=d.concept_code) ' ||
        'and d.trial_name=''' || TrialId || '''' ||
        'and p.patient_id=m.patient_id ' ||
        'and p.probeset=m.probeset_id ' ;
		
	raise notice 'sqlText= %', sqlText;
	execute sqlText;
	get diagnostics rowCt := ROW_COUNT;
	exception
	when others then
		errorNumber := SQLSTATE;
		errorMessage := SQLERRM;
		--Handle errors.
		select cz_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
		--End Proc
		select cz_end_audit (jobID, 'FAIL') into rtnCd;
		return;
	end;

	stepCt := stepCt + 1;
	select cz_write_audit(jobId,databaseName,procedureName,'Inserted data into ' || partitionName,rowCt,stepCt,'Done') into rtnCd;

	begin
		-- FIXME: Drop constraint (and don't recover, because it don't work with inherited tables)
    ALTER TABLE deapp.de_rbm_data_annotation_join DROP CONSTRAINT IF EXISTS de_rbm_data_ann_jn_data_id_fk;

	insert into DEAPP.DE_RBM_DATA_ANNOTATION_JOIN
	select d.id, ann.id from deapp.de_subject_rbm_data d
	inner join deapp.de_rbm_annotation ann on ann.antigen_name = d.antigen_name
	inner join deapp.de_subject_sample_mapping ssm on ssm.assay_id = d.assay_id and ann.gpl_id = ssm.gpl_id
	where not exists( select * from deapp.de_rbm_data_annotation_join j where j.data_id = d.id AND j.annotation_id = ann.id );
  get diagnostics rowCt := ROW_COUNT;
	exception
	when others then
		errorNumber := SQLSTATE;
		errorMessage := SQLERRM;
		--Handle errors.
		select cz_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
		--End Proc
		select cz_end_audit (jobID, 'FAIL') into rtnCd;
		return;
	end;

	-- create indexes on partition
	sqlText := ' create index ' || partitionIndx || '_idx1 on ' || partitionName || ' using btree (partition_id) tablespace indx';
	raise notice 'sqlText= %', sqlText;
	execute sqlText;
	sqlText := ' create index ' || partitionIndx || '_idx2 on ' || partitionName || ' using btree (assay_id) tablespace indx';
	raise notice 'sqlText= %', sqlText;
	execute sqlText;
	sqlText := ' create index ' || partitionIndx || '_idx3 on ' || partitionName || ' using btree (antigen_name) tablespace indx';
	raise notice 'sqlText= %', sqlText;
	execute sqlText;
	sqlText := ' create index ' || partitionIndx || '_idx4 on ' || partitionName || ' using btree (assay_id, antigen_name) tablespace indx';
	raise notice 'sqlText= %', sqlText;
	execute sqlText;
    
    ---Cleanup OVERALL JOB if this proc is being run standalone
  IF newJobFlag = 1
  THEN 
    select cz_end_audit (jobID, 'SUCCESS') into rtnCd; 
  END IF;

END;
 
$$;

ALTER FUNCTION i2b2_rbm_zscore_calc_new(character varying, character varying, character varying, numeric, character varying, bigint, character varying, bigint, character varying)
	OWNER TO postgres;