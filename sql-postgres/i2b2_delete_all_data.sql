create or replace function tm_cz.I2B2_DELETE_ALL_DATA
(
  trial_id character varying
 ,path_string varchar
 ,currentJobID numeric default -1
 ,removeTop varchar default 'N'
) returns numeric AS
$BODY$
Declare
  TrialID   varchar(100);
  pathString  VARCHAR(700);
  TrialType 	VARCHAR(250);
  sourceCD  	VARCHAR(250);
  
  --Audit variables
  rowCt		numeric(18,0);
  newJobFlag INTEGER;
  trialCount INTEGER;
  pathCount INTEGER;
  countNodeUnderTop INTEGER;
  topNode	VARCHAR(500);
  topNodeCount	INTEGER;
  sourceCDCount INTEGER;
  databaseName VARCHAR(100);
  procedureName VARCHAR(100);
  jobID numeric(18,0);
  stepCt numeric(18,0);
  rtnCd	 integer;
BEGIN
  if (path_string is not null) then
	pathString := REGEXP_REPLACE('\' || path_string || '\','(\\){2,}', '\','g');
  end if;
  
  procedureName := 'i2b2_delete_all_data';
  if (trial_id is null) then
	select count(distinct trial_name) into trialCount
		from DEAPP.de_subject_sample_mapping where concept_code in (
			select concept_cd from I2B2DEMODATA.concept_dimension where concept_path like path_string || '%' ESCAPE '`'
		);
	if (trialCount = 1) then
		select distinct trial_name into TrialId
			from DEAPP.de_subject_sample_mapping where concept_code in (
				select concept_cd from I2B2DEMODATA.concept_dimension where concept_path like path_string || '%' ESCAPE '`'
			);
	ELSIF ( trialCount = 0 ) THEN  
		TrialId := null;
	else
		stepCt := stepCt + 1;
		select tm_cz.cz_write_audit(jobId,databasename,procedurename,'Please select right path to study',1,stepCt,'ERROR') into rtnCd;
		select tm_cz.cz_error_handler(jobid,procedurename) into rtnCd;
		select tm_cz.cz_end_audit (jobId,'FAIL') into rtnCd;
		return -16;
	end if;
  else
	TrialId := trial_id;
  end if;
  
  if (path_string is null) then
    select count(concept_path) into pathCount 
      from I2B2DEMODATA.concept_dimension where concept_cd in (
        select concept_code from DEAPP.de_subject_sample_mapping where trial_name = TrialId
      );
    if (pathCount = 1) then
      WITH RECURSIVE temp1 (concept_path, PARENT_CONCEPT_PATH, level) as 
	(select concept_path, parent_concept_path, 1 from i2b2demodata.concept_counts t1 where concept_path = (
		select concept_path
		      from I2B2DEMODATA.concept_dimension t1
		      where concept_cd in (
			select concept_code from DEAPP.de_subject_sample_mapping where trial_name = TrialId
		      )
	      )
	 union 
	  select t2.concept_path, t2.parent_concept_path, level + 1 from i2b2demodata.concept_counts t2
	  inner join temp1 on (temp1.parent_concept_path = t2.concept_path)
	)
	select concept_path into pathString from temp1 order by parent_concept_path limit 1;
    else 
	stepCt := stepCt + 1;
	select tm_cz.cz_write_audit(jobId,databasename,procedurename,'Please select right trial to study',1,stepCt,'ERROR') into rtnCd;
	select tm_cz.cz_error_handler(jobid,procedurename) into rtnCd;
	select tm_cz.cz_end_audit (jobId,'FAIL') into rtnCd;
	return -16;
    end if;
  else 
    pathString := path_string;
  end if;
  
  select count(parent_concept_path) into topNodeCount
    from I2B2DEMODATA.concept_counts 
    where 
    concept_path = pathString;
  
  if (topNodeCount > 0) then
    select parent_concept_path into topNode
      from I2B2DEMODATA.concept_counts 
      where 
      concept_path = pathString;
  else
    topNode := pathString;
  end if;

  
  stepCt := 0;
  
  --Set Audit Parameters
  newJobFlag := 0; -- False (Default)
  jobID := currentJobID;

  /*SELECT sys_context('USERENV', 'CURRENT_SCHEMA') INTO databaseName FROM dual;*/
  

  --Audit JOB Initialization
  --If Job ID does not exist, then this is a single procedure run and we need to create it
  IF(jobID IS NULL or jobID < 1)
  THEN
    newJobFlag := 1; -- True
    select tm_cz.cz_start_audit (procedureName, databaseName) into rtnCd;
  END IF;
  
  if pathString != ''  or pathString != '%'
  then 
	stepCt := stepCt + 1;
  if (topNode is null) then
    select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Starting I2B2_DELETE_ALL_DATA from '||'/',0,stepCt,'Done') into rtnCd;
  else
    select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Starting I2B2_DELETE_ALL_DATA from '||topNode||' trial id '||trialId,0,stepCt,'Done') into rtnCd;
  end if;
	
  --	delete all i2b2 nodes
	
  select tm_cz.i2b2_delete_all_nodes(pathString,jobId) into rtnCd;
  
  --	delete any table_access data
  delete from i2b2metadata.table_access 
  where c_fullname like pathString || '%' ESCAPE '`';
	stepCt := stepCt + 1;
	get diagnostics rowCt := ROW_COUNT;
	select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from I2B2METADATA table_access',rowCt,stepCt,'Done') into rtnCd;
	--	delete any i2b2_tag data
	
	delete from i2b2metadata.i2b2_tags
	where path like pathString || '%' ESCAPE '`';
	stepCt := stepCt + 1;
	get diagnostics rowCt := ROW_COUNT;
	select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from I2B2METADATA i2b2_tags',rowCt,stepCt,'Done') into rtnCd;

	
	--	delete clinical data
	if (trialId is not NUll) 
	then
		delete from tm_lz.lz_src_clinical_data
		where study_id = trialId;
		stepCt := stepCt + 1;
		get diagnostics rowCt := ROW_COUNT;
		select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from lz_src_clinical_data',rowCt,stepCt,'Done') into rtnCd;
		
		/*Deleting data from de_variant_subject_summary*/
		delete from deapp.de_variant_subject_summary v
		  where assay_id = (select sm.assay_id
		  from deapp.de_subject_sample_mapping sm
		  where sm.trial_name = TrialID and sm.sample_cd = v.subject_id);
		stepCt := stepCt + 1;
		get diagnostics rowCt := ROW_COUNT;
		select cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from de_variant_subject_summary',rowCt,stepCt,'Done') into rtnCd;

		delete from deapp.de_variant_population_data where dataset_id = TrialId;
		stepCt := stepCt + 1;
		get diagnostics rowCt := ROW_COUNT;
		select cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from de_variant_population_data',rowCt,stepCt,'Done') into rtnCd;

    delete from deapp.de_variant_population_info where dataset_id = TrialId;
    stepCt := stepCt + 1;
    get diagnostics rowCt := ROW_COUNT;
		select cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from de_variant_population_info',rowCt,stepCt,'Done') into rtnCd;

    delete from deapp.de_variant_subject_detail where dataset_id = TrialId;
    stepCt := stepCt + 1;
    get diagnostics rowCt := ROW_COUNT;
		select cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from de_variant_subject_detail',rowCt,stepCt,'Done') into rtnCd;

    delete from deapp.de_variant_subject_idx where dataset_id = TrialId;
    stepCt := stepCt + 1;
    get diagnostics rowCt := ROW_COUNT;
		select cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from de_variant_subject_idx',rowCt,stepCt,'Done') into rtnCd;


    delete from deapp.de_variant_dataset where dataset_id = TrialId;
    stepCt := stepCt + 1;
    get diagnostics rowCt := ROW_COUNT;
		cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from de_variant_dataset',SQL%ROWCOUNT,stepCt,'Done');

		--	delete observation_fact SECURITY data, do before patient_dimension delete
		select count(x.source_cd) into sourceCDCount
			  from deapp.de_subject_sample_mapping x
			  where x.trial_name = trialId;
    if (sourceCDCount <>0) then
      select distinct x.source_cd into sourceCD
          from deapp.de_subject_sample_mapping x
          where x.trial_name = trialId;
    else 
      sourceCD := null;
    end if;
			  
		delete from i2b2demodata.observation_fact f
		where f.concept_cd = 'SECURITY'
		  and f.patient_num in
			 (select distinct p.patient_num from i2b2demodata.patient_dimension p
			  where p.sourcesystem_cd like trialId || '%');
		stepCt := stepCt + 1;
		get diagnostics rowCt := ROW_COUNT;
		select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Delete SECURITY data for trial from I2B2DEMODATA observation_fact',rowCt,stepCt,'Done') into rtnCd;
		/*commit;*/	
		
		
		delete from deapp.de_subject_microarray_data
		where trial_name = trialId 
		and assay_id in (
		  select dssm.assay_id from
			TM_LZ.lt_src_mrna_subj_samp_map ltssm
			left join
			deapp.de_subject_sample_mapping dssm
			on
			dssm.trial_name = ltssm.trial_name
			and dssm.gpl_id = ltssm.platform
			and dssm.subject_id = ltssm.subject_id
			and dssm.sample_cd  = ltssm.sample_cd
		  where
			dssm.trial_name = trialId
			and coalesce(dssm.source_cd,'STD') = sourceCd
		);
		stepCt := stepCt + 1;
		get diagnostics rowCt := ROW_COUNT;
		select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from deapp de_subject_microarray_data',rowCt,stepCt,'Done') into rtnCd;
		/*commit;*/	
		
		delete from deapp.de_subject_sample_mapping where
		  assay_id in (
			select dssm.assay_id from
			  TM_LZ.lt_src_mrna_subj_samp_map ltssm
			  left join
			  deapp.de_subject_sample_mapping dssm
			  on
			  dssm.trial_name     = ltssm.trial_name
			  and dssm.gpl_id     = ltssm.platform
			  and dssm.subject_id = ltssm.subject_id
			  and dssm.sample_cd  = ltssm.sample_cd
			where
			  dssm.trial_name = trialID
			  and coalesce(dssm.source_cd,'STD') = sourceCd);

		stepCt := stepCt + 1;
		get diagnostics rowCt := ROW_COUNT;
		select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Delete trial from DEAPP de_subject_sample_mapping',rowCt,stepCt,'Done') into rtnCd;

		/*commit;*/
		  

		stepCt := stepCt + 1;
		get diagnostics rowCt := ROW_COUNT;
		select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Delete trial from DEAPP de_subject_sample_mapping',rowCt,stepCt,'Done') into rtnCd;

		/*commit;*/
		--	delete patient data
		
		delete from i2b2demodata.patient_dimension
		where sourcesystem_cd like trialId || '%';
		stepCt := stepCt + 1;
		get diagnostics rowCt := ROW_COUNT;
		select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from I2B2DEMODATA patient_dimension',rowCt,stepCt,'Done') into rtnCd;
		/*commit;*/
		
		delete from i2b2demodata.patient_trial
		where trial=  trialId;
		stepCt := stepCt + 1;
		get diagnostics rowCt := ROW_COUNT;
		select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from I2B2DEMODATA patient_trial',rowCt,stepCt,'Done') into rtnCd;
		/*commit;*/
	end if;
	
	/*Check and delete top node, if remove node is last*/
	if (removeTop = 'N') then
    select count(*) into countNodeUnderTop
      from I2B2DEMODATA.concept_counts 
      where parent_concept_path = topNode;
      
      stepCt := stepCt + 1;
	get diagnostics rowCt := ROW_COUNT;
      select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Check need removed top node '||topNode,rowCt,stepCt,'Done') into rtnCd;
      
      if (countNodeUnderTop = 0) 
      then
        select tm_cz.i2b2_delete_all_data(null, topNode, jobID, 'Y') into rtnCd;
      end if;
  end if;

  end if;
  
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
