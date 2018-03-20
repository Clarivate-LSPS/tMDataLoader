create or replace function I2B2_DELETE_ALL_DATA
(
  trial_id character varying
 ,path_string varchar
 ,currentJobID numeric default -1
) returns numeric AS
$BODY$
Declare
  TrialID   varchar(100);
  bioexpid  bigint;
  pathString  VARCHAR(700);
  TrialType 	VARCHAR(250);
  sourceCD  	VARCHAR(250);

  rowsExists INTEGER;
  studyNum NUMERIC(18,0);

  -- vcf datasets
  vcfDataSetId varchar(100);
  vcfDataSets CURSOR(trialId VARCHAR) IS
    select distinct v.dataset_id
    from  deapp.de_subject_sample_mapping sm, deapp.de_variant_subject_summary v
    where sm.assay_id = v.assay_id and sm.trial_name = trialId;

  --Audit variables
  rowCt		numeric(18,0);
  trialCount INTEGER;
  pathCount INTEGER;
  countNodeUnderTop INTEGER;
  topNode	VARCHAR(500);
  topNodeCount	INTEGER;
  isExistTopNode integer;
  sourceCDCount INTEGER;
  databaseName VARCHAR(100);
  procedureName VARCHAR(100);
  jobID numeric(18,0);
  stepCt numeric(18,0);
  rtnCd	 integer;
  nCount  integer;
BEGIN
  databaseName := current_schema();
  procedureName := 'i2b2_delete_all_data';

  select case when coalesce(currentjobid, -1) < 1 then cz_start_audit(procedureName, databaseName) else currentjobid end into jobId;
  stepCt := 0;

  if (path_string is null and trial_id is null) then
    stepCt := stepCt + 1;
    select cz_write_audit(jobId,databasename,procedurename, 'Path string and study id are null',1,stepCt,'ERROR') into rtnCd;
    select cz_error_handler(jobid, procedurename, '-1', 'Application raised error') into rtnCd;
    select cz_end_audit (jobId,'FAIL') into rtnCd;
    return -16;
  end if;

  if (path_string is null) then
    SELECT DISTINCT
      first_value(concept_path) over (partition by sourcesystem_cd order by concept_path)
    INTO pathString
    FROM i2b2demodata.concept_dimension
    WHERE sourcesystem_cd = trial_id;
  else
    pathString := path_string;
    pathString := REGEXP_REPLACE('\' || pathString || '\','(\\){2,}', '\','g');
  end if;

  if (trial_id is null) then
	  select count(distinct sourcesystem_cd) into trialCount
		from i2b2metadata.i2b2
		where c_fullname like pathString || '%' ESCAPE '`';

    if (trialCount = 1) then
			select distinct sourcesystem_cd into TrialId
			from i2b2metadata.i2b2
			where c_fullname like pathString || '%' ESCAPE '`';
    ELSIF ( trialCount = 0 ) THEN
      TrialId := null;
    else
      stepCt := stepCt + 1;
      select cz_write_audit(jobId,databasename,procedurename,'Please select right path to study',1,stepCt,'ERROR') into rtnCd;
      select cz_error_handler(jobid, procedurename, '-1', 'Application raised error') into rtnCd;
      select cz_end_audit (jobId,'FAIL') into rtnCd;
      return -16;
    end if;
  else
	  TrialId := trial_id;
  end if;


  if (pathString is not null and (pathString != '' or pathString != '%')) then
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
      topNode := substring(pathString from 1 for position('\' in substring(pathString from 2))+1);
    end if;

	  stepCt := stepCt + 1;
    if (TrialID is null) then
      select cz_write_audit(jobId,databaseName,procedureName,'Starting I2B2_DELETE_ALL_DATA for ''' || pathString || '''',0,stepCt,'Done') into rtnCd;
    else
      select cz_write_audit(jobId,databaseName,procedureName,'Starting I2B2_DELETE_ALL_DATA for ''' || pathString || ''' with id: ' || TrialId,0,stepCt,'Done') into rtnCd;
    end if;

    --	delete all i2b2 nodes
    select i2b2_delete_all_nodes(pathString,jobId) into rtnCd;

    --	delete any table_access data
    delete from i2b2metadata.table_access
    where c_fullname like pathString || '%' ESCAPE '`';

    --	delete any i2b2_tag data
	  delete from i2b2metadata.i2b2_tags
	  where path like pathString || '%' ESCAPE '`';
	  stepCt := stepCt + 1;
	  get diagnostics rowCt := ROW_COUNT;
	  select cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from I2B2METADATA i2b2_tags',rowCt,stepCt,'Done') into rtnCd;
  end if;

	--	delete clinical data
	if (trialId is not NUll) then
		delete from gwas_plink.plink_data where study_id = trialId;
		stepCt := stepCt + 1;
		get diagnostics rowCt := ROW_COUNT;
		select cz_write_audit(jobId,databaseName,procedureName,'Delete gwas_plink.plink_data for trial',rowCt,stepCt,'Done') into rtnCd;

		delete from lz_src_clinical_data
		where study_id = trialId;
		stepCt := stepCt + 1;
		get diagnostics rowCt := ROW_COUNT;
		select cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from lz_src_clinical_data',rowCt,stepCt,'Done') into rtnCd;

    FOR vcfDataSet in vcfDataSets(TrialID) LOOP
      vcfDataSetId := vcfDataSet.dataset_id;

      /*Deleting data from de_variant_subject_summary*/
      delete from deapp.de_variant_subject_summary v
      where v.dataset_id = vcfDataSetId;

      stepCt := stepCt + 1;
      get diagnostics rowCt := ROW_COUNT;
      select cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from de_variant_subject_summary',rowCt,stepCt,'Done') into rtnCd;

      delete from deapp.de_variant_population_data where dataset_id = vcfDataSetId;
      stepCt := stepCt + 1;
      get diagnostics rowCt := ROW_COUNT;
      select cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from de_variant_population_data',rowCt,stepCt,'Done') into rtnCd;

      delete from deapp.de_variant_population_info where dataset_id = vcfDataSetId;
      stepCt := stepCt + 1;
      get diagnostics rowCt := ROW_COUNT;
      select cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from de_variant_population_info',rowCt,stepCt,'Done') into rtnCd;

      delete from deapp.de_variant_subject_detail where dataset_id = vcfDataSetId;
      stepCt := stepCt + 1;
      get diagnostics rowCt := ROW_COUNT;
      select cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from de_variant_subject_detail',rowCt,stepCt,'Done') into rtnCd;

      delete from deapp.de_variant_subject_idx where dataset_id = vcfDataSetId;
      stepCt := stepCt + 1;
      get diagnostics rowCt := ROW_COUNT;
      select cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from de_variant_subject_idx',rowCt,stepCt,'Done') into rtnCd;


      delete from deapp.de_variant_dataset where dataset_id = vcfDataSetId;
      stepCt := stepCt + 1;
      get diagnostics rowCt := ROW_COUNT;

      select cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from de_variant_dataset',rowCt,stepCt,'Done') into rtnCd;
    END LOOP;

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
			  where p.sourcesystem_cd like trialId || ':%');
		stepCt := stepCt + 1;
		get diagnostics rowCt := ROW_COUNT;
		select cz_write_audit(jobId,databaseName,procedureName,'Delete SECURITY data for trial from I2B2DEMODATA observation_fact',rowCt,stepCt,'Done') into rtnCd;
		/*commit;*/

		--	drop partition if it exists for expression data
    select i2b2_delete_partition(trialId, 'MRNA_AFFYMETRIX', 'de_subject_microarray_data', 'deapp', sourceCd, jobId) into rtnCd;

		delete from deapp.de_subject_microarray_data dssm
		where trial_name = trialId;

		stepCt := stepCt + 1;
		get diagnostics rowCt := ROW_COUNT;
		select cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from deapp de_subject_microarray_data',rowCt,stepCt,'Done') into rtnCd;
		/*commit;*/

    DELETE FROM i2b2demodata.visit_dimension
    WHERE patient_num IN
          (SELECT patient_num
           FROM i2b2demodata.patient_dimension
           WHERE sourcesystem_cd LIKE trialId || ':%');
    stepCt := stepCt + 1;
    get diagnostics rowCt := ROW_COUNT;
    select cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from I2B2DEMODATA visit_dimension',rowCt,stepCt,'Done') into rtnCd;
    
    DELETE FROM i2b2demodata.observation_fact
    WHERE patient_num IN
          (SELECT patient_num
           FROM i2b2demodata.patient_dimension
           WHERE sourcesystem_cd LIKE trialId || ':%');
    stepCt := stepCt + 1;
    get diagnostics rowCt := ROW_COUNT;
    select cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from I2B2DEMODATA observation_fact',rowCt,stepCt,'Done') into rtnCd;
		--	delete patient data

		delete from i2b2demodata.patient_dimension
		where sourcesystem_cd like trialId || ':%';
		stepCt := stepCt + 1;
		get diagnostics rowCt := ROW_COUNT;
		select cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from I2B2DEMODATA patient_dimension',rowCt,stepCt,'Done') into rtnCd;
		/*commit;*/

		delete from i2b2demodata.patient_trial
		where trial=  trialId;
		stepCt := stepCt + 1;
		get diagnostics rowCt := ROW_COUNT;
		select cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from I2B2DEMODATA patient_trial',rowCt,stepCt,'Done') into rtnCd;
		/*commit;*/

		-- delete protein data
		begin
      delete from deapp.de_subject_protein_data
      where trial_name = trialId;
      stepCt := stepCt + 1;
      get diagnostics rowCt := ROW_COUNT;
      select cz_write_audit(jobId,databaseName,procedureName,'Delete protein data for trial from DEAPP de_subject_protein_data',rowCt,stepCt,'Done') into rtnCd;

      exception when undefined_table then
         select cz_write_audit(jobId,databaseName,procedureName,'Table de_subject_protein_data is not defined.',0,stepCt,'Warning') into rtnCd;
		end;

		-- delete MIRNA data
		begin
      delete from deapp.de_subject_mirna_data
      where trial_name = trialId;
      stepCt := stepCt + 1;
      get diagnostics rowCt := ROW_COUNT;
      select cz_write_audit(jobId,databaseName,procedureName,'Delete MIRNA data for trial from DEAPP de_subject_mirna_data',rowCt,stepCt,'Done') into rtnCd;

      exception when undefined_table then
         select cz_write_audit(jobId,databaseName,procedureName,'Table de_subject_mirna_data is not defined.',0,stepCt,'Warning') into rtnCd;
    end;

		-- delete metabolomics data
		begin
      delete from deapp.de_subject_metabolomics_data
      where trial_name = trialId;
      stepCt := stepCt + 1;
      get diagnostics rowCt := ROW_COUNT;
      select cz_write_audit(jobId,databaseName,procedureName,'Delete metabolomics data for trial from DEAPP de_subject_metabolomics_data',rowCt,stepCt,'Done') into rtnCd;

      --	drop partition if it exists
      select i2b2_delete_partition(trialId, 'METABOLOMICS', 'de_subject_metabolomics_data', 'deapp', sourceCd, jobId) into rtnCd;

      exception when undefined_table then
         select cz_write_audit(jobId,databaseName,procedureName,'Table de_subject_metabolomics_data is not defined.',0,stepCt,'Warning') into rtnCd;
    end;

		-- delete RBM data
		begin
      delete from deapp.de_subject_rbm_data
      where trial_name = trialId;
      stepCt := stepCt + 1;
      get diagnostics rowCt := ROW_COUNT;
      select cz_write_audit(jobId,databaseName,procedureName,'Delete RBM data for trial from DEAPP de_subject_rbm_data',rowCt,stepCt,'Done') into rtnCd;

      --	drop partition if it exists
      select i2b2_delete_partition(trialId, 'RBM', 'de_subject_rbm_data', 'deapp', sourceCd, jobId) into rtnCd;

      exception when undefined_table then
         select cz_write_audit(jobId,databaseName,procedureName,'Table de_subject_rbm_data is not defined.',0,stepCt,'Warning') into rtnCd;
    end;

		-- delete RNASeq data
		begin
      delete from deapp.de_subject_rna_data
      where trial_name = trialId;

      stepCt := stepCt + 1;
      get diagnostics rowCt := ROW_COUNT;
      select cz_write_audit(jobId,databaseName,procedureName,'Delete RNASeq data for trial from DEAPP de_subject_rbm_data',rowCt,stepCt,'Done') into rtnCd;

      --	drop partition if it exists
      select i2b2_delete_partition(trialId, 'RNA_AFFYMETRIX', 'de_subject_rna_data', 'deapp', sourceCd, jobId) into rtnCd;

      exception when undefined_table then
          select cz_write_audit(jobId,databaseName,procedureName,'Table de_subject_rna_data is not defined.',0,stepCt,'Warning') into rtnCd;
    end;

    delete from deapp.de_subject_sample_mapping dssm
    where dssm.trial_name = trialID and coalesce(dssm.source_cd,'STD') = sourceCd;

		stepCt := stepCt + 1;
		get diagnostics rowCt := ROW_COUNT;
		select cz_write_audit(jobId,databaseName,procedureName,'Delete trial from DEAPP de_subject_sample_mapping',rowCt,stepCt,'Done') into rtnCd;

    delete from deapp.de_subject_snp_dataset
    where trial_name = trialId;
  end if;

	stepCt := stepCt + 1;
  get diagnostics rowCt := ROW_COUNT;
  select cz_write_audit(jobId,databaseName,procedureName,'Delete SNP data for trial from DE_SUBJECT_SNP_DATASET',rowCt,stepCt,'Done') into rtnCd;

  /* Delete aCGH data */
  delete from deapp.de_subject_acgh_data WHERE trial_name= TrialId;
  stepCt := stepCt + 1;
  get diagnostics rowCt := ROW_COUNT;
  select cz_write_audit(jobId,databaseName,procedureName,'Delete data from DE_SUBJECT_ACGH_DATA',rowCt,stepCt,'Done') into rtnCd;

  select count(*) into rowsExists from i2b2demodata.study where study_id = TrialId;
  IF rowsExists > 0
  THEN
    SELECT study_num
    INTO studyNum
    FROM i2b2demodata.study
    WHERE study_id = TrialId;
    DELETE FROM i2b2metadata.study_dimension_descriptions
    WHERE study_id = studyNum;

    stepCt := stepCt + 1;
    GET DIAGNOSTICS rowCt := ROW_COUNT;
    SELECT
      cz_write_audit(jobId, databaseName, procedureName, 'Delete data from study_dimension_descriptions', rowCt, stepCt,
                     'Done')
    INTO rtnCd;

    DELETE FROM i2b2demodata.trial_visit_dimension
    WHERE study_num = studyNum;
    
    stepCt := stepCt + 1;
    GET DIAGNOSTICS rowCt := ROW_COUNT;
    SELECT
      cz_write_audit(jobId, databaseName, procedureName, 'Delete data from trial_visit_dimension', rowCt, stepCt,
                     'Done')
    INTO rtnCd;
  END IF;
  DELETE FROM i2b2demodata.study
  WHERE study_id = trialid;
  stepCt := stepCt + 1;
  GET DIAGNOSTICS rowCt := ROW_COUNT;
  SELECT
    cz_write_audit(jobId, databaseName, procedureName, 'Delete data from study_dimension_descriptions', rowCt, stepCt,
                   'Done')
  INTO rtnCd;
  select cz_write_audit(jobId,databaseName,procedureName,'Delete study row from study table',rowCt,stepCt,'Done') into rtnCd;

  /*Check and delete top node, if removed node is last*/
    if (topNode is not null) then
      stepCt := stepCt + 1;
      select cz_write_audit(jobId,databaseName,procedureName,'Check and delete top node '||topNode||' if removed node is last',0,stepCt,'Done') into rtnCd;
      select count(*) into countNodeUnderTop
      from I2B2DEMODATA.concept_counts
      where parent_concept_path = topNode;

      stepCt := stepCt + 1;
	    get diagnostics rowCt := ROW_COUNT;
      select cz_write_audit(jobId,databaseName,procedureName,'Check if need to remove top node '||topNode,rowCt,stepCt,'Done') into rtnCd;

      if (countNodeUnderTop = 0)
      then
        select count(*) into isExistTopNode
             from I2B2METADATA.i2b2
            where c_fullname = topNode;

        if (isExistTopNode !=0 ) then
          select i2b2_delete_all_data(null, topNode, jobID) into rtnCd;
        end if;
      end if;
    end if;

  perform cz_end_audit (jobID, 'SUCCESS') where coalesce(currentJobId, -1) <> jobId;

  return 1;
END;
$BODY$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path FROM CURRENT
COST 100;
