create or replace
PROCEDURE "I2B2_DELETE_ALL_DATA"
(
  trial_id VARCHAR2 := null
 ,path_string varchar2 :=null
 ,currentJobID NUMBER := null
)
AS

--	JEA@20100106	New
--	JEA@20100112	Added removal of SECURITY records from observation_fact
  TYPE sourceCDs IS TABLE OF Varchar2(250);
  TYPE tVarCh2 IS TABLE OF Varchar2(700);
  bioexpid    NUMBER(18,0);
  TrialID 		varchar2(100);
  pathString  VARCHAR2(700 BYTE);
  tPathString  VARCHAR2(700 BYTE);
  TrialType 	VARCHAR2(250);
  sourceCD  	sourceCDs;
  pathStrings tVarCh2;

  --Audit variables
  newJobFlag INTEGER(1);
  trialCount INTEGER(1);
  pathCount INTEGER(1);
  countNodeUnderTop INTEGER(8);
  topNodeCount integer(8);
  isExistTopNode integer(1);
  countSourceCD integer(8);
  rowsExists  INT;
  topNode	VARCHAR(500 BYTE);
  databaseName VARCHAR(100);
  procedureName VARCHAR(100);
  jobID number(18,0);
  stepCt number(18,0);
  more_trial exception;
  more_path exception;
	res	number;

BEGIN
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

  if (path_string is not null) then
    select REGEXP_REPLACE('\' || path_string || '\','(\\){2,}', '\') into pathString from dual;
  end if;

  if (trial_id is null) then
	select count(distinct trial_name) into trialCount
		from DEAPP.de_subject_sample_mapping where concept_code in (
			select concept_cd from I2B2DEMODATA.concept_dimension where concept_path like path_string || '%'
		);
	if (trialCount = 1) then
		select distinct trial_name into TrialId
			from DEAPP.de_subject_sample_mapping where concept_code in (
				select concept_cd from I2B2DEMODATA.concept_dimension where concept_path like path_string || '%'
			);
	ELSIF ( trialCount = 0 ) THEN
		TrialId := null;
	else
		raise more_trial;
	end if;
  else
	TrialId := trial_id;
  end if;

  if (path_string is null) then
    select concept_path bulk collect into pathStrings
      from I2B2DEMODATA.concept_dimension where concept_cd in (
        select concept_code from DEAPP.de_subject_sample_mapping where trial_name = TrialId
      );

    pathCount := 0;
    FOR i IN pathStrings.FIRST..pathStrings.LAST LOOP
      select concept_path into tPathString
        from (
          select level, concept_path
            from i2b2demodata.concept_counts
            start with CONCEPT_PATH = pathStrings(i)
        connect by prior  PARENT_CONCEPT_PATH = CONCEPT_PATH
        order by level desc)
        where ROWNUM  = 1;

      if (tPathString = pathString) then
        pathCount := pathCount + 1;
        pathString := tPathString;
      else
        pathString := tPathString;
      end if;
    end loop;
    if (pathCount = 2) then
      raise more_path;
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
    topNode := SUBSTR(pathString,0,instr(pathString,'\',2));
  end if;


  stepCt := 0;

  if pathString != ''  or pathString != '%'
  then
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Starting I2B2_DELETE_ALL_DATA '||topNode,0,stepCt,'Done');

	--	delete all i2b2 nodes

	i2b2_delete_all_nodes(pathString,jobId);

  --	delete any table_access data
  delete from table_access
  where c_fullname like pathString || '%';

	--	delete any i2b2_tag data

	delete from i2b2_tags
	where path like pathString || '%';
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from I2B2METADATA i2b2_tags',SQL%ROWCOUNT,stepCt,'Done');
	commit;

	--	delete clinical data
	if (trialId is not NUll)
	then
		delete from lz_src_clinical_data
		where study_id = trialId;
		stepCt := stepCt + 1;
		cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from lz_src_clinical_data',SQL%ROWCOUNT,stepCt,'Done');
		commit;

    /*Deleting data from de_variant_subject_summary*/
    delete from deapp.de_variant_subject_summary v
      where assay_id in (select sm.assay_id
      from deapp.de_subject_sample_mapping sm
      where sm.trial_name = TrialID
        --and sm.sample_cd = v.subject_id
        and sm.platform='VCF'
        and v.dataset_id = sm.trial_name||':'||sm.source_cd
        );
    stepCt := stepCt + 1;
		cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from de_variant_subject_summary',SQL%ROWCOUNT,stepCt,'Done');
		commit;

		delete from deapp.de_variant_population_data where dataset_id in (
      select sm.trial_name||':'||sm.source_cd
      from deapp.de_subject_sample_mapping sm
      where sm.trial_name = TrialID
            and sm.platform='VCF'
    );
		stepCt := stepCt + 1;
		cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from de_variant_population_data',SQL%ROWCOUNT,stepCt,'Done');
		commit;

    delete from deapp.de_variant_population_info where dataset_id in (
      select sm.trial_name||':'||sm.source_cd
      from deapp.de_subject_sample_mapping sm
      where sm.trial_name = TrialID
            and sm.platform='VCF'
    );
    stepCt := stepCt + 1;
		cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from de_variant_population_info',SQL%ROWCOUNT,stepCt,'Done');
		commit;

    delete from deapp.de_variant_subject_detail where dataset_id in (
      select sm.trial_name||':'||sm.source_cd
      from deapp.de_subject_sample_mapping sm
      where sm.trial_name = TrialID
            and sm.platform='VCF'
    );
    stepCt := stepCt + 1;
		cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from de_variant_subject_detail',SQL%ROWCOUNT,stepCt,'Done');
		commit;

    delete from deapp.de_variant_subject_idx where dataset_id in (
      select sm.trial_name||':'||sm.source_cd
      from deapp.de_subject_sample_mapping sm
      where sm.trial_name = TrialID
            and sm.platform='VCF'
    );
    stepCt := stepCt + 1;
		cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from de_variant_subject_idx',SQL%ROWCOUNT,stepCt,'Done');
		commit;

    delete from deapp.de_variant_dataset where dataset_id in (
      select sm.trial_name||':'||sm.source_cd
      from deapp.de_subject_sample_mapping sm
      where sm.trial_name = TrialID
            and sm.platform='VCF'
    );
    stepCt := stepCt + 1;
		cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from de_variant_dataset',SQL%ROWCOUNT,stepCt,'Done');
		commit;

		--	delete observation_fact SECURITY data, do before patient_dimension delete
		select count(x.source_cd) into countSourceCD
			  from de_subject_sample_mapping x
			  where x.trial_name = trialId;

    if (countSourceCD>0) then
      select distinct x.source_cd bulk collect into sourceCD
          from de_subject_sample_mapping x
          where x.trial_name = trialId;

    FOR i IN sourceCD.FIRST..sourceCD.LAST LOOP
      delete from observation_fact f
      where f.concept_cd = 'SECURITY'
        and f.patient_num in
         (select distinct p.patient_num from patient_dimension p
          where p.sourcesystem_cd like trialId || ':%');
      stepCt := stepCt + 1;
      cz_write_audit(jobId,databaseName,procedureName,'Delete SECURITY data for trial from I2B2DEMODATA observation_fact',SQL%ROWCOUNT,stepCt,'Done');
      commit;

			i2b2_delete_lv_partition('DEAPP', 'DE_SUBJECT_MICROARRAY_DATA', 'TRIAL_SOURCE',
															 trialID || ':' || sourceCD(i), drop_partition=>1,
															 job_id=>jobId, ret_code=>res);

      stepCt := stepCt + 1;
      cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from deapp de_subject_microarray_data',SQL%ROWCOUNT,stepCt,'Done');
      commit;

      delete from deapp.de_subject_sample_mapping
			where trial_name = trialID and nvl(source_cd,'STD') = sourceCD(i);

      stepCt := stepCt + 1;
      cz_write_audit(jobId,databaseName,procedureName,'Delete trial from DEAPP de_subject_sample_mapping',SQL%ROWCOUNT,stepCt,'Done');

      commit;
      end loop;
    end if;
		--	delete patient data

		delete from patient_dimension
		where sourcesystem_cd like trialId || ':%';
		stepCt := stepCt + 1;
		cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from I2B2DEMODATA patient_dimension',SQL%ROWCOUNT,stepCt,'Done');
		commit;

		delete from patient_trial
		where trial = trialId;
		stepCt := stepCt + 1;
		cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from I2B2DEMODATA patient_trial',SQL%ROWCOUNT,stepCt,'Done');
		commit;

    -- delete protein data
    select count(*) into rowsExists from tab where tname='DE_SUBJECT_PROTEIN_DATA';
    if rowsExists > 0 then
      delete from deapp.de_subject_protein_data
      where trial_name = trialId;
      stepCt := stepCt + 1;
      cz_write_audit(jobId,databaseName,procedureName,'Delete protein data for trial from DEAPP de_subject_protein_data',SQL%ROWCOUNT,stepCt,'Done');
      commit;
    end if;

    -- delete MIRNA data
    select count(*) into rowsExists from tab where tname='DE_SUBJECT_MIRNA_DATA';
    if rowsExists > 0 then
      delete from deapp.de_subject_mirna_data
      where trial_name = trialId;
      stepCt := stepCt + 1;
      cz_write_audit(jobId,databaseName,procedureName,'Delete MIRNA data for trial from DEAPP de_subject_mirna_data',SQL%ROWCOUNT,stepCt,'Done');
      commit;
    end if;

    -- delete metabolomics data
    select count(*) into rowsExists from tab where tname='DE_SUBJECT_METABOLOMICS_DATA';
    if rowsExists > 0 then
      delete from deapp.de_subject_metabolomics_data
      where trial_name = trialId;
      stepCt := stepCt + 1;
      cz_write_audit(jobId,databaseName,procedureName,'Delete metabolomics data for trial from DEAPP de_subject_metabolomics_data',SQL%ROWCOUNT,stepCt,'Done');
      commit;
    end if;

    -- delete rbm data
    select count(*) into rowsExists from tab where tname='DE_SUBJECT_RBM_DATA';
    if rowsExists > 0 then
      delete from deapp.de_subject_rbm_data
      where trial_name = trialId;
      stepCt := stepCt + 1;
      cz_write_audit(jobId,databaseName,procedureName,'Delete RBM data for trial from DEAPP de_subject_rbm_data',SQL%ROWCOUNT,stepCt,'Done');
      commit;
    end if;

    -- delete rna data
    select count(*) into rowsExists from tab where tname='DE_SUBJECT_RNA_DATA';
    if rowsExists > 0 then
      delete from deapp.de_subject_rna_data
      where trial_name = trialId;
      stepCt := stepCt + 1;
      cz_write_audit(jobId,databaseName,procedureName,'Delete RNASeq data for trial from DEAPP de_subject_rbm_data',SQL%ROWCOUNT,stepCt,'Done');
      commit;
    end if;
	end if;

  delete from deapp.de_subject_snp_dataset
  where trial_name = trialId;
  stepCt := stepCt + 1;
  cz_write_audit(jobId,databaseName,procedureName,'Delete data for trial from DE_SUBJECT_SNP_DATASET',SQL%ROWCOUNT,stepCt,'Done');
  commit;

  select count(bio_experiment_id) into rowsExists from biomart.bio_experiment
  where accession = trialId;

  if rowsExists > 0 then
    select bio_experiment_id into bioexpid from biomart.bio_experiment
    where accession = trialId;

    delete from biomart.bio_experiment
    where accession = trialId;
    stepCt := stepCt + 1;
    cz_write_audit(jobId,databaseName,procedureName,'Delete data from BIO_EXPERIMENT',SQL%ROWCOUNT,stepCt,'Done');
    commit;

    delete from biomart.bio_data_uid where bio_data_id = bioexpid;
    stepCt := stepCt + 1;
    cz_write_audit(jobId,databaseName,procedureName,'Delete data from BIO_DATA_UID',SQL%ROWCOUNT,stepCt,'Done');
    commit;

    select count(*) into rowsExists from searchapp.search_secure_object where bio_data_id = bioexpid;
    if rowsExists > 0 then
      delete from searchapp.search_secure_object where bio_data_id = bioexpid;
      stepCt := stepCt + 1;
      get diagnostics rowCt := ROW_COUNT;
      cz_write_audit(jobId,databaseName,procedureName,'Delete data from SEARCH_SECURE_OBJECT',SQL%ROWCOUNT,stepCt,'Done');
    end if;
  end if;

	/*Check and delete top node, if remove node is last*/
  stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Check and delete top node '||topNode||', if remove node is last',SQL%ROWCOUNT,stepCt,'Done');
	commit;

  select count(*) into countNodeUnderTop
    from I2B2DEMODATA.concept_counts
    where parent_concept_path = topNode;
  stepCt := stepCt + 1;
  cz_write_audit(jobId,databaseName,procedureName,'Check need removed top node '||topNode,SQL%ROWCOUNT,stepCt,'Done');
  commit;

  if (countNodeUnderTop = 0)
  then
    select count(*) into isExistTopNode
     from I2B2METADATA.i2b2
    where c_fullname = topNode;

    if (isExistTopNode !=0 ) then
      i2b2_delete_all_data(null, topNode, jobID);
    end if;

  end if;

  end if;

    ---Cleanup OVERALL JOB if this proc is being run standalone
  IF newJobFlag = 1
  THEN
    cz_end_audit (jobID, 'SUCCESS');
  END IF;

  EXCEPTION
  WHEN more_trial then
	cz_write_audit(jobId,databasename,procedurename,'Please select right path to study',1,stepCt,'ERROR');
	cz_error_handler(jobid,procedurename);
	cz_end_audit (jobId,'FAIL');
  WHEN more_path then
	cz_write_audit(jobId,databasename,procedurename,'Please select right trial to study',1,stepCt,'ERROR');
	cz_error_handler(jobid,procedurename);
	cz_end_audit (jobId,'FAIL');

  WHEN OTHERS THEN
    --Handle errors.
    cz_error_handler (jobID, procedureName);
    --End Proc
    cz_end_audit (jobID, 'FAIL');

END;