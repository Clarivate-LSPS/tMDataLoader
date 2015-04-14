--
-- Type: PROCEDURE; Owner: TM_CZ; Name: I2B2_PROCESS_RNA_SEQ_DATA
--
  CREATE OR REPLACE PROCEDURE "I2B2_PROCESS_RNA_SEQ_DATA"
(
  trial_id 		VARCHAR2
 ,top_node		varchar2
 ,data_type		varchar2 := 'R'		--	R = raw data, do zscore calc, T = transformed data, load raw values as zscore,
									--	L = log intensity data, skip log step in zscore calc
 ,source_cd		varchar2 := 'STD'	--	default source_cd = 'STD'
 ,log_base		number := 2			--	log base value for conversion back to raw
 ,secure_study	varchar2			--	security setting if new patients added to patient_dimension
 ,currentJobID 	NUMBER := null
 ,rtn_code		OUT	NUMBER
)
AS
/*************************************************************************
* This stored procedure is for ETL to load RNA sequencing
* Date:10/23/2013
******************************************************************/
--	***  NOTE ***
--	The input file columns are mapped to the following table columns.  This is done so that the javascript for the advanced workflows
--	selects the correct data for the dropdowns.

--		tissue_type	=>	sample_type
--		attribute_1	=>	tissue_type
--		atrribute_2	=>	timepoint

  TrialID		varchar2(100);
  RootNode		VARCHAR2(2000);
  root_level	integer;
  topNode		varchar2(2000);
  topLevel		integer;
  tPath			varchar2(2000);
  study_name	varchar2(100);
  sourceCd		varchar2(50);
  secureStudy	varchar2(1);

  dataType		varchar2(10);
  sqlText		varchar2(1000);
  tText			varchar2(1000);
  gplTitle		varchar2(1000);
  pExists		number;
  partTbl   	number;
  partExists 	number;
  sampleCt		number;
  idxExists 	number;
  logBase		number;
  pCount		integer;
  sCount		integer;
  tablespaceName	varchar2(200);
  v_bio_experiment_id	number(18,0);

    --Audit variables
  newJobFlag INTEGER(1);
  databaseName VARCHAR(100);
  procedureName VARCHAR(100);
  jobID number(18,0);
  stepCt number(18,0);

  --unmapped_patients exception;
  missing_platform	exception;
  missing_tissue	EXCEPTION;
  unmapped_platform exception;
  multiple_platform	exception;
  no_probeset_recs	exception;



	CURSOR addNodes is
	select distinct t.leaf_node
          ,t.node_name
	from  wt_RNA_SEQ_nodes t
	where not exists
		 (select 1 from i2b2 x
		  where t.leaf_node = x.c_fullname);


--	cursor to define the path for delete_one_node  this will delete any nodes that are hidden after i2b2_create_concept_counts

  CURSOR delNodes is
  select distinct c_fullname
  from  i2b2
  where c_fullname like topNode || '%'
    and substr(c_visualattributes,2,1) = 'H';
    --and c_visualattributes like '_H_';


        cursor uploadI2b2 is
    select category_cd,display_value,display_label,display_unit from
    tm_lz.lt_src_rna_display_mapping;


BEGIN
EXECUTE IMMEDIATE 'alter session set NLS_NUMERIC_CHARACTERS=".,"';
	TrialID := upper(trial_id);
	secureStudy := upper(secure_study);

	if (secureStudy not in ('Y','N') ) then
		secureStudy := 'Y';
	end if;

	topNode := REGEXP_REPLACE('\' || top_node || '\','(\\){2,}', '\');
	select length(topNode)-length(replace(topNode,'\','')) into topLevel from dual;

	if data_type is null then
		dataType := 'R';
	else
		if data_type in ('R','T','L') then
			dataType := data_type;
		else
			dataType := 'R';
		end if;
	end if;

	logBase := log_base;
	sourceCd := upper(nvl(source_cd,'STD'));

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
	cz_write_audit(jobId,databaseName,procedureName,'Starting i2b2_process_RNA_SEQ_data',0,stepCt,'Done');

	--	Get count of records in lt_src_RNA_SEQ_subj_samp_map

	select count(*) into sCount
	from lt_src_RNA_SEQ_subj_samp_map;

	--	check if all subject_sample map records have a platform, If not, abort run

	select count(*) into pCount
	from lt_src_RNA_SEQ_subj_samp_map
	where platform is null;

	if pCount > 0 then
		raise missing_platform;
	end if;




	/*select count(*) into pCount
	from DE_gpl_info
	where platform in (select distinct m.platform from lt_src_RNA_SEQ_subj_samp_map m);
	*/
	/*if PCOUNT = 0 then
		RAISE UNMAPPED_platform;
	end if;*/

	--	check if all subject_sample map records have a tissue_type, If not, abort run

	select count(*) into pCount
	from lt_src_RNA_SEQ_subj_samp_map
	where tissue_type is null;

	if pCount > 0 then
		raise missing_tissue;
	end if;

	--	check if there are multiple platforms, if yes, then platform must be supplied in lt_src_RNA_SEQ_data

	select count(*) into pCount
	from (select sample_cd
		  from lt_src_RNA_SEQ_subj_samp_map
		  group by sample_cd
		  having count(distinct platform) > 1);

	if pCount > 0 then
		raise multiple_platform;
	end if;

	-- Get root_node from topNode

	select parse_nth_value(topNode, 2, '\') into RootNode from dual;

	select count(*) into pExists
	from table_access
	where c_name = rootNode;

	if pExists = 0 then
		i2b2_add_root_node(rootNode, jobId);
	end if;

	select c_hlevel into root_level
	from i2b2
	where c_name = RootNode;

	-- Get study name from topNode

	select parse_nth_value(topNode, topLevel, '\') into study_name from dual;

	--	Add any upper level nodes as needed

	tPath := REGEXP_REPLACE(replace(top_node,study_name,null),'(\\){2,}', '\');
	select length(tPath) - length(replace(tPath,'\',null)) into pCount from dual;

	if pCount > 2 then
		i2b2_fill_in_tree(null, tPath, jobId);
	end if;

	--	uppercase study_id in lt_src_RNA_SEQ_subj_samp_map in case curator forgot

	update lt_src_RNA_SEQ_subj_samp_map
	set trial_name=upper(trial_name);

	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Uppercase trial_name in lt_src_RNA_SEQ_subj_samp_map',SQL%ROWCOUNT,stepCt,'Done');
	commit;

	--	create records in patient_dimension for subject_ids if they do not exist
	--	format of sourcesystem_cd:  trial:[site:]subject_cd

	insert into patient_dimension
    ( patient_num,
      sex_cd,
      age_in_years_num,
      race_cd,
      update_date,
      download_date,
      import_date,
      sourcesystem_cd
    )
    select seq_patient_num.nextval
		  ,x.sex_cd
		  ,x.age_in_years_num
		  ,x.race_cd
		  ,sysdate
		  ,sysdate
		  ,sysdate
		  ,x.sourcesystem_cd
	from (select distinct 'Unknown' as sex_cd,
				 0 as age_in_years_num,
				 null as race_cd,
				 regexp_replace(TrialID || ':' || s.site_id || ':' || s.subject_id,'(::){1,}', ':') as sourcesystem_cd
		 from lt_src_RNA_SEQ_subj_samp_map s
		    -- ,de_gpl_info g
		 where s.subject_id is not null
		   and s.trial_name = TrialID
		   and s.source_cd = sourceCD
		 --  and s.platform = g.platform
		   --and upper(g.marker_type) = 'GENE EXPRESSION'
		   and not exists
			  (select 1 from patient_dimension x
			   where x.sourcesystem_cd =
				 regexp_replace(TrialID || ':' || s.site_id || ':' || s.subject_id,'(::){1,}', ':'))
		) x;

	pCount := SQL%ROWCOUNT;

	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Insert subjects to patient_dimension',pCount,stepCt,'Done');
	commit;

	i2b2_create_security_for_trial(TrialId, secureStudy, jobID);

	--	Delete existing observation_fact data, will be repopulated

	delete from observation_fact obf
	where obf.concept_cd in
		 (select distinct x.concept_code
		  from de_subject_sample_mapping x
		  where x.trial_name = TrialId
		    and nvl(x.source_cd,'STD') = sourceCD
		    and x.platform = 'RNA_SEQ');

	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Delete data from observation_fact',SQL%ROWCOUNT,stepCt,'Done');
	commit;

	select count(*) into pExists
	from all_tables
	where table_name = 'DE_SUBJECT_RNA_DATA'
	  and partitioned = 'YES';

	if pExists = 0 then
		--	dataset is not partitioned so must delete

		delete from de_subject_rna_data
		where trial_source = TrialId || ':' || sourceCd;
		stepCt := stepCt + 1;
		cz_write_audit(jobId,databaseName,procedureName,'Delete data from de_subject_rna_data',SQL%ROWCOUNT,stepCt,'Done');
		commit;
	else
		--	Create partition in de_subject_RNA_data if it doesn't exist else truncate partition

		select count(*)
			into pExists
			from all_tab_partitions
			where table_name = 'DE_SUBJECT_RNA_DATA'
			  and partition_name = TrialId || ':' || sourceCd;
			--10/30/2013  //modified
		if pExists = 0 then

			--	needed to add partition to de_subject_RNA_data

			sqlText := 'alter table deapp.de_subject_rna_data add PARTITION "' || TrialID || ':' || sourceCd || '"  VALUES (' || '''' || TrialID || ':' || sourceCd || '''' || ') ' ||
						   'NOLOGGING COMPRESS TABLESPACE "TRANSMART" ';
			execute immediate(sqlText);
			stepCt := stepCt + 1;
			cz_write_audit(jobId,databaseName,procedureName,'Adding partition to de_subject_rna_data',0,stepCt,'Done');

		else
			sqlText := 'alter table deapp.de_subject_rna_data truncate partition "' || TrialID || ':' || sourceCd || '"';
			execute immediate(sqlText);
			stepCt := stepCt + 1;
			cz_write_audit(jobId,databaseName,procedureName,'Truncating partition in de_subject_rna_data',0,stepCt,'Done');
		end if;

	end if;

	--	Cleanup any existing data in de_subject_sample_mapping.

	delete from DE_SUBJECT_SAMPLE_MAPPING
	where trial_name = TrialID
	  and nvl(source_cd,'STD') = sourceCd
	  and platform = 'RNA_SEQ'
	 ; --Making sure only RNA_sequencing data is deleted

	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Delete trial from DEAPP de_subject_sample_mapping',SQL%ROWCOUNT,stepCt,'Done');

	commit;

--	truncate tmp node table

	execute immediate('truncate table tm_wz.wt_RNA_SEQ_nodes');

--	load temp table with leaf node path, use temp table with distinct sample_type, ATTR2, platform, and title   this was faster than doing subselect
--	from wt_subject_RNA_sequencing_data

	execute immediate('truncate table tm_wz.wt_RNA_SEQ_node_values');

	insert into wt_RNA_SEQ_node_values
	(category_cd
	,platform
	,tissue_type
	,attribute_1
	,attribute_2
	,title
	)
	select distinct a.category_cd
				   ,nvl(a.platform,'GPL570')
				   ,nvl(a.tissue_type,'Unspecified Tissue Type')
	               ,a.attribute_1
				   ,a.attribute_2
				   ,a.platform--g.title
    from lt_src_RNA_SEQ_subj_samp_map a
	  --  ,de_gpl_info g
	where a.trial_name = TrialID
	--  and nvl(a.platform,'GPL570') = g.platform
	  and a.source_cd = sourceCD
	 -- and a.platform = g.platform
	--  and upper(g.marker_type) = 'GENE EXPRESSION'
	--  and g.title = (select min(x.title) from de_gpl_info x where nvl(a.platform,'GPL570') = x.platform)
      -- and upper(g.organism) = 'HOMO SAPIENS'
	  ;
	--  and decode(dataType,'R',sign(a.intensity_value),1) = 1;	--	take all values when dataType T, only >0 for dataType R
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Insert node values into DEAPP wt_RNA_SEQ_node_values',SQL%ROWCOUNT,stepCt,'Done');
	commit;

	insert into wt_RNA_SEQ_nodes
	(leaf_node
	,category_cd
	,platform
	,tissue_type
	,attribute_1
    ,attribute_2
	,node_type
	)
	select distinct topNode || regexp_replace(replace(replace(replace(replace(replace(replace(
	       category_cd,'PLATFORM',title),'ATTR1',attribute_1),'ATTR2',attribute_2),'TISSUETYPE',tissue_type),'+','\'),'_',' ') || '\','(\\){2,}', '\')
		  ,category_cd
		  ,platform as platform
		  ,tissue_type
		  ,attribute_1 as attribute_1
          ,attribute_2 as attribute_2
		  ,'LEAF'
	from  wt_RNA_SEQ_node_values;

    stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Create leaf nodes in DEAPP tmp_RNA_SEQ_nodes',SQL%ROWCOUNT,stepCt,'Done');
	commit;

	--	insert for platform node so platform concept can be populated

	insert into wt_RNA_SEQ_nodes
	(leaf_node
	,category_cd
	,platform
	,tissue_type
	,attribute_1
    ,attribute_2
	,node_type
	)
	select distinct topNode || regexp_replace(replace(replace(replace(replace(replace(replace(
	       substr(category_cd,1,instr(category_cd,'PLATFORM')+8),'PLATFORM',title),'ATTR1',attribute_1),'ATTR2',attribute_2),'TISSUETYPE',tissue_type),'+','\'),'_',' ') || '\',
		   '(\\){2,}', '\')
		  ,substr(category_cd,1,instr(category_cd,'PLATFORM')+8)
		  ,platform as platform
		  ,case when instr(substr(category_cd,1,instr(category_cd,'PLATFORM')+8),'TISSUETYPE') > 1 then tissue_type else null end as tissue_type
		  ,case when instr(substr(category_cd,1,instr(category_cd,'PLATFORM')+8),'ATTR1') > 1 then attribute_1 else null end as attribute_1
          ,case when instr(substr(category_cd,1,instr(category_cd,'PLATFORM')+8),'ATTR2') > 1 then attribute_2 else null end as attribute_2
		  ,'PLATFORM'
	from  wt_RNA_SEQ_node_values;

    stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Create platform nodes in wt_RNA_SEQ_nodes',SQL%ROWCOUNT,stepCt,'Done');
	commit;

	--	insert for ATTR1 node so ATTR1 concept can be populated in tissue_type_cd

	insert into wt_RNA_SEQ_nodes
	(leaf_node
	,category_cd
	,platform
	,tissue_type
    ,attribute_1
	,attribute_2
	,node_type
	)
	select distinct topNode || regexp_replace(replace(replace(replace(replace(replace(replace(
	       substr(category_cd,1,instr(category_cd,'ATTR1')+5),'PLATFORM',title),'ATTR1',attribute_1),'ATTR2',attribute_2),'TISSUETYPE',tissue_type),'+','\'),'_',' ') || '\',
		   '(\\){2,}', '\')
		  ,substr(category_cd,1,instr(category_cd,'ATTR1')+5)
		  ,case when instr(substr(category_cd,1,instr(category_cd,'ATTR1')+5),'PLATFORM') > 1 then platform else null end as platform
		  ,case when instr(substr(category_cd,1,instr(category_cd,'ATTR1')+5),'TISSUETYPE') > 1 then tissue_type else null end as tissue_type
		  ,attribute_1 as attribute_1
          ,case when instr(substr(category_cd,1,instr(category_cd,'ATTR1')+5),'ATTR2') > 1 then attribute_2 else null end as attribute_2
		  ,'ATTR1'
	from  wt_RNA_SEQ_node_values
	where category_cd like '%ATTR1%'
	  and attribute_1 is not null;

    stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Create ATTR1 nodes in wt_RNA_SEQ_nodes',SQL%ROWCOUNT,stepCt,'Done');
	commit;

	--	insert for ATTR2 node so ATTR2 concept can be populated in timepoint_cd

	insert into wt_RNA_SEQ_nodes
	(leaf_node
	,category_cd
	,platform
	,tissue_type
    ,attribute_1
	,attribute_2
	,node_type
	)
	select distinct topNode || regexp_replace(replace(replace(replace(replace(replace(replace(
	       substr(category_cd,1,instr(category_cd,'ATTR2')+5),'PLATFORM',title),'ATTR1',attribute_1),'ATTR2',attribute_2),'TISSUETYPE',tissue_type),'+','\'),'_',' ') || '\',
		   '(\\){2,}', '\')
		  ,substr(category_cd,1,instr(category_cd,'ATTR2')+5)
		  ,case when instr(substr(category_cd,1,instr(category_cd,'ATTR2')+5),'PLATFORM') > 1 then platform else null end as platform
		  ,case when instr(substr(category_cd,1,instr(category_cd,'ATTR1')+5),'TISSUETYPE') > 1 then tissue_type else null end as tissue_type
          ,case when instr(substr(category_cd,1,instr(category_cd,'ATTR2')+5),'ATTR1') > 1 then attribute_1 else null end as attribute_1
		  ,attribute_2 as attribute_2
		  ,'ATTR2'
	from  wt_RNA_SEQ_node_values
	where category_cd like '%ATTR2%'
	  and attribute_2 is not null;

    stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Create ATTR2 nodes in wt_RNA_SEQ_nodes',SQL%ROWCOUNT,stepCt,'Done');
	commit;

	--	insert for tissue_type node so sample_type_cd can be populated

	insert into wt_RNA_SEQ_nodes
	(leaf_node
	,category_cd
	,platform
	,tissue_type
	,attribute_1
    ,attribute_2
	,node_type
	)
	select distinct topNode || regexp_replace(replace(replace(replace(replace(replace(replace(
	       substr(category_cd,1,instr(category_cd,'TISSUETYPE')+10),'PLATFORM',title),'ATTR1',attribute_1),'ATTR2',attribute_2),'TISSUETYPE',tissue_type),'+','\'),'_',' ') || '\',
		   '(\\){2,}', '\')
		  ,substr(category_cd,1,instr(category_cd,'TISSUETYPE')+10)
		  ,case when instr(substr(category_cd,1,instr(category_cd,'TISSUETYPE')+10),'PLATFORM') > 1 then platform else null end as platform
		  ,tissue_type as tissue_type
		  ,case when instr(substr(category_cd,1,instr(category_cd,'TISSUETYPE')+10),'ATTR1') > 1 then attribute_1 else null end as attribute_1
          ,case when instr(substr(category_cd,1,instr(category_cd,'TISSUETYPE')+10),'ATTR2') > 1 then attribute_2 else null end as attribute_2
		  ,'TISSUETYPE'
	from  wt_RNA_SEQ_node_values
	where category_cd like '%TISSUETYPE%';

    stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Create ATTR2 nodes in wt_RNA_SEQ_nodes',SQL%ROWCOUNT,stepCt,'Done');
	commit;

	update wt_RNA_SEQ_nodes
	set node_name=parse_nth_value(leaf_node,length(leaf_node)-length(replace(leaf_node,'\',null)),'\');

    stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Updated node_name in DEAPP tmp_RNA_SEQ_nodes',SQL%ROWCOUNT,stepCt,'Done');
	commit;

--	add leaf nodes for RNA_sequencing data  The cursor will only add nodes that do not already exist.

	 FOR r_addNodes in addNodes Loop

    --Add nodes for all types (ALSO DELETES EXISTING NODE)

		i2b2_add_node(TrialID, r_addNodes.leaf_node, r_addNodes.node_name, jobId);
		stepCt := stepCt + 1;
		tText := 'Added Leaf Node: ' || r_addNodes.leaf_node || '  Name: ' || r_addNodes.node_name;

		cz_write_audit(jobId,databaseName,procedureName,tText,SQL%ROWCOUNT,stepCt,'Done');

		i2b2_fill_in_tree(TrialId, r_addNodes.leaf_node, jobID);

	END LOOP;

--	update concept_cd for nodes, this is done to make the next insert easier

	update wt_RNA_SEQ_nodes t
	set concept_cd=(select c.concept_cd from concept_dimension c
	                where c.concept_path = t.leaf_node
				   )
    where exists
         (select 1 from concept_dimension x
	                where x.concept_path = t.leaf_node
				   )
	  and t.concept_cd is null;

	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Update wt_RNA_SEQ_nodes with newly created concept_cds',SQL%ROWCOUNT,stepCt,'Done');
	commit;

  --Load the DE_SUBJECT_SAMPLE_MAPPING from wt_subject_RNA_sequencing_data

  --PATIENT_ID      = PATIENT_ID (SAME AS ID ON THE PATIENT_DIMENSION)
  --SITE_ID         = site_id
  --SUBJECT_ID      = subject_id
  --SUBJECT_TYPE    = NULL
  --CONCEPT_CODE    = from LEAF records in wt_RNA_SEQ_nodes
  --SAMPLE_TYPE    	= TISSUE_TYPE
  --SAMPLE_TYPE_CD  = concept_cd from TISSUETYPE records in wt_RNA_SEQ_nodes
  --TRIAL_NAME      = TRIAL_NAME
  --TIMEPOINT		= attribute_2
  --TIMEPOINT_CD	= concept_cd from ATTR2 records in wt_RNA_SEQ_nodes
  --TISSUE_TYPE     = attribute_1
  --TISSUE_TYPE_CD  = concept_cd from ATTR1 records in wt_RNA_SEQ_nodes
  --PLATFORM        = RNA_sequencing_AFFYMETRIX - this is required by ui code
  --PLATFORM_CD     = concept_cd from PLATFORM records in wt_RNA_SEQ_nodes
  --DATA_UID		= concatenation of concept_cd-patient_num
  --GPL_ID			= platform from wt_subject_RNA_SEQ_data
  --CATEGORY_CD		= category_cd that generated ontology
  --SAMPLE_ID		= id of sample (trial:S:[site_id]:subject_id:sample_cd) from patient_dimension, may be the same as patient_num
  --SAMPLE_CD		= sample_cd
  --SOURCE_CD		= sourceCd

  --ASSAY_ID        = generated by trigger

	insert into de_subject_sample_mapping
	(patient_id
	,site_id
	,subject_id
	,subject_type
	,concept_code
	,assay_id
	,sample_type
	,sample_type_cd
	,trial_name
	,timepoint
	,timepoint_cd
	,tissue_type
	,tissue_type_cd
	,platform
	,platform_cd
	,data_uid
	,gpl_id
	,sample_id
	,sample_cd
	,category_cd
	,source_cd
	,omic_source_study
	,omic_patient_id
    )
	select t.patient_id
		  ,t.site_id
		  ,t.subject_id
		  ,t.subject_type
		  ,t.concept_code
		  ,deapp.seq_assay_id.nextval
		  ,t.sample_type
		  ,t.sample_type_cd
		  ,t.trial_name
		  ,t.timepoint
		  ,t.timepoint_cd
		  ,t.tissue_type
		  ,t.tissue_type_cd
		  ,t.platform
		  ,t.platform_cd
		  ,t.data_uid
		  ,t.gpl_id
		  ,t.sample_id
		  ,t.sample_cd
		  ,t.category_cd
		  ,t.source_cd
		  ,t.omic_source_study
		  ,t.omic_patient_id
	from (select distinct b.patient_num as patient_id
			  ,a.site_id
			  ,a.subject_id
			  ,null as subject_type
			  ,ln.concept_cd as concept_code
			  ,a.tissue_type as sample_type
			  ,ttp.concept_cd as sample_type_cd
			  ,a.trial_name
			  ,a.attribute_2 as timepoint
			  ,a2.concept_cd as timepoint_cd
			  ,a.attribute_1 as tissue_type
			  ,a1.concept_cd as tissue_type_cd
			  ,'RNA_SEQ' as platform
			  ,pn.concept_cd as platform_cd
			  ,ln.concept_cd || '-' || to_char(b.patient_num) as data_uid
			  ,a.platform as gpl_id
			  ,coalesce(sid.patient_num,b.patient_num) as sample_id
			  ,a.sample_cd
			  ,nvl(a.category_cd,'Biomarker_Data+RNA_SEQ+PLATFORM+TISSUETYPE+ATTR1+ATTR2') as category_cd
			  ,a.source_cd
			  ,TrialId as omic_source_study
			  ,b.patient_num as omic_patient_id
		from lt_src_RNA_SEQ_subj_samp_map a
		--Joining to Pat_dim to ensure the ID's match. If not I2B2 won't work.
		inner join patient_dimension b
		  on regexp_replace(TrialID || ':' || a.site_id || ':' || a.subject_id,'(::){1,}', ':') = b.sourcesystem_cd
		inner join wt_RNA_SEQ_nodes ln
			on a.platform = ln.platform
			and a.category_cd=ln.category_cd
			and a.tissue_type = ln.tissue_type
			and nvl(a.attribute_1,'@') = nvl(ln.attribute_1,'@')
			and nvl(a.attribute_2,'@') = nvl(ln.attribute_2,'@')
			and ln.node_type = 'LEAF'
		inner join wt_RNA_SEQ_nodes pn
			on a.platform = pn.platform
			and  pn.category_cd=substr(a.category_cd,1,instr(a.category_cd,'PLATFORM')+8)
			and case when instr(substr(a.category_cd,1,instr(a.category_cd,'PLATFORM')+8),'TISSUETYPE') > 1 then a.tissue_type else '@' end = nvl(pn.tissue_type,'@')
			and case when instr(substr(a.category_cd,1,instr(a.category_cd,'PLATFORM')+8),'ATTR1') > 1 then a.attribute_1 else '@' end = nvl(pn.attribute_1,'@')
			and case when instr(substr(a.category_cd,1,instr(a.category_cd,'PLATFORM')+8),'ATTR2') > 1 then a.attribute_2 else '@' end = nvl(pn.attribute_2,'@')
			and pn.node_type = 'PLATFORM'
		left outer join wt_RNA_SEQ_nodes ttp
			on a.tissue_type = ttp.tissue_type
			and ttp.category_cd=substr(a.category_cd,1,instr(a.category_cd,'TISSUETYPE')+10)
			and case when instr(substr(a.category_cd,1,instr(a.category_cd,'TISSUETYPE')+10),'PLATFORM') > 1 then a.platform else '@' end = nvl(ttp.platform,'@')
			and case when instr(substr(a.category_cd,1,instr(a.category_cd,'TISSUETYPE')+10),'ATTR1') > 1 then a.attribute_1 else '@' end = nvl(ttp.attribute_1,'@')
			and case when instr(substr(a.category_cd,1,instr(a.category_cd,'TISSUETYPE')+10),'ATTR2') > 1 then a.attribute_2 else '@' end = nvl(ttp.attribute_2,'@')
			and ttp.node_type = 'TISSUETYPE'
		left outer join wt_RNA_SEQ_nodes a1
			on a.attribute_1 = a1.attribute_1
			and a1.category_cd=substr(a.category_cd,1,instr(a.category_cd,'ATTR1')+5)
			and case when instr(substr(a.category_cd,1,instr(a.category_cd,'ATTR1')+5),'PLATFORM') > 1 then a.platform else '@' end = nvl(a1.platform,'@')
			and case when instr(substr(a.category_cd,1,instr(a.category_cd,'ATTR1')+5),'TISSUETYPE') > 1 then a.tissue_type else '@' end = nvl(a1.tissue_type,'@')
			and case when instr(substr(a.category_cd,1,instr(a.category_cd,'ATTR1')+5),'ATTR2') > 1 then a.attribute_2 else '@' end = nvl(a1.attribute_2,'@')
			and a1.node_type = 'ATTR1'
		left outer join wt_RNA_SEQ_nodes a2
			on a.attribute_2 = a1.attribute_2
			and a2.category_cd=substr(a.category_cd,1,instr(a.category_cd,'ATTR2')+5)
			and case when instr(substr(a.category_cd,1,instr(a.category_cd,'ATTR2')+5),'PLATFORM') > 1 then a.platform else '@' end = nvl(a2.platform,'@')
			and case when instr(substr(a.category_cd,1,instr(a.category_cd,'ATTR2')+5),'TISSUETYPE') > 1 then a.tissue_type else '@' end = nvl(a2.tissue_type,'@')
			and case when instr(substr(a.category_cd,1,instr(a.category_cd,'ATTR2')+5),'ATTR1') > 1 then a.attribute_1 else '@' end = nvl(a2.attribute_1,'@')
			and a2.node_type = 'ATTR2'
		left outer join patient_dimension sid
			on  regexp_replace(TrialId || ':S:' || a.site_id || ':' || a.subject_id || ':' || a.sample_cd,
							  '(::){1,}', ':') = sid.sourcesystem_cd
		where a.trial_name = TrialID
		  and a.source_cd = sourceCD
		  and  ln.concept_cd is not null) t;

	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Insert trial into DEAPP de_subject_sample_mapping',SQL%ROWCOUNT,stepCt,'Done');

  commit;
--	recreate de_subject_sample_mapping indexes

	--execute immediate('create index de_subject_smpl_mpng_idx1 on de_subject_sample_mapping(timepoint, patient_id, trial_name) parallel nologging');
	--execute immediate('create index de_subject_smpl_mpng_idx2 on de_subject_sample_mapping(patient_id, timepoint_cd, platform_cd, assay_id, trial_name) parallel nologging');
	--execute immediate('create bitmap index de_subject_smpl_mpng_idx3 on de_subject_sample_mapping(sample_type_cd) parallel nologging');
	--execute immediate('create index de_subject_smpl_mpng_idx4 on de_subject_sample_mapping(gpl_id) parallel nologging');
	--execute immediate('create index de_subject_smpl_mpng_idx4 on de_subject_sample_mapping(platform, gpl_id) parallel nologging');
    --stepCt := stepCt + 1;
	--cz_write_audit(jobId,databaseName,procedureName,'Recreate indexes on DEAPP de_subject_sample_mapping',0,stepCt,'Done');

--	Insert records for patients and samples into observation_fact

	insert into observation_fact
    (patient_num
	,concept_cd
	,modifier_cd
	,valtype_cd
	,tval_char
	,nval_num
	,sourcesystem_cd
	,import_date
	,valueflag_cd
	,provider_id
	,location_cd
	,units_cd
        ,sample_cd
        ,INSTANCE_NUM
    )
    select distinct m.patient_id
		  ,m.concept_code
		  ,'@'
		  ,'T' -- Text data type
		  ,'E'  --Stands for Equals for Text Types
		  ,null	--	not numeric for RNA_sequencing
		  ,m.trial_name
		  ,sysdate
		  ,'@'
		  ,'@'
		  ,'@'
		  ,'' -- no units available
                  ,m.sample_cd
                  ,1
    from  de_subject_sample_mapping m
    where m.trial_name = TrialID
	  and m.source_cd = sourceCD
      and m.platform = 'RNA_SEQ';

    stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Insert patient facts into I2B2DEMODATA observation_fact',SQL%ROWCOUNT,stepCt,'Done');

    commit;

	--	Insert sample facts

	insert into observation_fact
    (patient_num
	,concept_cd
	,modifier_cd
	,valtype_cd
	,tval_char
	,nval_num
	,sourcesystem_cd
	,import_date
	,valueflag_cd
	,provider_id
	,location_cd
	,units_cd
        ,sample_cd
        ,INSTANCE_NUM
    )
    select distinct m.sample_id
		  ,m.concept_code
		  ,m.trial_name
		  ,'T' -- Text data type
		  ,'E'  --Stands for Equals for Text Types
		  ,null	--	not numeric for RNA_sequencing
		  ,m.trial_name
		  ,sysdate
		  ,'@'
		  ,'@'
		  ,'@'
		  ,'' -- no units available
                  ,m.sample_cd
                  ,1
    from  de_subject_sample_mapping m
    where m.trial_name = TrialID
	  and m.source_cd = sourceCd
      and m.platform = 'RNA_SEQ'
	  and m.patient_id != m.sample_id;

    stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Insert sample facts into I2B2DEMODATA observation_fact',SQL%ROWCOUNT,stepCt,'Done');

    commit;

	--Update I2b2 for correct data type

	update i2b2 t
	set c_columndatatype = 'T', c_metadataxml = null, c_visualattributes='FA'
	where t.c_basecode in (select distinct x.concept_cd from wt_RNA_SEQ_nodes x);

	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Initialize data_type and xml in i2b2',SQL%ROWCOUNT,stepCt,'Done');
	commit;


 ---INSERT sample_dimension
      INSERT INTO I2B2DEMODATA.SAMPLE_DIMENSION(SAMPLE_CD)
         SELECT DISTINCT SAMPLE_CD FROM
           DEAPP.DE_SUBJECT_SAMPLE_MAPPING WHERE SAMPLE_CD NOT IN (SELECT SAMPLE_CD FROM I2B2DEMODATA.SAMPLE_DIMENSION) ;

	   stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'insert distinct sample_cd in sample_dimension from de_subject_sample_mapping',SQL%ROWCOUNT,stepCt,'Done');
	commit;

    ---- update c_metedataxml in i2b2


       for ul in uploadI2b2
        loop
	 update i2b2 n
	SET n.c_columndatatype = 'T',
      --Static XML String
		n.c_metadataxml =  ('<?xml version="1.0"?><ValueMetadata><Version>3.02</Version><CreationDateTime>08/14/2008 01:22:59</CreationDateTime><TestID></TestID><TestName></TestName><DataType>PosFloat</DataType><CodeType></CodeType><Loinc></Loinc><Flagstouse></Flagstouse><Oktousevalues>Y</Oktousevalues><MaxStringLength></MaxStringLength><LowofLowValue>0</LowofLowValue>
                <HighofLowValue>0</HighofLowValue><LowofHighValue>100</LowofHighValue>100<HighofHighValue>100</HighofHighValue>
                <LowofToxicValue></LowofToxicValue><HighofToxicValue></HighofToxicValue>
                <EnumValues></EnumValues><CommentsDeterminingExclusion><Com></Com></CommentsDeterminingExclusion>
                <UnitValues><NormalUnits>ratio</NormalUnits><EqualUnits></EqualUnits>
                <ExcludingUnits></ExcludingUnits><ConvertingUnits><Units></Units><MultiplyingFactor></MultiplyingFactor>
                </ConvertingUnits></UnitValues><Analysis><Enums /><Counts />
                <New /></Analysis>'||(select xmlelement(name "SeriesMeta",xmlforest(m.display_value as "Value",m.display_unit as "Unit",m.display_label as "DisplayName")) as hi
      from tm_lz.lt_src_rna_display_mapping m where m.category_cd=ul.category_cd)||
                '</ValueMetadata>') where n.c_fullname=(select leaf_node from wt_RNA_SEQ_nodes where category_cd=ul.category_cd and leaf_node=n.c_fullname);

                end loop;

	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Update c_columndatatype and c_metadataxml for numeric data types in I2B2METADATA i2b2',SQL%ROWCOUNT,stepCt,'Done');
	commit;

/*
	--UPDATE VISUAL ATTRIBUTES for Leaf Active (Default is folder)
	update i2b2 a
    set c_visualattributes = 'LA'
    where 1 = (
      select count(*)
      from i2b2 b
      where b.c_fullname like (a.c_fullname || '%'))
      and c_fullname like '%' || topNode || '%';
*/

	--UPDATE VISUAL ATTRIBUTES for Leaf Active (Default is folder)
	update i2b2 a
        set c_visualattributes = 'LAH'
	where a.c_basecode in (select distinct x.concept_code from de_subject_sample_mapping x
						   where x.trial_name = TrialId
						     and x.platform = 'RNA_SEQ'
							 and x.concept_code is not null);

	stepCt := stepCt + 1;

	cz_write_audit(jobId,databaseName,procedureName,'Update visual attributes for leaf nodes in I2B2METADATA i2b2',SQL%ROWCOUNT,stepCt,'Done');

        update i2b2 a
	set c_visualattributes='FAS'
        where a.c_fullname = substr(topNode,1,instr(topNode,'\',1,3));


       -- SUBSTR(topNode,1,Length(TrialId) + instr(topNode,TrialId));

      /*  update i2b2 a
	set c_visualattributes=(
		with upd as (select p.c_fullname, count(*) as nbr_children
				 from i2b2 p
					 ,i2b2 c
				 where p.c_fullname like topNode || '%'
				   and c.c_fullname like p.c_fullname || '%'
				 group by p.c_fullname)
		select case when u.nbr_children = 1
					then 'L' || substr(a.c_visualattributes,2,2)
	                else 'F' || substr(a.c_visualattributes,2,1) ||
						 case when u.c_fullname = topNode
						      then case when '' = 'Y' then 'J' else 'S' end
						 else substr(a.c_visualattributes,3,1) end
			   end
		from upd u
		where a.c_fullname = u.c_fullname)
	where a.c_fullname in
		(select x.c_fullname from i2b2 x
		 where x.c_fullname like topNode || '%');*/

        stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Update visual attributes for study nodes in I2B2METADATA i2b2',SQL%ROWCOUNT,stepCt,'Done');

	COMMIT;
   insert into probeset_deapp
   (
   probeset,
   platform
   )select distinct s.probeset
               ,m.platform
            from lt_src_rna_seq_data s,
                 lt_src_RNA_SEQ_subj_samp_map m
                 where s.trial_name=m.trial_name
                   and not exists
		 (select 1 from probeset_deapp x
		  where m.platform = x.platform
		    and s.probeset = x.probeset);

                    stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Insert new probesets into probeset_deapp',SQL%ROWCOUNT,stepCt,'Done');



  --Build concept Counts
  --Also marks any i2B2 records with no underlying data as Hidden, need to do at Trial level because there may be multiple platform and there is no longer
  -- a unique top-level node for RNA_sequencing data

    i2b2_create_concept_counts(topNode ,jobID );
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Create concept counts',0,stepCt,'Done');

	--	delete each node that is hidden

	 FOR r_delNodes in delNodes Loop

    --	deletes hidden nodes for a trial one at a time

		i2b2_delete_1_node(r_delNodes.c_fullname);
		stepCt := stepCt + 1;
		tText := 'Deleted node: ' || r_delNodes.c_fullname;

		cz_write_audit(jobId,databaseName,procedureName,tText,SQL%ROWCOUNT,stepCt,'Done');

	END LOOP;

  --Reload Security: Inserts one record for every I2B2 record into the security table

    i2b2_load_security_data(jobId);
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Load security data',0,stepCt,'Done');

--	tag data with probeset_id from reference.probeset_deapp

	execute immediate ('truncate table tm_wz.wt_subject_rna_probeset');

	--	note: assay_id represents a unique subject/site/sample


	insert into wt_subject_rna_probeset
	(probeset_id
--	,expr_id
	,intensity_value
	,patient_id
--	,sample_cd
--	,subject_id
	,trial_name
	,assay_id
	) select md.probeset
--		  ,sd.sample_cd
		 , avg(md.intensity_value) as intensity_value
		  ,sd.patient_id
--		  ,sd.sample_cd
--		  ,sd.subject_id
		 ,TrialId as trial_name
		  ,sd.assay_id
	from de_subject_sample_mapping sd
		,lt_src_RNA_SEQ_data md
		,probeset_deapp gs
	where sd.sample_cd = md.expr_id
	  and sd.platform = 'RNA_SEQ'
	  and sd.trial_name = TrialId
	  and sd.source_cd = sourceCd
	--  and sd.gpl_id = gs.platform
	and md.probeset = gs.probeset
	  and decode(dataType,'R',sign(md.intensity_value),1) <>  -1  --	take only >=0 for dataType R --UAT 154 changes done 19/03/2014
          and sd.subject_id in (select subject_id from lt_src_RNA_SEQ_subj_samp_map)
	group by md.probeset
		--  ,sd.sample_cd
		  ,sd.patient_id
		--  ,sd.sample_cd
		--  ,sd.subject_id
		  ,sd.assay_id;

	pExists := SQL%ROWCOUNT;

	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Insert into DEAPP wt_subject_rna_probeset',SQL%ROWCOUNT,stepCt,'Done');

	commit;

	if pExists = 0 then
		raise no_probeset_recs;
	end if;

	--	insert into de_subject_rna_data when dataType is T (transformed)

	if dataType = 'T' then
		insert into de_subject_rna_data
		(trial_source
		,probeset_id
		,assay_id
		,patient_id
		--,sample_id
		--,subject_id
		,trial_name
		,zscore
		)
		select TrialId || ':' || sourceCd
			  ,probeset_id
			  ,assay_id
			  ,patient_id
			  --,sample_id
			  --,subject_id
			  ,trial_name
			  ,case when intensity_value < -2.5
			        then -2.5
					when intensity_value > 2.5
					then 2.5
					else intensity_value
			   end as zscore
		from wt_subject_rna_probeset
		where trial_name = TrialID;
		stepCt := stepCt + 1;
		cz_write_audit(jobId,databaseName,procedureName,'Insert transformed into DEAPP de_subject_rna_data',SQL%ROWCOUNT,stepCt,'Done');

		commit;
	else

	--	Calculate ZScores and insert data into de_subject_rna_data.  The 'L' parameter indicates that the RNA_sequencing  data will be selected from
	--	wt_subject_RNA_seq_probeset as part of a Load.

		if dataType = 'R' or dataType = 'L' then
			I2B2_RNA_SEQ_ZSCORE_CALC(TrialID,'L',jobId,dataType,logBase,sourceCD);
			stepCt := stepCt + 1;
			cz_write_audit(jobId,databaseName,procedureName,'Calculate Z-Score',0,stepCt,'Done');
			commit;
		end if;

	end if;

    ---Cleanup OVERALL JOB if this proc is being run standalone

	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'End i2b2_process_RNA_SEQ_data',0,stepCt,'Done');

	IF newJobFlag = 1
	THEN
		cz_end_audit (jobID, 'SUCCESS');
	END IF;

	select 0 into rtn_code from dual;

	EXCEPTION
	--when unmapped_patients then
	--	cz_write_audit(jobId,databasename,procedurename,'No site_id/subject_id mapped to patient_dimension',1,stepCt,'ERROR');
	--	cz_error_handler(jobid,procedurename);
	--	cz_end_audit (jobId,'FAIL');
	when missing_platform then
		cz_write_audit(jobId,databasename,procedurename,'Platform data missing from one or more subject_sample mapping records',1,stepCt,'ERROR');
		cz_error_handler(jobid,procedurename);
		cz_end_audit (jobId,'FAIL');
		select 161 into rtn_code from dual;
	when missing_tissue then
		cz_write_audit(jobId,databasename,procedurename,'Tissue Type data missing from one or more subject_sample mapping records',1,stepCt,'ERROR');
		cz_error_handler(jobid,procedurename);
		CZ_END_AUDIT (JOBID,'FAIL');
		select 162 into rtn_code from dual;
	/*when unmapped_platform then
		cz_write_audit(jobId,databasename,procedurename,'Platform not found in de_RNA_annotation',1,stepCt,'ERROR');
		CZ_ERROR_HANDLER(JOBID,PROCEDURENAME);
		cz_end_audit (jobId,'FAIL');
		select 163 into rtn_code from dual;*/
	when multiple_platform then
		cz_write_audit(jobId,databasename,procedurename,'Multiple platforms for sample_cd in lt_src_RNA_SEQ_subj_samp_map',1,stepCt,'ERROR');
		CZ_ERROR_HANDLER(JOBID,PROCEDURENAME);
		cz_end_audit (jobId,'FAIL');
		select 164 into rtn_code from dual;
	when no_probeset_recs then
		cz_write_audit(jobId,databasename,procedurename,'Unable to match probesets to platform in probeset_deapp',1,stepCt,'ERROR');
		CZ_ERROR_HANDLER(JOBID,PROCEDURENAME);
		cz_end_audit (jobId,'FAIL');
		select 165 into rtn_code from dual;
	WHEN OTHERS THEN
		--Handle errors.
		cz_error_handler (jobID, procedureName);
		--End Proc
		cz_end_audit (jobID, 'FAIL');
		select 166 into rtn_code from dual;
END;
/
