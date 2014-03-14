CREATE OR REPLACE PROCEDURE "TM_CZ"."I2B2_PROCESS_SNP_DATA" 
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
) AS
	--Audit variables
	newJobFlag		integer;
	databaseName 	VARCHAR(100);
	procedureName 	VARCHAR(100);
	jobID 			numeric(18,0);
	stepCt 			numeric(18,0);
	rowCt			numeric(18,0);
	errorNumber		varchar2(4000);
	errorMessage	varchar2(4000);
	rtnCd			integer;

	TrialID			varchar(100);
	RootNode		varchar(2000);
	root_level		integer;
	topNode			varchar(2000);
	topLevel		integer;
	tPath			varchar(2000);
	study_name		varchar(100);
	sourceCd		varchar(50);
	secureStudy		varchar(1);

	dataType		varchar(10);
	sqlText			varchar(1000);
	tText			varchar(1000);
	gplTitle		varchar(1000);
	pExists			numeric;
	partTbl   		numeric;
	partExists 		numeric;
	sampleCt		numeric;
	idxExists 		numeric;
	logBase			numeric;
	pCount			integer;
	sCount			integer;
	tablespaceName	varchar(200);
	partitioniD		numeric(18,0);
	partitionName	varchar(100);
	partitionIndx	varchar(100);


	--unmapped_patients exception;
	missing_platform	exception;
	missing_tissue	EXCEPTION;
	unmapped_platform exception;
	multiple_platform	exception;
	no_probeset_recs	exception;

	--	cursor to add leaf nodes, cursor is used here because there are few nodes to be added

	CURSOR addNodes is
	select distinct t.leaf_node
	        ,t.node_name
	from  tm_wz.wt_mrna_nodes t
	where not exists
		 (select 1 from i2b2metadata.i2b2 x
		  where t.leaf_node = x.c_fullname);

	--	cursor to define the path for delete_one_node  this will delete any nodes that are hidden after i2b2_create_concept_counts

	CURSOR delNodes is
	select distinct c_fullname
	from  i2b2metadata.i2b2
	where c_fullname like topNode || '%' escape '`'
	    and substr(c_visualattributes,2,1) = 'H';

BEGIN
	TrialID := upper(trial_id);
	secureStudy := upper(secure_study);
	
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
	    tm_cz.cz_start_audit(procedureName, databaseName, jobID);
	END IF;
	
	stepCt := 0;
	stepCt := stepCt + 1;
	tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Starting i2b2_process_snp_data',0,stepCt,'Done');

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
	
	logBase := coalesce(log_base, 2);
	sourceCd := upper(coalesce(source_cd,'STD'));

	--	Get count of records in tm_lz.lt_src_mrna_subj_samp_map

	select count(*) into sCount
	from tm_lz.lt_src_mrna_subj_samp_map;

	--	check if all subject_sample map records have a subject_id, If not, abort run

	select count(*) into pCount
	from tm_lz.lt_src_mrna_subj_samp_map t
	where subject_id is null;

	if pCount > 0 then
		stepCt := stepCt + 1;
		tm_cz.cz_write_audit(jobId,databaseName,procedureName,'subject_id missing in lt_src_mrna_subj_samp_map',0,pCount,'Done');
		tm_cz.cz_error_handler (jobID, procedureName);
		tm_cz.cz_end_audit (jobID, 'FAIL');
		raise missing_platform;
	end if;
	
	  	--	check if platform exists in probeset_deapp .  If not, abort run.

	select count(*) into pCount
	from tm_cz.probeset_deapp
	where platform in (select distinct m.platform from tm_lz.lt_src_mrna_subj_samp_map m);

	if pCount = 0 then
		tm_cz.cz_write_audit(jobId,databaseName,procedureName,'No Gene Expression platforms in deapp.de_mrna_annotation',0,pCount,'Done');
		tm_cz.cz_error_handler (jobID, procedureName);
		tm_cz.cz_end_audit (jobID, 'FAIL');
		raise missing_platform;
	end if;

	--	check if entry in deapp.de_gpl_info for every Gene Expression platform, if not, abort run

	select count(*) into pCount
	from tm_lz.lt_src_mrna_subj_samp_map sm
	where not exists
		 (select 1 from deapp.de_gpl_info gi
		  where sm.platform = gi.platform
		    and gi.marker_type = 'Gene Expression'
			and gi.title is not null);

	if pCount > 0 then
		tm_cz.cz_write_audit(jobId,databaseName,procedureName,'deapp.de_gpl_info entry missing for one or more platforms',0,pCount,'Done');
		tm_cz.cz_error_handler (jobID, procedureName);
		tm_cz.cz_end_audit (jobID, 'FAIL');
		raise missing_platform;
	end if;

	--	check if all subject_sample map records have a tissue_type, If not, abort run

	select count(*) into pCount
	from tm_lz.lt_src_mrna_subj_samp_map
	where tissue_type is null;

	if pCount > 0 then
		tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Tissue_Type is null for subjects',0,pCount,'Done');
		tm_cz.cz_error_handler (jobID, procedureName);
		tm_cz.cz_end_audit (jobID, 'FAIL');
		raise missing_platform;
	end if;

	--	check if there are multiple platforms for a single sample   if yes, then different source_cd must be used to load the samples.

	select count(*) into pCount
	from (select sample_cd
		  from tm_lz.lt_src_mrna_subj_samp_map
		  group by sample_cd
		  having count(distinct platform) > 1) x;

	if pCount > 0 then
		tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Multiple platforms for single sample',0,pCount,'Done');
		tm_cz.cz_error_handler (jobID, procedureName);
		tm_cz.cz_end_audit (jobID, 'FAIL');
		raise missing_platform;
	end if;

	-- Get root_node from topNode

	select tm_cz.parse_nth_value(topNode, 2, '\') into RootNode from dual;

	select count(*) into pExists
	from i2b2metadata.i2b2
	where c_name = rootNode;

	if pExists = 0 then
		tm_cz.i2b2_add_root_node(rootNode, jobId);
	end if;

	select c_hlevel into root_level
	from i2b2metadata.table_access
	where c_name = RootNode;

	-- Get study name from topNode

	select tm_cz.parse_nth_value(topNode, topLevel, '\') into study_name from dual;

	--	Add any upper level nodes as needed

	tPath := REGEXP_REPLACE(replace(top_node,study_name,''),'(\\){2,}', '\');
	select length(tPath) - length(replace(tPath,'\','')) into pCount from dual;

	if pCount > 2 then
		tm_cz.i2b2_fill_in_tree('', tPath, jobId);
	end if;  
	
	--	uppercase study_id in tm_lz.lt_src_mrna_subj_samp_map in case curator forgot

	begin
	  update tm_lz.lt_src_mrna_subj_samp_map
	    set trial_name=upper(trial_name);
	  commit;	
	exception
	when others then
		--Handle errors.
		tm_cz.cz_error_handler (jobID, procedureName);
		--End Proc
		tm_cz.cz_end_audit (jobID, 'FAIL');
		raise missing_platform;
	end;
	
	stepCt := stepCt + 1;
	tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Uppercase trial_name in tm_lz.lt_src_mrna_subj_samp_map',rowCt,stepCt,'Done');

	--	create records in patient_dimension for subject_ids if they do not exist
	--	format of sourcesystem_cd:  trial:[site:]subject_cd

	begin
	insert into i2b2demodata.patient_dimension
	  ( patient_num,
	    sex_cd,
	    age_in_years_num,
	    race_cd,
	    update_date,
	    download_date,
	    import_date,
	    sourcesystem_cd
	  )
	  select i2b2demodata.seq_patient_num.nextval
		  ,x.sex_cd
		  ,x.age_in_years_num
		  ,x.race_cd
		  ,current_timestamp
		  ,current_timestamp
		  ,current_timestamp
		  ,x.sourcesystem_cd
	from (select distinct 'Unknown' as sex_cd,
				 0 as age_in_years_num,
				 null as race_cd,
				 regexp_replace(TrialID || ':' || coalesce(s.site_id,'') || ':' || s.subject_id,'(::){1,}', ':') as sourcesystem_cd
		 from tm_lz.lt_src_mrna_subj_samp_map s
		     ,deapp.de_gpl_info g
		 where s.subject_id is not null
		   and s.trial_name = TrialID
		   and s.source_cd = sourceCD
		   and s.platform = g.platform
		   and upper(g.marker_type) = 'GENE EXPRESSION'
		   and not exists
			  (select 1 from i2b2demodata.patient_dimension x
			   where x.sourcesystem_cd =
				 regexp_replace(TrialID || ':' || coalesce(s.site_id,'') || ':' || s.subject_id,'(::){1,}', ':'))
		) x;
	  commit;
	exception
	when others then
		--Handle errors.
		tm_cz.cz_error_handler (jobID, procedureName);
		--End Proc
		tm_cz.cz_end_audit (jobID, 'FAIL');
		raise missing_platform;
	end;
	
	stepCt := stepCt + 1;
	tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Insert subjects to patient_dimension',rowCt,stepCt,'Done');

	--	add security for trial if new subjects added to patient_dimension

	if pCount > 0 then
		tm_cz.i2b2_create_security_for_trial(TrialId, secureStudy, jobID);
	end if;

	--	Delete existing observation_fact data, will be repopulated

	begin
	delete from i2b2demodata.observation_fact obf
	where obf.concept_cd in
		 (select distinct x.concept_code
		  from deapp.de_subject_sample_mapping x
		  where x.trial_name = TrialId
		    and coalesce(x.source_cd,'STD') = sourceCD
		    and x.platform = 'SNP');
	commit;
	exception
	when others then
		--Handle errors.
		tm_cz.cz_error_handler (jobID, procedureName);
		--End Proc
		tm_cz.cz_end_audit (jobID, 'FAIL');
		raise missing_platform;
	end;
	stepCt := stepCt + 1;
	tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Delete data from observation_fact',rowCt,stepCt,'Done');

	--	check if trial/source_cd already loaded, if yes, get existing partition_id else get new one

	select count(*) into partExists
	from deapp.de_subject_sample_mapping sm
	where sm.trial_name = TrialId
	  and coalesce(sm.source_cd,'STD') = sourceCd
	  and sm.platform = 'SNP';
	  --and sm.partition_id is not null;

--	if partExists = 0 then
--		select deapp.seq_mrna_partition_id.nextval into partitionId from dual;
--	else
--		select distinct partition_id into partitionId
--		from deapp.de_subject_sample_mapping sm
--		where sm.trial_name = TrialId
--		  and coalesce(sm.source_cd,'STD') = sourceCd
--		  and sm.platform = 'SNP';
----	end if;
--
--	partitionName := 'deapp.de_subject_microarray_data_' || partitionId;
--	partitionIndx := 'de_subject_microarray_data_' || partitionId;

	--	truncate tmp node table

	execute immediate ('truncate table tm_wz.wt_mrna_nodes');

	--	load temp table with leaf node path, use temp table with distinct sample_type, ATTR2, platform, and title   this was faster than doing subselect
	--	from wt_subject_mrna_data

	execute immediate ('truncate table tm_wz.wt_mrna_node_values');
	
	begin
	insert into tm_wz.wt_mrna_node_values
	(category_cd
	,platform
	,tissue_type
	,attribute_1
	,attribute_2
	,title
	)
	select distinct a.category_cd
				   ,coalesce(a.platform,'GPL570')
				   ,coalesce(a.tissue_type,'Unspecified Tissue Type')
	               ,a.attribute_1
				   ,a.attribute_2
				   ,g.title
	  from tm_lz.lt_src_mrna_subj_samp_map a
	    ,deapp.de_gpl_info g
	where a.trial_name = TrialID
	  and coalesce(a.platform,'GPL570') = g.platform
	  and a.source_cd = sourceCD
	  and a.platform = g.platform
	  and upper(g.marker_type) = 'GENE EXPRESSION';
	commit;
	exception
	when others then
		--Handle errors.
		tm_cz.cz_error_handler (jobID, procedureName);
		--End Proc
		tm_cz.cz_end_audit (jobID, 'FAIL');
		raise missing_platform;
	end;
	stepCt := stepCt + 1;
	tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Insert node values into DEAPP tm_wz.wt_mrna_node_values',rowCt,stepCt,'Done');

		--	inserts that create the ontology for the leaf nodes

	begin
	insert into tm_wz.wt_mrna_nodes
	(leaf_node
	,category_cd
	,platform
	,tissue_type
	,attribute_1
	  ,attribute_2
	,node_type
	)
	select distinct topNode || regexp_replace(replace(replace(replace(replace(replace(replace(
	       category_cd,'PLATFORM',title),'ATTR1',coalesce(attribute_1,'')),'ATTR2',coalesce(attribute_2,'')),'TISSUETYPE',coalesce(tissue_type,'')),'+','\'),'_',' ') || '\','(\\){2,}', '\')
		  ,category_cd
		  ,platform as platform
		  ,tissue_type
		  ,attribute_1 as attribute_1
	        ,attribute_2 as attribute_2
		  ,'LEAF'
	from  tm_wz.wt_mrna_node_values;
	commit;
	exception
	when others then
		--Handle errors.
		tm_cz.cz_error_handler (jobID, procedureName);
		--End Proc
		tm_cz.cz_end_audit (jobID, 'FAIL');
		raise missing_platform;
	end;

	stepCt := stepCt + 1;
	tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Create leaf nodes in DEAPP tmp_mrna_nodes',rowCt,stepCt,'Done');

	--	insert for platform node so platform concept can be populated

	begin
	insert into tm_wz.wt_mrna_nodes
	(leaf_node
	,category_cd
	,platform
	,tissue_type
	,attribute_1
	  ,attribute_2
	,node_type
	)
	select distinct topNode || regexp_replace(replace(replace(replace(replace(replace(replace(
	       substr(category_cd,1,instr(category_cd,'PLATFORM')+8),'PLATFORM',title),'ATTR1',coalesce(attribute_1,'')),'ATTR2',coalesce(attribute_2,'')),'TISSUETYPE',coalesce(tissue_type,'')),'+','\'),'_',' ') || '\',
		   '(\\){2,}', '\')
		  ,substr(category_cd,1,instr(category_cd,'PLATFORM')+8)
		  ,platform as platform
		  ,case when instr(substr(category_cd,1,instr(category_cd,'PLATFORM')+8),'TISSUETYPE') > 1 then tissue_type else '' end as tissue_type
		  ,case when instr(substr(category_cd,1,instr(category_cd,'PLATFORM')+8),'ATTR1') > 1 then attribute_1 else '' end as attribute_1
	    ,case when instr(substr(category_cd,1,instr(category_cd,'PLATFORM')+8),'ATTR2') > 1 then attribute_2 else '' end as attribute_2
		  ,'PLATFORM'
	from  tm_wz.wt_mrna_node_values;
	commit;
	exception
	when others then
		--Handle errors.
		tm_cz.cz_error_handler (jobID, procedureName);
		--End Proc
		tm_cz.cz_end_audit (jobID, 'FAIL');
		raise missing_platform;
	end;
	stepCt := stepCt + 1;
	tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Create platform nodes in tm_wz.wt_mrna_nodes',rowCt,stepCt,'Done');

	--	insert for ATTR1 node so ATTR1 concept can be populated in tissue_type_cd

	begin
	insert into tm_wz.wt_mrna_nodes
	(leaf_node
	,category_cd
	,platform
	,tissue_type
	  ,attribute_1
	,attribute_2
	,node_type
	)
	select distinct topNode || regexp_replace(replace(replace(replace(replace(replace(replace(
	       substr(category_cd,1,instr(category_cd,'ATTR1')+5),'PLATFORM',title),'ATTR1',coalesce(attribute_1,'')),'ATTR2',coalesce(attribute_2,'')),'TISSUETYPE',coalesce(tissue_type,'')),'+','\'),'_',' ') || '\',
		   '(\\){2,}', '\')
		  ,substr(category_cd,1,instr(category_cd,'ATTR1')+5)
		  ,case when instr(substr(category_cd,1,instr(category_cd,'ATTR1')+5),'PLATFORM') > 1 then platform else '' end as platform
		  ,case when instr(substr(category_cd,1,instr(category_cd,'ATTR1')+5),'TISSUETYPE') > 1 then tissue_type else '' end as tissue_type
		  ,attribute_1 as attribute_1
	    ,case when instr(substr(category_cd,1,instr(category_cd,'ATTR1')+5),'ATTR2') > 1 then attribute_2 else '' end as attribute_2
		  ,'ATTR1'
	from  tm_wz.wt_mrna_node_values
	where category_cd like '%ATTR1%'
	  and attribute_1 is not null;
	commit;
	exception
	when others then
		--Handle errors.
		tm_cz.cz_error_handler (jobID, procedureName);
		--End Proc
		tm_cz.cz_end_audit (jobID, 'FAIL');
		raise missing_platform;
	end;
	  stepCt := stepCt + 1;
	tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Create ATTR1 nodes in tm_wz.wt_mrna_nodes',rowCt,stepCt,'Done');

	--	insert for ATTR2 node so ATTR2 concept can be populated in timepoint_cd

	begin
	insert into tm_wz.wt_mrna_nodes
	(leaf_node
	,category_cd
	,platform
	,tissue_type
	  ,attribute_1
	,attribute_2
	,node_type
	)
	select distinct topNode || regexp_replace(replace(replace(replace(replace(replace(replace(
	       substr(category_cd,1,instr(category_cd,'ATTR2')+5),'PLATFORM',title),'ATTR1',coalesce(attribute_1,'')),'ATTR2',coalesce(attribute_2,'')),'TISSUETYPE',coalesce(tissue_type,'')),'+','\'),'_',' ') || '\',
		   '(\\){2,}', '\')
		  ,substr(category_cd,1,instr(category_cd,'ATTR2')+5)
		  ,case when instr(substr(category_cd,1,instr(category_cd,'ATTR2')+5),'PLATFORM') > 1 then platform else '' end as platform
		  ,case when instr(substr(category_cd,1,instr(category_cd,'ATTR1')+5),'TISSUETYPE') > 1 then tissue_type else '' end as tissue_type
	        ,case when instr(substr(category_cd,1,instr(category_cd,'ATTR2')+5),'ATTR1') > 1 then attribute_1 else '' end as attribute_1
		  ,attribute_2 as attribute_2
		  ,'ATTR2'
	from  tm_wz.wt_mrna_node_values
	where category_cd like '%ATTR2%'
	  and attribute_2 is not null;
	commit;
	exception
	when others then
		--Handle errors.
		tm_cz.cz_error_handler (jobID, procedureName);
		--End Proc
		tm_cz.cz_end_audit (jobID, 'FAIL');
		raise missing_platform;
	end;
	  stepCt := stepCt + 1;
	tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Create ATTR2 nodes in tm_wz.wt_mrna_nodes',rowCt,stepCt,'Done');

	--	insert for tissue_type node so sample_type_cd can be populated

	begin
	insert into tm_wz.wt_mrna_nodes
	(leaf_node
	,category_cd
	,platform
	,tissue_type
	,attribute_1
	  ,attribute_2
	,node_type
	)
	select distinct topNode || regexp_replace(replace(replace(replace(replace(replace(replace(
	       substr(category_cd,1,instr(category_cd,'TISSUETYPE')+10),'PLATFORM',title),'ATTR1',coalesce(attribute_1,'')),'ATTR2',coalesce(attribute_2,'')),'TISSUETYPE',coalesce(tissue_type,'')),'+','\'),'_',' ') || '\',
		   '(\\){2,}', '\')
		  ,substr(category_cd,1,instr(category_cd,'TISSUETYPE')+10)
		  ,case when instr(substr(category_cd,1,instr(category_cd,'TISSUETYPE')+10),'PLATFORM') > 1 then platform else '' end as platform
		  ,tissue_type as tissue_type
		  ,case when instr(substr(category_cd,1,instr(category_cd,'TISSUETYPE')+10),'ATTR1') > 1 then attribute_1 else '' end as attribute_1
	    ,case when instr(substr(category_cd,1,instr(category_cd,'TISSUETYPE')+10),'ATTR2') > 1 then attribute_2 else '' end as attribute_2
		  ,'TISSUETYPE'
	from  tm_wz.wt_mrna_node_values
	where category_cd like '%TISSUETYPE%';
	commit;
	exception
	when others then
		--Handle errors.
		tm_cz.cz_error_handler (jobID, procedureName);
		--End Proc
		tm_cz.cz_end_audit (jobID, 'FAIL');
		raise missing_platform;
	end;
	  stepCt := stepCt + 1;
	tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Create ATTR2 nodes in tm_wz.wt_mrna_nodes',rowCt,stepCt,'Done');

	--	set node_name

	begin
	update tm_wz.wt_mrna_nodes
	set node_name=tm_cz.parse_nth_value(leaf_node,length(leaf_node)-length(replace(leaf_node,'\','')),'\');
	commit;
	exception
	when others then
		--Handle errors.
		tm_cz.cz_error_handler (jobID, procedureName);
		--End Proc
		tm_cz.cz_end_audit (jobID, 'FAIL');
		raise missing_platform;
	end;
	  stepCt := stepCt + 1;
	tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Updated node_name in DEAPP tmp_mrna_nodes',rowCt,stepCt,'Done');

	--	add leaf nodes for mRNA data  The cursor will only add nodes that do not already exist.

	 FOR r_addNodes in addNodes Loop

	  --Add nodes for all types (ALSO DELETES EXISTING NODE)

		tm_cz.i2b2_add_node(TrialID, r_addNodes.leaf_node, r_addNodes.node_name, jobId);
		stepCt := stepCt + 1;
		tText := 'Added Leaf Node: ' || r_addNodes.leaf_node || '  Name: ' || r_addNodes.node_name;

		tm_cz.cz_write_audit(jobId,databaseName,procedureName,tText,1,stepCt,'Done');

		tm_cz.i2b2_fill_in_tree(TrialId, r_addNodes.leaf_node, jobID);

	END LOOP;

	--	set sourcesystem_cd, c_comment to null if any added upper-level nodes

	begin
	update i2b2metadata.i2b2 b
	set sourcesystem_cd=null,c_comment=null
	where b.sourcesystem_cd = TrialId
	  and length(b.c_fullname) < length(topNode);
	commit;
	exception
	when others then
		--Handle errors.
		tm_cz.cz_error_handler (jobID, procedureName);
		--End Proc
		tm_cz.cz_end_audit (jobID, 'FAIL');
		raise missing_platform;
	end;
	stepCt := stepCt + 1;
	tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Set sourcesystem_cd to null for added upper level nodes',rowCt,stepCt,'Done');

	--	update concept_cd for nodes, this is done to make the next insert easier

	begin
	update tm_wz.wt_mrna_nodes t
	set concept_cd=(select c.concept_cd from i2b2demodata.concept_dimension c
	                where c.concept_path = t.leaf_node
				   )
	  where exists
	       (select 1 from i2b2demodata.concept_dimension x
	                where x.concept_path = t.leaf_node
				   )
	  and t.concept_cd is null;
	commit;
	exception
	when others then
		--Handle errors.
		tm_cz.cz_error_handler (jobID, procedureName);
		--End Proc
		tm_cz.cz_end_audit (jobID, 'FAIL');
		raise missing_platform;
	end;
	stepCt := stepCt + 1;
	tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Update tm_wz.wt_mrna_nodes with newly created concept_cds',rowCt,stepCt,'Done');

	--	delete any site/subject/samples that are not in lt_src_mrna_data for the trial on a reload

--	if partExists > 0 then
--	begin
--	delete from deapp.de_subject_sample_mapping sm
--	where sm.trial_name = trial_id
--	  and sm.source_cd = sourceCd
--	  and sm.platform = 'SNP'
--	 and not exists
--		 (select 1 from tm_lz.lt_src_mrna_subj_samp_map x
--		  where coalesce(sm.site_id,'@') = coalesce(x.site_id,'@')
--		    and sm.subject_id = x.subject_id
--			and sm.sample_cd = x.sample_cd
--			and sm.source_cd = coalesce(x.source_cd,'STD'));
--	commit;
--	exception
--	when others then
--		--Handle errors.
--		tm_cz.cz_error_handler (jobID, procedureName);
--		--End Proc
--		tm_cz.cz_end_audit (jobID, 'FAIL');
--		raise missing_platform;
--		end;
--	end if;
--	stepCt := stepCt + 1;
--	tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Delete dropped site/subject/sample from de_subject_sample_mapping',rowCt,stepCt,'Done');

	--Update or insert DE_SUBJECT_SAMPLE_MAPPING from wt_subject_mrna_data

	--PATIENT_ID      = PATIENT_ID (SAME AS ID ON THE PATIENT_DIMENSION)
	--SITE_ID         = site_id
	--SUBJECT_ID      = subject_id
	--SUBJECT_TYPE    = NULL
	--CONCEPT_CODE    = from LEAF records in tm_wz.wt_mrna_nodes
	--SAMPLE_TYPE    	= TISSUE_TYPE
	--SAMPLE_TYPE_CD  = concept_cd from TISSUETYPE records in tm_wz.wt_mrna_nodes
	--TRIAL_NAME      = TRIAL_NAME
	--TIMEPOINT		= attribute_2
	--TIMEPOINT_CD	= concept_cd from ATTR2 records in tm_wz.wt_mrna_nodes
	--TISSUE_TYPE     = attribute_1
	--TISSUE_TYPE_CD  = concept_cd from ATTR1 records in tm_wz.wt_mrna_nodes
	--PLATFORM        = MRNA_AFFYMETRIX - this is required by ui code
	--PLATFORM_CD     = concept_cd from PLATFORM records in tm_wz.wt_mrna_nodes
	--DATA_UID		= concatenation of concept_cd-patient_num
	--GPL_ID			= platform from wt_subject_mrna_data
	--CATEGORY_CD		= category_cd that generated ontology
	--SAMPLE_ID		= id of sample (trial:S:[site_id]:subject_id:sample_cd) from patient_dimension, may be the same as patient_num
	--SAMPLE_CD		= sample_cd
	--SOURCE_CD		= sourceCd
	--PARTITION_ID	= partitionId

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
			  ,'MRNA_AFFYMETRIX' as platform
			  ,pn.concept_cd as platform_cd
			  ,ln.concept_cd || '-' || to_char(b.patient_num) as data_uid
			  ,a.platform as gpl_id
			  ,coalesce(sid.patient_num,b.patient_num) as sample_id
			  ,a.sample_cd
			  ,nvl(a.category_cd,'Biomarker_Data+Gene_Expression+PLATFORM+TISSUETYPE+ATTR1+ATTR2') as category_cd
			  ,a.source_cd
			  ,TrialId as omic_source_study
			  ,b.patient_num as omic_patient_id
		from lt_src_mrna_subj_samp_map a		
		--Joining to Pat_dim to ensure the ID's match. If not I2B2 won't work.
		inner join patient_dimension b
		  on regexp_replace(TrialID || ':' || a.site_id || ':' || a.subject_id,'(::){1,}', ':') = b.sourcesystem_cd
		inner join wt_mrna_nodes ln
			on a.platform = ln.platform
			and a.tissue_type = ln.tissue_type
			and nvl(a.attribute_1,'@') = nvl(ln.attribute_1,'@')
			and nvl(a.attribute_2,'@') = nvl(ln.attribute_2,'@')
			and ln.node_type = 'LEAF'
		inner join wt_mrna_nodes pn
			on a.platform = pn.platform
			and case when instr(substr(a.category_cd,1,instr(a.category_cd,'PLATFORM')+8),'TISSUETYPE') > 1 then a.tissue_type else '@' end = nvl(pn.tissue_type,'@')
			and case when instr(substr(a.category_cd,1,instr(a.category_cd,'PLATFORM')+8),'ATTR1') > 1 then a.attribute_1 else '@' end = nvl(pn.attribute_1,'@')
			and case when instr(substr(a.category_cd,1,instr(a.category_cd,'PLATFORM')+8),'ATTR2') > 1 then a.attribute_2 else '@' end = nvl(pn.attribute_2,'@')
			and pn.node_type = 'PLATFORM'	  
		left outer join wt_mrna_nodes ttp
			on a.tissue_type = ttp.tissue_type
			and case when instr(substr(a.category_cd,1,instr(a.category_cd,'TISSUETYPE')+10),'PLATFORM') > 1 then a.platform else '@' end = nvl(ttp.platform,'@')
			and case when instr(substr(a.category_cd,1,instr(a.category_cd,'TISSUETYPE')+10),'ATTR1') > 1 then a.attribute_1 else '@' end = nvl(ttp.attribute_1,'@')
			and case when instr(substr(a.category_cd,1,instr(a.category_cd,'TISSUETYPE')+10),'ATTR2') > 1 then a.attribute_2 else '@' end = nvl(ttp.attribute_2,'@')
			and ttp.node_type = 'TISSUETYPE'		  
		left outer join wt_mrna_nodes a1
			on a.attribute_1 = a1.attribute_1
			and case when instr(substr(a.category_cd,1,instr(a.category_cd,'ATTR1')+5),'PLATFORM') > 1 then a.platform else '@' end = nvl(a1.platform,'@')
			and case when instr(substr(a.category_cd,1,instr(a.category_cd,'ATTR1')+5),'TISSUETYPE') > 1 then a.tissue_type else '@' end = nvl(a1.tissue_type,'@')
			and case when instr(substr(a.category_cd,1,instr(a.category_cd,'ATTR1')+5),'ATTR2') > 1 then a.attribute_2 else '@' end = nvl(a1.attribute_2,'@')
			and a1.node_type = 'ATTR1'		  
		left outer join wt_mrna_nodes a2
			on a.attribute_2 = a1.attribute_2
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
		
	stepCt := stepCt + 1;
	tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Update existing data in de_subject_sample_mapping',rowCt,stepCt,'Done');
	pcount := rowCt;	--	set counter to check that all subject_sample mapping records were added/updated
	--	insert any site/subject/samples that are not in de_subject_sample_mapping

	begin
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
			  ,'SNP' as platform
			  ,pn.concept_cd as platform_cd
			  ,ln.concept_cd || '-' || b.patient_num as data_uid
			  ,a.platform as gpl_id
			  ,a.sample_cd
			  ,coalesce(a.category_cd,'Biomarker_Data+Gene_Expression+PLATFORM+TISSUETYPE+ATTR1+ATTR2') as category_cd
			  ,a.source_cd
			  ,TrialId as omic_source_study
			  ,b.patient_num as omic_patient_id
		from tm_lz.lt_src_mrna_subj_samp_map a
		--Joining to Pat_dim to ensure the ID's match. If not I2B2 won't work.
		inner join i2b2demodata.patient_dimension b
		  on regexp_replace(TrialID || ':' || coalesce(a.site_id,'') || ':' || a.subject_id,'(::){1,}', ':','g') = b.sourcesystem_cd
		inner join tm_wz.wt_mrna_nodes ln
			on a.platform = ln.platform
			and a.tissue_type = ln.tissue_type
			and coalesce(a.attribute_1,'') = coalesce(ln.attribute_1,'')
			and coalesce(a.attribute_2,'') = coalesce(ln.attribute_2,'')
			and ln.node_type = 'LEAF'
		inner join tm_wz.wt_mrna_nodes pn
			on a.platform = pn.platform
			and case when instr(substr(a.category_cd,1,instr(a.category_cd,'PLATFORM')+8),'TISSUETYPE') > 1 then a.tissue_type else '' end = coalesce(pn.tissue_type,'')
			and case when instr(substr(a.category_cd,1,instr(a.category_cd,'PLATFORM')+8),'ATTR1') > 1 then a.attribute_1 else '' end = coalesce(pn.attribute_1,'')
			and case when instr(substr(a.category_cd,1,instr(a.category_cd,'PLATFORM')+8),'ATTR2') > 1 then a.attribute_2 else '' end = coalesce(pn.attribute_2,'')
			and pn.node_type = 'PLATFORM'
		left outer join tm_wz.wt_mrna_nodes ttp
			on a.tissue_type = ttp.tissue_type
			and case when instr(substr(a.category_cd,1,instr(a.category_cd,'TISSUETYPE')+10),'PLATFORM') > 1 then a.platform else '' end = coalesce(ttp.platform,'')
			and case when instr(substr(a.category_cd,1,instr(a.category_cd,'TISSUETYPE')+10),'ATTR1') > 1 then a.attribute_1 else '' end = coalesce(ttp.attribute_1,'')
			and case when instr(substr(a.category_cd,1,instr(a.category_cd,'TISSUETYPE')+10),'ATTR2') > 1 then a.attribute_2 else '' end = coalesce(ttp.attribute_2,'')
			and ttp.node_type = 'TISSUETYPE'
		left outer join tm_wz.wt_mrna_nodes a1
			on a.attribute_1 = a1.attribute_1
			and case when instr(substr(a.category_cd,1,instr(a.category_cd,'ATTR1')+5),'PLATFORM') > 1 then a.platform else '' end = coalesce(a1.platform,'')
			and case when instr(substr(a.category_cd,1,instr(a.category_cd,'ATTR1')+5),'TISSUETYPE') > 1 then a.tissue_type else '' end = coalesce(a1.tissue_type,'')
			and case when instr(substr(a.category_cd,1,instr(a.category_cd,'ATTR1')+5),'ATTR2') > 1 then a.attribute_2 else '' end = coalesce(a1.attribute_2,'')
			and a1.node_type = 'ATTR1'
		left outer join tm_wz.wt_mrna_nodes a2
			on a.attribute_2 = a1.attribute_2
			and case when instr(substr(a.category_cd,1,instr(a.category_cd,'ATTR2')+5),'PLATFORM') > 1 then a.platform else '' end = coalesce(a2.platform,'')
			and case when instr(substr(a.category_cd,1,instr(a.category_cd,'ATTR2')+5),'TISSUETYPE') > 1 then a.tissue_type else '' end = coalesce(a2.tissue_type,'')
			and case when instr(substr(a.category_cd,1,instr(a.category_cd,'ATTR2')+5),'ATTR1') > 1 then a.attribute_1 else '' end = coalesce(a2.attribute_1,'')
			and a2.node_type = 'ATTR2'
		where a.trial_name = TrialID
		  and a.source_cd = sourceCD
		  and  ln.concept_cd is not null) t;
	    
	commit;
	exception
	when others then
		--Handle errors.
		tm_cz.cz_error_handler(jobID, procedureName);
		--End Proc
		tm_cz.cz_end_audit(jobID, 'FAIL');
		raise missing_platform;
	end;
	stepCt := stepCt + 1;
	tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Insert trial into DEAPP de_subject_sample_mapping',rowCt,stepCt,'Done');
	pCount := pCount + rowCt;
	
	--	check if all records from lt_src_mrna_subj_samp_map were added/updated
	
	if scount <> pCount then
		stepCt := stepCt + 1;
		tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Not all records in lt_src_mrna_subj_samp_map inserted/updated in de_subject_sample_mapping',0,stepCt,'Done');
		tm_cz.cz_error_handler (jobID, procedureName);
		tm_cz.cz_end_audit (jobID, 'FAIL');
		raise missing_platform;
	end if;	
	--	Insert records for subjects into observation_fact

	begin
	insert into i2b2demodata.observation_fact
	  (patient_num
	,concept_cd
	,modifier_cd
	,valtype_cd
	,tval_char
	,sourcesystem_cd
	,import_date
	,valueflag_cd
	,provider_id
	,location_cd
	,units_cd
	  )
	  select distinct m.patient_id
		  ,m.concept_code
		  ,m.trial_name
		  ,'T' -- Text data type
		  ,'E'  --Stands for Equals for Text Types
		  ,m.trial_name
		  ,current_timestamp
		  ,'@'
		  ,'@'
		  ,'@'
		  ,'' -- no units available
	  from  deapp.de_subject_sample_mapping m
	  where m.trial_name = TrialID
	  and m.source_cd = sourceCD
	    and m.platform = 'SNP';
	commit;
	exception
	when others then
		--Handle errors.
		tm_cz.cz_error_handler (jobID, procedureName);
		--End Proc
		tm_cz.cz_end_audit (jobID, 'FAIL');
		raise missing_platform;
	end;
	stepCt := stepCt + 1;
	tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Insert patient facts into I2B2DEMODATA observation_fact',rowCt,stepCt,'Done');

	--Update I2b2 for correct c_columndatatype, c_visualattributes, c_metadataxml

	begin
	update i2b2 t
	set c_columndatatype = 'T', c_metadataxml = null, c_visualattributes='FA'
	where t.c_basecode in (select distinct x.concept_cd from wt_mrna_nodes x);
	commit;
	exception
	when others then
		--Handle errors.
		tm_cz.cz_error_handler (jobID, procedureName);
		--End Proc
		tm_cz.cz_end_audit (jobID, 'FAIL');
		raise missing_platform;
	end;
	stepCt := stepCt + 1;
	tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Initialize data_type, visualattributes and xml in i2b2',rowCt,stepCt,'Done');

	--Build concept Counts
	--Also marks any i2B2 records with no underlying data as Hidden, need to do at Trial level because there may be multiple platform and there is no longer
	-- a unique top-level node for mRNA data

	tm_cz.i2b2_create_concept_counts(topNode ,jobID );
	stepCt := stepCt + 1;
	tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Create concept counts',0,stepCt,'Done');

	--	delete each node that is hidden

	 FOR r_delNodes in delNodes Loop

	  --	deletes hidden nodes for a trial one at a time

		tm_cz.i2b2_delete_1_node(r_delNodes.c_fullname);
		stepCt := stepCt + 1;
		tText := 'Deleted node: ' || r_delNodes.c_fullname;

		tm_cz.cz_write_audit(jobId,databaseName,procedureName,tText,0,stepCt,'Done');

	END LOOP;

	--Reload Security: Inserts one record for every I2B2 record into the security table

	tm_cz.i2b2_load_security_data(jobId);
	stepCt := stepCt + 1;
	tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Load security data',0,stepCt,'Done');

	--	tag data with probeset_id from reference.probeset_deapp

	execute immediate ('truncate table tm_wz.wt_subject_mrna_probeset');

	--	note: assay_id represents a unique subject/site/sample

	begin
	insert into tm_wz.wt_subject_mrna_probeset
	(probeset_id
	,intensity_value
	,assay_id
	,trial_name
	)
	select gs.probeset_id
		  ,avg(md.intensity_value)
		  ,sd.assay_id
		  ,TrialId
	from
	  tm_lz.lt_src_mrna_data md
	    inner join deapp.de_subject_sample_mapping sd
	      inner join tm_cz.probeset_deapp gs
	      on sd.gpl_id = gs.platform
	    on md.expr_id = sd.sample_cd and md.probeset = gs.probeset
	where sd.platform = 'SNP'
	  and sd.trial_name = TrialId
	  and sd.source_cd = sourceCd
	  and case when dataType = 'R'
			   then case when md.intensity_value > 0 then 1 else 0 end
			   else 1 end = 1         --	take only >0 for dataType R
	group by gs.probeset_id
		  ,sd.assay_id;
	commit;
	exception
	when others then
		--Handle errors.
		tm_cz.cz_error_handler (jobID, procedureName);
		--End Proc
		tm_cz.cz_end_audit (jobID, 'FAIL');
		raise missing_platform;
	end;
	stepCt := stepCt + 1;
	tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Insert into DEAPP wt_subject_mrna_probeset',rowCt,stepCt,'Done');

	if rowCt = 0 then
		tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Unable to match probesets to platform in probeset_deapp',0,rowCt,'Done');
		tm_cz.cz_error_handler (jobID, procedureName);
		tm_cz.cz_end_audit (jobID, 'FAIL');
		raise missing_platform;
	end if;
	
	--	add partition if it doesn't exist, drop indexes and truncate if it does (reload)

	--pExists := SQL%ROWCOUNT;
	
	/*if pExists = 0 then
		raise no_probeset_recs;
	end if;*/

	/*	if dataType = 'T' then
		insert into de_subject_microarray_data
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
		from wt_subject_mrna_probeset
		where trial_name = TrialID;
		stepCt := stepCt + 1;
		cz_write_audit(jobId,databaseName,procedureName,'Insert transformed into DEAPP de_subject_microarray_data',SQL%ROWCOUNT,stepCt,'Done');

		commit;	
	else
	  --	insert into de_subject_microarray_data when dataType is T (transformed)
	
	  if dataType = 'Z' then
	    insert into de_subject_microarray_data
	    (trial_source
	    ,probeset_id
	    ,assay_id
	    ,patient_id
	    --,sample_id
	    --,subject_id
	    ,TRIAL_NAME
	    ,log_intensity
	    ,zscore
	    )
	    select TrialId || ':' || sourceCd
	        ,probeset_id
	        ,assay_id
	        ,patient_id
	        --,sample_id
	        --,subject_id
	        ,TRIAL_NAME
	        ,intensity_value
	        ,case when intensity_value < -2.5
	              then -2.5
	          when intensity_value > 2.5
	          then 2.5
	          else intensity_value
	         end as zscore
	    from wt_subject_mrna_probeset
	    where trial_name = TrialID;
	    STEPCT := STEPCT + 1;
	    cz_write_audit(jobId,databaseName,procedureName,'Insert transformed (workaround) into DEAPP de_subject_microarray_data',SQL%ROWCOUNT,stepCt,'Done');
	
	    commit;	
	  else	
	  --	Calculate ZScores and insert data into de_subject_microarray_data.  The 'L' parameter indicates that the gene expression data will be selected from
	  --	wt_subject_mrna_probeset as part of a Load.  
	
	    if dataType = 'R' or dataType = 'L' then
	      i2b2_mrna_zscore_calc(TrialID,'L',jobId,dataType,logBase,sourceCD);
	      stepCt := stepCt + 1;
	      cz_write_audit(jobId,databaseName,procedureName,'Calculate Z-Score',0,stepCt,'Done');
	      commit;
	    end if;
	  end if;
	
	end if;*/


	stepCt := stepCt + 1;
	tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Create indices for ' || partitionIndx,0,stepCt,'Done');

	delete from deapp.de_snp_calls_by_gsm
	where patient_num in (
	  select omic_patient_id
	  from deapp.de_subject_sample_mapping
	  where trial_name = TrialID
	);

	commit;
	stepCt := stepCt + 1;
	tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Cleanup de_snp_calls_by_gsm',rowCt,stepCt,'Done');

	insert into deapp.DE_SNP_CALLS_BY_GSM
	(gsm_num, snp_name, snp_calls, patient_num)
	select ltscbg.gsm_num, ltscbg.snp_name, ltscbg.snp_calls, sm.omic_patient_id from
		tm_lz.lt_snp_calls_by_gsm ltscbg
		inner join deapp.de_subject_sample_mapping sm
		on sm.sample_cd = ltscbg.gsm_num
		where sm.trial_name = TrialID;
	commit;

	stepCt := stepCt + 1;
	tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Fill de_snp_calls_by_gsm',rowCt,stepCt,'Done');

	delete from deapp.de_snp_copy_number
	where patient_num in (
	  select omic_patient_id
	  from deapp.de_subject_sample_mapping
	  where trial_name = TrialID
	);

	commit;
	stepCt := stepCt + 1;
	tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Cleanup de_snp_copy_number',rowCt,stepCt,'Done');

	insert into deapp.DE_SNP_COPY_NUMBER
	(snp_name, chrom, chrom_pos, copy_number, patient_num)
	select lscn.snp_name, lscn.chrom, lscn.chrom_pos, lscn.copy_number, sm.omic_patient_id as patient_num from
		tm_lz.LT_SNP_COPY_NUMBER lscn
		inner join deapp.de_subject_sample_mapping sm
		on sm.sample_cd = lscn.gsm_num
		where sm.trial_name = TrialID;
	commit;
	stepCt := stepCt + 1;
	tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Fill de_snp_copy_number',rowCt,stepCt,'Done');	
	
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'End i2b2_process_snp_data',0,stepCt,'Done');

	IF newJobFlag = 1
	THEN
		cz_end_audit (jobID, 'SUCCESS');
	END IF;
	
	select 0 into rtn_code from dual;
	
	EXCEPTION
	when missing_platform then
		select 16 into rtn_code from dual;
	when no_probeset_recs then
		cz_write_audit(jobId,databasename,procedurename,'Unable to match probesets to platform in probeset_deapp',1,stepCt,'ERROR');
		CZ_ERROR_HANDLER(JOBID,PROCEDURENAME);
		cz_end_audit (jobId,'FAIL');
		select 16 into rtn_code from dual;
END;
/