SET DEFINE ON;

DEFINE TM_WZ_SCHEMA='TM_WZ';
DEFINE TM_LZ_SCHEMA='TM_LZ';
DEFINE TM_CZ_SCHEMA='TM_CZ';

create or replace
PROCEDURE  "I2B2_CHECK_DUBLICATES" 
(
  trial_id 			IN	VARCHAR2
 ,top_node			in  varchar2
 ,secure_study		in varchar2 := 'N'
 ,highlight_study	in	varchar2 := 'N'
 ,alwaysSetVisitName in varchar2 := 'N'
 ,currentJobID		IN	NUMBER := null
)
AS
   
  topNode		VARCHAR2(2000);
  topLevel		number(10,0);
  root_node		varchar2(2000);
  root_level	int;
  study_name	varchar2(2000);
  TrialID		varchar2(100);
  secureStudy	varchar2(200);
  etlDate		date;
  tPath			varchar2(2000);
  pCount		int;
  pExists		int;
  rtnCode		int;
  tText			varchar2(2000);
  
    --Audit variables
  newJobFlag INTEGER(1);
  databaseName VARCHAR(100);
  procedureName VARCHAR(100);
  jobID number(18,0);
  stepCt number(18,0);
  
  duplicate_values	exception;
  invalid_topNode	exception;
  MULTIPLE_VISIT_NAMES	EXCEPTION;
  INDEX_NOT_EXISTS EXCEPTION;
  PRAGMA EXCEPTION_INIT(index_not_exists, -1418);
  
  CURSOR addNodes is
  select DISTINCT 
         leaf_node,
    		 node_name
  from  wt_trial_nodes a
  ;
   
	--	cursor to define the path for delete_one_node  this will delete any nodes that are hidden after i2b2_create_concept_counts

	CURSOR delNodes is
	select distinct c_fullname 
	from  i2b2
	where c_fullname like topNode || '%'
      and substr(c_visualattributes,2,1) = 'H';
	  
	--	cursor to determine if any leaf nodes exist in i2b2 that are not used in this reload (node changes from text to numeric or numeric to text)
	  
	cursor delUnusedLeaf is
	select l.c_fullname
	from i2b2 l
	where l.c_visualattributes like 'L%'
	  and l.c_fullname like topNode || '%'
	  --and l.c_fullname not in
	  and not exists
		 (select t.leaf_node 
		  from wt_trial_nodes t WHERE t.leaf_node = l.c_fullname
		  union all
		  select m.c_fullname
		  from de_subject_sample_mapping sm
			  ,i2b2 m
		  where sm.trial_name = TrialId
		    and sm.concept_code = m.c_basecode
			and m.c_visualattributes like 'L%' AND m.c_fullname = l.c_fullname);
BEGIN

	TrialID := upper(trial_id);
	secureStudy := upper(secure_study);
	
	--Set Audit Parameters
	newJobFlag := 0; -- False (Default)
	jobID := currentJobID;

	SELECT sys_context('USERENV', 'CURRENT_SCHEMA') INTO databaseName FROM dual;
	procedureName := $$PLSQL_UNIT;
	
	select sysdate into etlDate from dual;

	--Audit JOB Initialization
	--If Job ID does not exist, then this is a single procedure run and we need to create it
	IF(jobID IS NULL or jobID < 1)
	THEN
		newJobFlag := 1; -- True
		cz_start_audit (procedureName, databaseName, jobID);
	END IF;
    	
	stepCt := 0;

	stepCt := stepCt + 1;
	tText := 'Start I2B2_CHECK_DUBLICATES for ' || TrialId;
	cz_write_audit(jobId,databaseName,procedureName,tText,0,stepCt,'Done');
	
	if (secureStudy not in ('Y','N') ) then
		secureStudy := 'Y';
	end if;
  
  -- added by Eugr: enable parallel queries
  execute immediate 'alter session enable parallel dml';
	
	topNode := REGEXP_REPLACE('\' || top_node || '\','(\\){2,}', '\');
	
	--	figure out how many nodes (folders) are at study name and above
	--	\Public Studies\Clinical Studies\Pancreatic_Cancer_Smith_GSE22780\: topLevel = 4, so there are 3 nodes
	--	\Public Studies\GSE12345\: topLevel = 3, so there are 2 nodes
	
	select length(topNode)-length(replace(topNode,'\','')) into topLevel from dual;
	
	if topLevel < 3 then
		raise invalid_topNode;
	end if;	

	
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Validate secure params',SQL%ROWCOUNT,stepCt,'Done');
	commit;
	
	--	delete any existing data from lz_src_clinical_data and load new data
	
	/*delete from lz_src_clinical_data
	where study_id = TrialId;*/
	
	/*execute immediate('truncate table "&TM_LZ_SCHEMA".lz_src_clinical_data');
	
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Delete existing data from lz_src_clinical_data',SQL%ROWCOUNT,stepCt,'Done');
	commit;
	
	insert *//*+ APPEND *//* into lz_src_clinical_data nologging
	(study_id
	,site_id
	,subject_id
	,visit_name
	,data_label
	,data_value
	,category_cd
	,etl_job_id
	,etl_date
	,ctrl_vocab_code)
	select study_id
		  ,site_id
		  ,subject_id
		  ,visit_name
		  ,data_label
		  ,data_value
		  ,category_cd
		  ,jobId
		  ,etlDate
		  ,ctrl_vocab_code
	from lt_src_clinical_data;
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Insert data into lz_src_clinical_data',SQL%ROWCOUNT,stepCt,'Done');
	commit;*/
		
	--	truncate wrk_clinical_data and load data from external file
	
	execute immediate('truncate table "&TM_WZ_SCHEMA".wrk_clinical_data');
	begin
    execute immediate('drop index "&TM_WZ_SCHEMA"."IDX_WRK_CD"');
  exception
    when index_not_exists then null;
  end;
	
	--	insert data from lt_src_clinical_data to wrk_clinical_data
	-- Optimization: do not insert null data_Value
	insert /*+ APPEND */ into wrk_clinical_data nologging
	(study_id
	,site_id
	,subject_id
	,visit_name
	,data_label
	,data_value
	,category_cd
	,ctrl_vocab_code
  ,category_path
  ,usubjid
  ,data_type
	)
	select study_id
		  ,site_id
		  ,subject_id
		  ,visit_name
		  ,data_label
		  ,data_value
		  ,category_cd
		  ,ctrl_vocab_code
		  -- All tag values prefixed with $$, so we should remove prefixes in category_path
      ,replace(replace(replace(category_cd,'_',' '),'+','\'),'\$$','\')
      ,(CASE WHEN site_id IS NOT NULL THEN TrialID || ':' || site_id || ':' || subject_id ELSE TrialID || ':' || subject_id END)
      ,'T'
	from lt_src_clinical_data
	WHERE data_value is not null;
	
	execute immediate('CREATE INDEX "&TM_WZ_SCHEMA".IDX_WRK_CD ON "&TM_WZ_SCHEMA".WRK_CLINICAL_DATA (DATA_TYPE ASC, DATA_VALUE ASC, VISIT_NAME ASC, DATA_LABEL ASC, CATEGORY_CD ASC, USUBJID ASC)');
	
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Load lt_src_clinical_data to work table',SQL%ROWCOUNT,stepCt,'Done');
	
	commit;  	

	-- Get root_node from topNode
  
	select parse_nth_value(topNode, 2, '\') into root_node from dual;
	
	select count(*) into pExists
	from table_access
	where c_name = root_node;
	
	select count(*) into pCount
	from i2b2
	where c_name = root_node;
	
	if pExists = 0 or pCount = 0 then
		i2b2_add_root_node(root_node, jobId);
	end if;
	
	select c_hlevel into root_level
	from table_access
	where c_name = root_node;
	
	-- Get study name from topNode
  
	select parse_nth_value(topNode, topLevel, '\') into study_name from dual;
	
	--	Add any upper level nodes as needed
	
	tPath := REGEXP_REPLACE(replace(top_node,study_name,null),'(\\){2,}', '\');
	select length(tPath) - length(replace(tPath,'\',null)) into pCount from dual;

	if pCount > 2 then
		stepCt := stepCt + 1;
		cz_write_audit(jobId,databaseName,procedureName,'Adding upper-level nodes',0,stepCt,'Done');
		i2b2_fill_in_tree(null, tPath, jobId);
	end if;
	
/*	Don't delete existing data, concept_cds will be reused
	--	delete any existing data
	
	i2b2_delete_all_nodes(topNode, jobId);
*/

	select count(*) into pExists
	from i2b2
	where c_fullname = topNode;
	
	--	add top node for study
	
	if pExists = 0 then
		i2b2_add_node(TrialId, topNode, study_name, jobId);
	end if;
  
  stepCt := stepCt + 1;
  cz_write_audit(jobId,databaseName,procedureName,'add top node for study',SQL%ROWCOUNT,stepCt,'Done');
  commit;
	--	Set data_type, category_path, and usubjid 
  
	/*update  wrk_clinical_data
	set data_type = 'T'
	   ,category_path = replace(replace(category_cd,'_',' '),'+','\')
	  -- ,usubjid = TrialID || ':' || site_id || ':' || subject_id;
	   ,usubjid = REGEXP_REPLACE(TrialID || ':' || site_id || ':' || subject_id,
                   '(::){1,}', ':'); */
				   
	--21 July 2013. Performace fix by TR. Split into 2 sub queries
  --moved to insert 
--	update /*+ parallel(4) */ wrk_clinical_data
--	set data_type = 'T'
--	   ,category_path = replace(replace(category_cd,'_',' '),'+','\')
--     ,usubjid = TrialID || ':' || subject_id
--	WHERE site_id IS NULL;
--    commit;
	
--  update /*+ parallel(4) */ wrk_clinical_data
--	set data_type = 'T'
--	   ,category_path = replace(replace(category_cd,'_',' '),'+','\')
--     ,usubjid = TrialID || ':' || site_id || ':' || subject_id
--  WHERE site_id IS NOT NULL;
  
	 
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Set columns in wrk_clinical_data',SQL%ROWCOUNT,stepCt,'Done');
	
	commit;
  
	--	Delete rows where data_value is null
    -- we simply do not insert data_value null values
	delete from wrk_clinical_data
	where data_value is null;
	
	
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Delete null data_values in wrk_clinical_data',SQL%ROWCOUNT,stepCt,'Done');
	
	--Remove Invalid pipes in the data values.
	--RULE: If Pipe is last or first, delete it
	--If it is in the middle replace with a dash

	update wrk_clinical_data
	set data_value = replace(trim('|' from data_value), '|', '-')
	where data_value like '%|%';
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Remove pipes in data_value',SQL%ROWCOUNT,stepCt,'Done');
	
	commit;  
  
	--Remove invalid Parens in the data
	--They have appeared as empty pairs or only single ones.
  
	update wrk_clinical_data
	set data_value = replace(data_value,'(', '')
	where data_value like '%()%'
	   or data_value like '%( )%'
	   or (data_value like '%(%' and data_value NOT like '%)%');
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Remove empty parentheses 1',SQL%ROWCOUNT,stepCt,'Done');
	
	update wrk_clinical_data
	set data_value = replace(data_value,')', '')
	where data_value like '%()%'
	   or data_value like '%( )%'
	   or (data_value like '%)%' and data_value NOT like '%(%');
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Remove empty parentheses 2',SQL%ROWCOUNT,stepCt,'Done');
	
	commit;

	--Replace the Pipes with Commas in the data_label column
	update wrk_clinical_data
    set data_label = replace (data_label, '|', ',')
    where data_label like '%|%';
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Replace pipes with comma in data_label',SQL%ROWCOUNT,stepCt,'Done');
	
	commit;

	--	set visit_name to null when there's only a single visit_name for the category_cd
	if alwaysSetVisitName = 'N' then
		update wrk_clinical_data tpm
		set visit_name=null
		where (regexp_replace(tpm.category_cd,'\$\$[^+]+','\$\$')) in
				(select regexp_replace(x.category_cd,'\$\$[^+]+','\$\$')
				 from wrk_clinical_data x
				 -- all tag values started with $$ ($$ will be removed from concept_path),
				 -- concept_cd with different tags should be in same group, so we just replace tag with $$ for grouping
				 group by regexp_replace(x.category_cd,'\$\$[^+]+','\$\$')
				 having count(distinct upper(x.visit_name)) = 1);

		stepCt := stepCt + 1;
		cz_write_audit(jobId,databaseName,procedureName,'Set single visit_name to null',SQL%ROWCOUNT,stepCt,'Done');
  else
    stepCt := stepCt + 1;
    cz_write_audit(jobId,databaseName,procedureName,'Use single visit_name in path',0,stepCt,'Done');
  end if;
  commit;
	
	--	set data_label to null when it duplicates the last part of the category_path
	--	Remove data_label from last part of category_path when they are the same
	update wrk_clinical_data tpm
	--set data_label = null
	set category_path=substr(tpm.category_path,1,instr(tpm.category_path,'\',-2)-1)
		 ,category_cd=substr(tpm.category_cd,1,instr(tpm.category_cd,'+',-2)-1)
	where (tpm.category_cd, tpm.data_label) in
			(select distinct t.category_cd
				 ,t.data_label
			 from wrk_clinical_data t
			 where upper(substr(t.category_path,instr(t.category_path,'\',-1)+1,length(t.category_path)-instr(t.category_path,'\',-1)))
					 = upper(t.data_label)
				 and t.data_label is not null)
		and tpm.data_label is not null;

	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Set data_label to null when found in category_path',SQL%ROWCOUNT,stepCt,'Done');
	commit;

	--	set visit_name to null if same as data_label
	
	update wrk_clinical_data t
	set visit_name=null
	where (t.category_cd, t.visit_name, t.data_label) in
	      (select distinct tpm.category_cd
				 ,tpm.visit_name
				 ,tpm.data_label
		  from wrk_clinical_data tpm
		  where tpm.visit_name = tpm.data_label);

	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Set visit_name to null when found in data_label',SQL%ROWCOUNT,stepCt,'Done');
		
	commit;
	
	--	set visit_name to null if same as data_value
	
	update wrk_clinical_data t
	set visit_name=null
	where (t.category_cd, t.visit_name, t.data_value) in
	      (select distinct tpm.category_cd
				 ,tpm.visit_name
				 ,tpm.data_value
		  from wrk_clinical_data tpm
		  where tpm.visit_name = tpm.data_value);

	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Set visit_name to null when found in data_value',SQL%ROWCOUNT,stepCt,'Done');
		
	commit;

	-- set visit_name to null if category_path uses terminator and VISITNAME not in path. Avoids duplicates for wt_trial_nodes
	update wrk_clinical_data t
	set visit_name=null
	where category_path like '%\$' and category_path not like '%VISITNAME%';

	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Set visit_name to null when terminator used and visit_name not in category_path',SQL%ROWCOUNT,stepCt,'Done');

	commit;
	
	--	set visit_name to null if only DATALABEL in category_cd
  -- EUGR: disabled!!!!!
	
	/*update wrk_clinical_data t
	set visit_name=null
	where t.category_cd like '%DATALABEL%'
	  and t.category_cd not like '%VISITNAME%';

	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Set visit_name to null when only DATALABE in category_cd',SQL%ROWCOUNT,stepCt,'Done');
		
	commit;*/

/*	--	Remove sample_type if found in category_path
	
	update wrk_clinical_data t
	set sample_type = null
	where exists
	     (select 1 from wrk_clinical_data c
		  where instr(c.category_path,t.sample_type) > 0);
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Remove sample_type if already in category_path',SQL%ROWCOUNT,stepCt,'Done');
	commit;
*/
	--	comment out may need later
	
/*	--	change any % to Pct and '&' and + to ' and ' and _ to space in data_label only
	
	update wrk_clinical_data
	set data_label=replace(replace(replace(replace(data_label,'%',' Pct'),'&',' and '),'+',' and '),'_',' ')
	   ,data_value=replace(replace(replace(data_value,'%',' Pct'),'&',' and '),'+',' and ')
	   ,category_cd=replace(replace(category_cd,'%',' Pct'),'&',' and ')
	   ,category_path=replace(replace(category_path,'%',' Pct'),'&',' and ');

  --Trim trailing and leadling spaces as well as remove any double spaces, remove space from before comma, remove trailing comma

	update wrk_clinical_data
	set data_label  = trim(trailing ',' from trim(replace(replace(data_label,'  ', ' '),' ,',','))),
		data_value  = trim(trailing ',' from trim(replace(replace(data_value,'  ', ' '),' ,',','))),
--		sample_type = trim(trailing ',' from trim(replace(replace(sample_type,'  ', ' '),' ,',','))),
		visit_name  = trim(trailing ',' from trim(replace(replace(visit_name,'  ', ' '),' ,',',')));*/
		
	 -- July 2013. Performace fix by TR. Merge into one query
   
	update /*+ parallel(4) */ wrk_clinical_data
	set data_label  = trim(trailing ',' from trim(replace(replace(/**/
				replace(replace(replace(replace(replace(data_label,'%',' Pct'),'&',' and '),'+',' and '),'_',' '),'(plus)','+')
		 /**/   ,'  ', ' '),' ,',',')))
		 ,data_value  = trim(trailing ',' from trim(replace(replace(/**/  replace(replace(replace(data_value,'%',' Pct'),'&',' and '),'+',' and ') /**/  ,'  ', ' '),' ,',',')))
     ,visit_name  = trim(trailing ',' from trim(replace(replace(visit_name,'  ', ' '),' ,',',')))
     ,category_cd=replace(replace(category_cd,'%',' Pct'),'&',' and ')
	   ,category_path=replace(replace(replace(category_path,'%',' Pct'),'&',' and '),'(plus)','+')
    ;	
		
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Remove leading, trailing, double spaces',SQL%ROWCOUNT,stepCt,'Done');

	commit;

-- determine numeric data types

	execute immediate('truncate table "&TM_WZ_SCHEMA".wt_num_data_types');
  
	insert into wt_num_data_types
	(category_cd
	,data_label
	,visit_name
	)
    select category_cd,
           data_label,
           visit_name
    from wrk_clinical_data
    where data_value is not null
    group by category_cd
	        ,data_label
            ,visit_name
      having sum(is_number(data_value)) = 0;
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Insert numeric data into WZ wt_num_data_types',SQL%ROWCOUNT,stepCt,'Done');
	
	commit;

	update wrk_clinical_data t
	set data_type='N'
	where exists
	     (select 1 from wt_num_data_types x
	      where nvl(t.category_cd,'@') = nvl(x.category_cd,'@')
			and nvl(t.data_label,'**NULL**') = nvl(x.data_label,'**NULL**')
			and nvl(t.visit_name,'**NULL**') = nvl(x.visit_name,'**NULL**')
		  );
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Updated data_type flag for numeric data_types',SQL%ROWCOUNT,stepCt,'Done');

	commit;


	update /*+ parallel(4) */ wrk_clinical_data
	set category_path =
		case
			-- Path with terminator, don't change, just remove terminator
			when category_path like '%\$'
			then substr(category_path, 1, length(category_path) - 2)
			-- Add missing fields to concept_path
			else
				case
					when category_path like '%\VISITNFST' then replace(category_path, '\VISITNFST', '')
					else category_path
				end ||
				case
					when category_path not like '%DATALABEL%' then '\DATALABEL'
					else ''
				end ||
				case
					when category_path like '%\VISITNFST' then '\VISITNAME'
					else ''
				end ||
				case
					when data_type = 'T' and category_path not like '%DATAVALUE%' then '\DATAVALUE'
					else ''
				end ||
				case
					when category_path not like '%\VISITNFST' and category_path not like '%VISITNAME%' then '\VISITNAME'
					else ''
				end
		end;

	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Add if missing DATALABEL, VISITNAME and DATAVALUE to category_path',SQL%ROWCOUNT,stepCt,'Done');

	commit;

	delete from /*+ parallel(4) */ wrk_clinical_data
 	where rowid IN (
 		select rid
 		from (
 			select rowid rid, row_number() over (
				partition by subject_id, visit_name, data_label, category_cd order by rowid
			) rn from wrk_clinical_data
 		) where rn <> 1
 	);

	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Remove duplicates from wrk_clinical_data',SQL%ROWCOUNT,stepCt,'Done');

	commit;

	--	Check if any duplicate records of key columns (site_id, subject_id, visit_name, data_label, category_cd) for numeric data
	--	exist.  Raise error if yes
	
	execute immediate('truncate table "&TM_WZ_SCHEMA".wt_clinical_data_dups');
	
	insert into wt_clinical_data_dups
	(site_id
	,subject_id
	,visit_name
	,data_label
	,category_cd)
	select w.site_id, w.subject_id, w.visit_name, w.data_label, w.category_cd
	from wrk_clinical_data w
	where exists
		 (select 1 from wt_num_data_types t
		 where coalesce(w.category_cd,'@') = coalesce(t.category_cd,'@')
		   and coalesce(w.data_label,'@') = coalesce(t.data_label,'@')
		   and coalesce(w.visit_name,'@') = coalesce(t.visit_name,'@')
		  )
	group by w.site_id, w.subject_id, w.visit_name, w.data_label, w.category_cd
	having count(*) > 1;
		  
	pCount := SQL%ROWCOUNT;
		  
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Check for duplicate key columns',pCount,stepCt,'Done');
			  
	if pCount > 0 then
		raise duplicate_values;
	end if;
	
		stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'End I2B2_CHECK_DUBLICATES',0,stepCt,'Done');
	
    ---Cleanup OVERALL JOB if this proc is being run standalone
	if newJobFlag = 1
	then
		cz_end_audit (jobID, 'SUCCESS');
	end if;

	rtnCode := 0;
  
	exception
	when duplicate_values then
		stepCt := stepCt + 1;
		cz_write_audit(jobId,databaseName,procedureName,'Duplicate values found in key columns',0,stepCt,'Done');	
		cz_error_handler (jobID, procedureName);
		cz_end_audit (jobID, 'FAIL');
		rtnCode := 16;		
	when invalid_topNode then
		stepCt := stepCt + 1;
		cz_write_audit(jobId,databaseName,procedureName,'Path specified in top_node must contain at least 2 nodes',0,stepCt,'Done');	
		cz_error_handler (jobID, procedureName);
		cz_end_audit (jobID, 'FAIL');
		rtnCode := 16;	
	when multiple_visit_names then
		stepCt := stepCt + 1;
		cz_write_audit(jobId,databaseName,procedureName,'Not for all subject_id/category/label/value visit names specified. Visit names should be all empty or specified for all records.',0,stepCt,'Done');
		cz_error_handler (jobID, procedureName);
		cz_end_audit (jobID, 'FAIL');
		rtnCode := 16;
	when others then
    --Handle errors.
		cz_error_handler (jobID, procedureName);
    --End Proc
		cz_end_audit (jobID, 'FAIL');
		rtnCode := 16;
	
END;
/
