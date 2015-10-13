-- Function: i2b2_check_dublicates(character varying, character varying, character varying, character varying, numeric)

-- DROP FUNCTION i2b2_check_dublicates(character varying, character varying, character varying, character varying, numeric);

CREATE OR REPLACE FUNCTION i2b2_check_dublicates(
  trial_id character varying,
  top_node character varying,
  secure_study character varying DEFAULT 'N'::character varying,
  highlight_study character varying DEFAULT 'N'::character varying,
  alwaysSetVisitName character varying DEFAULT 'N'::character varying,
  currentjobid numeric DEFAULT (-1))
  RETURNS numeric AS
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
	databaseName 	VARCHAR(100);
	procedureName 	VARCHAR(100);
	jobID 			numeric(18,0);
	stepCt 			numeric(18,0);
	rowCt			numeric(18,0);
	errorNumber		character varying;
	errorMessage	character varying;
	rtnCd			integer;

	topNode			varchar(2000);
	topLevel		numeric(10,0);
	root_node		varchar(2000);
	root_level		integer;
	study_name		varchar(2000);
	TrialID			varchar(100);
	secureStudy		varchar(200);
	etlDate			timestamp;
	tPath			varchar(2000);
	pCount			integer;
	pExists			integer;
	rtnCode			integer;
	tText			varchar(2000);
	recreateIndexes boolean;
	recreateIndexesSql text;

	addNodes CURSOR is
	select DISTINCT leaf_node, node_name
	from  wt_trial_nodes a;

	--	cursor to define the path for delete_one_node  this will delete any nodes that are hidden after i2b2_create_concept_counts

	delNodes CURSOR is
	select distinct c_fullname
	from  i2b2metadata.i2b2
	where c_fullname like topNode || '%' escape '`'
      and substr(c_visualattributes,2,1) = 'H';

	--	cursor to determine if any leaf nodes exist in i2b2 that are not used in this reload (node changes from text to numeric or numeric to text)

	delUnusedLeaf cursor is
	select l.c_fullname
	from i2b2metadata.i2b2 l
	where l.c_visualattributes like 'L%'
	  and l.c_fullname like topNode || '%' escape '`'
	  and l.c_fullname not in
		 (select t.leaf_node
		  from wt_trial_nodes t
		  union
		  select m.c_fullname
		  from deapp.de_subject_sample_mapping sm
			  ,i2b2metadata.i2b2 m
		  where sm.trial_name = TrialId
		    and sm.concept_code = m.c_basecode
			and m.c_visualattributes like 'L%');
BEGIN

	TrialID := upper(trial_id);
	secureStudy := upper(secure_study);

	databaseName := current_schema();
	procedureName := 'i2b2_check_dublicates';

	--Audit JOB Initialization
	--If Job ID does not exist, then this is a single procedure run and we need to create it
	select case when coalesce(currentjobid, -1) < 1 then cz_start_audit(procedureName, databaseName) else currentjobid end into jobId;

	stepCt := 0;
	stepCt := stepCt + 1;
	tText := 'Start i2b2_check_dublicates for ' || TrialId;
	select cz_write_audit(jobId,databaseName,procedureName,tText,0,stepCt,'Done') into rtnCd;

	if (secureStudy not in ('Y','N') ) then
		secureStudy := 'Y';
	end if;

	topNode := REGEXP_REPLACE('\' || top_node || '\','(\\){2,}', '\', 'g');

	--	figure out how many nodes (folders) are at study name and above
	--	\Public Studies\Clinical Studies\Pancreatic_Cancer_Smith_GSE22780\: topLevel = 4, so there are 3 nodes
	--	\Public Studies\GSE12345\: topLevel = 3, so there are 2 nodes

	select length(topNode)-length(replace(topNode,'\','')) into topLevel;

	if topLevel < 3 then
		stepCt := stepCt + 1;
		select cz_write_audit(jobId,databaseName,procedureName,'Path specified in top_node must contain at least 2 nodes',0,stepCt,'Done') into rtnCd;
		select cz_error_handler (jobID, procedureName, '-1', 'Application raised error') into rtnCd;
		select cz_end_audit (jobID, 'FAIL') into rtnCd;
		return -16;
	end if;

	--	delete any existing data from lz_src_clinical_data and load new data
	begin
	delete from lz_src_clinical_data
	where study_id = TrialId;
	exception
	when others then
		errorNumber := SQLSTATE;
		errorMessage := SQLERRM;
		--Handle errors.
		select cz_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
		--End Proc
		select cz_end_audit (jobID, 'FAIL') into rtnCd;
		return -16;
	get diagnostics rowCt := ROW_COUNT;
	end;
	stepCt := stepCt + 1;
	select cz_write_audit(jobId,databaseName,procedureName,'Delete existing data from lz_src_clinical_data',rowCt,stepCt,'Done') into rtnCd;

	begin
	insert into lz_src_clinical_data
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
	get diagnostics rowCt := ROW_COUNT;
	stepCt := stepCt + 1;
	select cz_write_audit(jobId,databaseName,procedureName,'Insert data into lz_src_clinical_data',rowCt,stepCt,'Done') into rtnCd;

	--	truncate wrk_clinical_data and load data from external file

	execute ('truncate table wrk_clinical_data');

	--	insert data from lt_src_clinical_data to wrk_clinical_data

	begin
	insert into wrk_clinical_data
	(study_id
	,site_id
	,subject_id
	,visit_name
	,data_label
	,data_value
	,category_cd
	,ctrl_vocab_code
	,sample_cd
	)
	select study_id
		  ,site_id
		  ,subject_id
		  ,visit_name
		  ,data_label
		  ,data_value
		  ,category_cd
		  ,ctrl_vocab_code
		  ,sample_cd
	from lt_src_clinical_data;
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
	get diagnostics rowCt := ROW_COUNT;
	stepCt := stepCt + 1;
	select cz_write_audit(jobId,databaseName,procedureName,'Load lt_src_clinical_data to work table',rowCt,stepCt,'Done') into rtnCd;

	-- Get root_node from topNode

	select parse_nth_value(topNode, 2, '\') into root_node;

	select count(*) into pExists
	from i2b2metadata.table_access
	where c_name = root_node;

	select count(*) into pCount
	from i2b2metadata.i2b2
	where c_name = root_node;

	if pExists = 0 or pCount = 0 then
		select i2b2_add_root_node(root_node, jobId) into rtnCd;
	end if;

	select c_hlevel into root_level
	from i2b2metadata.table_access
	where c_name = root_node;

	-- Get study name from topNode

	select parse_nth_value(topNode, topLevel, '\') into study_name;

	--	Add any upper level nodes as needed

	tPath := REGEXP_REPLACE(replace(top_node,study_name,''),'(\\){2,}', '\', 'g');
	select length(tPath) - length(replace(tPath,'\','')) into pCount;

	if pCount > 2 then
		stepCt := stepCt + 1;
		select cz_write_audit(jobId,databaseName,procedureName,'Adding upper-level nodes',0,stepCt,'Done') into rtnCd;
		select i2b2_fill_in_tree(null, tPath, jobId) into rtnCd;
	end if;

	select count(*) into pExists
	from i2b2metadata.i2b2
	where c_fullname = topNode;

	--	add top node for study

	if pExists = 0 then
		select i2b2_add_node(TrialId, topNode, study_name, jobId) into rtnCd;
	end if;

	--	Set data_type, category_path, and usubjid

	update wrk_clinical_data
	set data_type = 'T'
		-- All tag values prefixed with $$, so we should remove prefixes in category_path
		,category_path = replace(replace(replace(category_cd,'_',' '),'+','\'),'\$$','\')
	  ,usubjid = REGEXP_REPLACE(TrialID || ':' || coalesce(site_id,'') || ':' || subject_id,
                   '(::){1,}', ':', 'g');
	 get diagnostics rowCt := ROW_COUNT;
	stepCt := stepCt + 1;
	select cz_write_audit(jobId,databaseName,procedureName,'Set columns in wrk_clinical_data',rowCt,stepCt,'Done') into rtnCd;

	--	Delete rows where data_value is null

	begin
	delete from wrk_clinical_data
	where coalesce(data_value, '') = '';
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
	get diagnostics rowCt := ROW_COUNT;
	stepCt := stepCt + 1;
	select cz_write_audit(jobId,databaseName,procedureName,'Delete null data_values in wrk_clinical_data',rowCt,stepCt,'Done') into rtnCd;

	--Remove Invalid pipes in the data values.
	--RULE: If Pipe is last or first, delete it
	--If it is in the middle replace with a dash

	begin
	update wrk_clinical_data
	set data_value = replace(trim('|' from data_value), '|', '-')
	where data_value like '%|%';
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
	get diagnostics rowCt := ROW_COUNT;
	stepCt := stepCt + 1;
	select cz_write_audit(jobId,databaseName,procedureName,'Remove pipes in data_value',rowCt,stepCt,'Done') into rtnCd;

	--Remove invalid Parens in the data
	--They have appeared as empty pairs or only single ones.

	begin
	update wrk_clinical_data
	set data_value = replace(data_value,'(', '')
	where data_value like '%()%'
	   or data_value like '%( )%'
	   or (data_value like '%(%' and data_value NOT like '%)%');
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
	select cz_write_audit(jobId,databaseName,procedureName,'Remove empty parentheses 1',rowCt,stepCt,'Done') into rtnCd;

	begin
	update wrk_clinical_data
	set data_value = replace(data_value,')', '')
	where data_value like '%()%'
	   or data_value like '%( )%'
	   or (data_value like '%)%' and data_value NOT like '%(%');
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
	select cz_write_audit(jobId,databaseName,procedureName,'Remove empty parentheses 2',rowCt,stepCt,'Done') into rtnCd;

	--Replace the Pipes with Commas in the data_label column
	begin
	update wrk_clinical_data
    set data_label = replace (data_label, '|', ',')
    where data_label like '%|%';
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
	select cz_write_audit(jobId,databaseName,procedureName,'Replace pipes with comma in data_label',rowCt,stepCt,'Done') into rtnCd;

	--	set visit_name to null when there's only a single visit_name for the category

  if alwaysSetVisitName = 'N' then
   begin
    begin
    update wrk_clinical_data tpm
    set visit_name=null
    where (regexp_replace(tpm.category_cd,'\$\$[^+]+','\$\$')) in
        (select regexp_replace(x.category_cd,'\$\$[^+]+','\$\$')
         from wrk_clinical_data x
         group by regexp_replace(x.category_cd,'\$\$[^+]+','\$\$')
         having count(distinct upper(x.visit_name)) = 1);
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
    select cz_write_audit(jobId,databaseName,procedureName,'Set single visit_name to null',rowCt,stepCt,'Done') into rtnCd;
   end;
   else
    stepCt := stepCt + 1;
    select cz_write_audit(jobId,databaseName,procedureName,'Use single visit_name in path',0,stepCt,'Done') into rtnCd;
   end if;
	--	set data_label to null when it duplicates the last part of the category_path
	--	Remove data_label from last part of category_path when they are the same

	begin
	update wrk_clinical_data tpm
	--set data_label = null
	set category_path=substr(tpm.category_path,1,instr(tpm.category_path,'\',-2,1)-1)
	   ,category_cd=substr(tpm.category_cd,1,instr(tpm.category_cd,'+',-2,1)-1)
	where (tpm.category_cd, tpm.data_label) in
		  (select distinct t.category_cd
				 ,t.data_label
		   from wrk_clinical_data t
		   where upper(substr(t.category_path,instr(t.category_path,'\',-1,1)+1,length(t.category_path)-instr(t.category_path,'\',-1,1)))
			     = upper(t.data_label)
		     and t.data_label is not null)
	  and tpm.data_label is not null AND instr(tpm.category_path,'\',-2, 1) > 0 AND instr(tpm.category_cd,'+',-2, 1) > 0;
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
	select cz_write_audit(jobId,databaseName,procedureName,'Set data_label to null when found in category_path',rowCt,stepCt,'Done') into rtnCd;

	--	set visit_name to null if same as data_label

	begin
	update wrk_clinical_data t
	set visit_name=null
	where (t.category_cd, t.visit_name, t.data_label) in
	      (select distinct tpm.category_cd
				 ,tpm.visit_name
				 ,tpm.data_label
		  from wrk_clinical_data tpm
		  where tpm.visit_name = tpm.data_label);
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
	select cz_write_audit(jobId,databaseName,procedureName,'Set visit_name to null when found in data_label',rowCt,stepCt,'Done') into rtnCd;

	--	set visit_name to null if same as data_value

	begin
	update wrk_clinical_data t
	set visit_name=null
	where (t.category_cd, t.visit_name, t.data_value) in
	      (select distinct tpm.category_cd
				 ,tpm.visit_name
				 ,tpm.data_value
		  from wrk_clinical_data tpm
		  where tpm.visit_name = tpm.data_value);
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
	select cz_write_audit(jobId,databaseName,procedureName,'Set visit_name to null when found in data_value',rowCt,stepCt,'Done') into rtnCd;

	-- set visit_name to null if category_path uses terminator and VISITNAME not in path. Avoids duplicates for wt_trial_nodes
	begin
		update wrk_clinical_data t
		set visit_name=null
		where category_path like '%\\$' and category_path not like '%VISITNAME%';

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
	perform cz_write_audit(jobId,databaseName,procedureName,'Set visit_name to null when terminator used and visit_name not in category_path',rowCt,stepCt,'Done');

	--	set visit_name to null if only DATALABEL in category_cd

	-- TR: disabled!!!!
	/*
	begin
	update wrk_clinical_data t
	set visit_name=null
	where t.category_cd like '%DATALABEL%'
	  and t.category_cd not like '%VISITNAME%';
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
	select cz_write_audit(jobId,databaseName,procedureName,'Set visit_name to null when only DATALABE in category_cd',rowCt,stepCt,'Done') into rtnCd; */

	--	change any % to Pct and & and + to ' and ' and _ to space in data_label only

	begin
		update wrk_clinical_data
		set data_label=replace(replace(replace(replace(replace(data_label,'%',' Pct'),'&',' and '),'+',' and '),'_',' '),'(plus)','+')
	   		,data_value=replace(replace(replace(data_value,'%',' Pct'),'&',' and '),'+',' and ')
	   		,category_cd=replace(replace(category_cd,'%',' Pct'),'&',' and ')
	   		,category_path=replace(replace(replace(category_path,'%',' Pct'),'&',' and '),'(plus)','+');

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

	--Trim trailing and leadling spaces as well as remove any double spaces, remove space from before comma, remove trailing comma
	begin
		update wrk_clinical_data
		set data_label  = trim(trailing ',' from trim(replace(replace(data_label,'  ', ' '),' ,',','))),
			data_value  = trim(trailing ',' from trim(replace(replace(data_value,'  ', ' '),' ,',','))),
			--		sample_type = trim(trailing ',' from trim(replace(replace(sample_type,'  ', ' '),' ,',','))),
			visit_name  = trim(trailing ',' from trim(replace(replace(visit_name,'  ', ' '),' ,',',')));
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
	select cz_write_audit(jobId,databaseName,procedureName,'Remove leading, trailing, double spaces',rowCt,stepCt,'Done') into rtnCd;

	--1. DETERMINE THE DATA_TYPES OF THE FIELDS
	--	replaced cursor with update, used temp table to store category_cd/data_label because correlated subquery ran too long

	execute ('truncate table wt_num_data_types');

	begin
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
			having sum(is_numeric(data_value)) = 0;
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
	select cz_write_audit(jobId,databaseName,procedureName,'Insert numeric data into WZ wt_num_data_types',rowCt,stepCt,'Done') into rtnCd;

	begin
		update wrk_clinical_data t
		set data_type='N'
		where exists
		(select 1 from wt_num_data_types x
				where coalesce(t.category_cd,'@') = coalesce(x.category_cd,'@')
							and coalesce(t.data_label,'**NULL**') = coalesce(x.data_label,'**NULL**')
							and coalesce(t.visit_name,'**NULL**') = coalesce(x.visit_name,'**NULL**')
		);
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
	select cz_write_audit(jobId,databaseName,procedureName,'Updated data_type flag for numeric data_types',rowCt,stepCt,'Done') into rtnCd;

	update wrk_clinical_data
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

	get diagnostics rowCt := ROW_COUNT;
	stepCt := stepCt + 1;
	select cz_write_audit(jobId,databaseName,procedureName,'Add if missing DATALABEL, VISITNAME and DATAVALUE to category_path',rowCt,stepCt,'Done') into rtnCd;

	WITH duplicates AS (
		DELETE FROM wrk_clinical_data
		WHERE (subject_id, coalesce(visit_name, '**NULL**'), data_label, category_cd) in (
			SELECT subject_id, coalesce(visit_name, '**NULL**'), data_label, category_cd
			FROM wrk_clinical_data
			GROUP BY subject_id, visit_name, data_label, category_cd
			HAVING count(*) > 1)
		RETURNING *
	)
	INSERT INTO wrk_clinical_data
	SELECT DISTINCT * FROM duplicates;
	get diagnostics rowCt := ROW_COUNT;

	stepCt := stepCt + 1;
	select cz_write_audit(jobId,databaseName,procedureName,'Remove duplicates from wrk_clinical_data',rowCt,stepCt,'Done') into rtnCd;

	--	Check if any duplicate records of key columns (site_id, subject_id, visit_name, data_label, category_cd) for numeric data
	--	exist.  Raise error if yes

	execute ('truncate table wt_clinical_data_dups');

	begin
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
	select cz_write_audit(jobId,databaseName,procedureName,'Check for duplicate key columns',rowCt,stepCt,'Done') into rtnCd;

	if rowCt > 0 then
		stepCt := stepCt + 1;
		select cz_write_audit(jobId,databaseName,procedureName,'Duplicate values found in key columns',0,stepCt,'Done') into rtnCd;
		select cz_error_handler (jobID, procedureName, '-1', 'Application raised error') into rtnCd;
		select cz_end_audit (jobID, 'FAIL') into rtnCd;
		return -16;
	end if;

	stepCt := stepCt + 1;
	select cz_write_audit(jobId,databaseName,procedureName,'End i2b2_check_dublicates',0,stepCt,'Done') into rtnCd;

	---Cleanup OVERALL JOB if this proc is being run standalone
	perform cz_end_audit (jobID, 'SUCCESS') where coalesce(currentJobId, -1) <> jobId;

	return 1;
/*
	EXCEPTION
	WHEN OTHERS THEN
		errorNumber := SQLSTATE;
		errorMessage := SQLERRM;
		--Handle errors.
		select cz_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
		--End Proc
		select cz_end_audit (jobID, 'FAIL') into rtnCd;
		return -16;
*/
END;

$BODY$
  LANGUAGE plpgsql VOLATILE SECURITY DEFINER
  SET search_path FROM CURRENT
  COST 100;

ALTER FUNCTION i2b2_check_dublicates(character varying, character varying, character varying, character varying, character varying, numeric)
  OWNER TO postgres;
