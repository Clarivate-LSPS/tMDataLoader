create or replace
PROCEDURE "I2B2_LOAD_STUDY_METADATA" 
(
  currentJobID NUMBER := null
)
AS
	--	NOTE****	The CleanCell macro must be run for all the data in Dataset_Explorer_MetaData.xls file before it is saved to a text file!!  CleanCell will remove
	--				any embedded line feeds in the data.

	-- JEA@20110720	New, cloned for tranSMART consortia

  
	--Audit variables
	newJobFlag INTEGER(1);
	databaseName VARCHAR(100);
	procedureName VARCHAR(100);
	jobID number(18,0);
	stepCt number(18,0);
	
	dcount 				int;
	lcount 				int;
	tmp_compound		varchar2(200);
	tmp_disease			varchar2(200);
	tmp_pubmed			varchar2(200);
	tmp_organism		varchar2(200);
	pubmed_id			  varchar2(200);
	pubmed_title		varchar2(200);
	tag_path				varchar(400);
	etl_program_id 	int;
	study_folder_id int;
	study_phase_tag_item_id int;
	
	Type study_compound_rec is record
	(study_id	varchar2(200)
	,compound	varchar2(500)
	);
  
	Type study_compound_tab is table of study_compound_rec;
  
	study_compound_array study_compound_tab;
  
	Type study_disease_rec is record
	(study_id	varchar2(200)
	,disease	varchar2(500)
	);
  
	Type study_disease_tab is table of study_disease_rec;
  
	study_disease_array study_disease_tab;
  
	Type study_pubmed_rec is record
	(study_id	varchar2(200)
	,pubmed	varchar2(500)
	);
  
	Type study_pubmed_tab is table of study_pubmed_rec;
  
	study_pubmed_array study_pubmed_tab;

	Type study_organism_rec is record
	(study_id	varchar2(200)
	,organism	varchar2(500)
	);

	Type study_organism_tab is table of study_organism_rec;

	study_organism_array study_organism_tab;

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
  
	stepCt := 0;
  
 --	figure out study_type
  /* select sourcesystem_cd, c_fullname, parse_nth_value(c_fullname,2,'\') as study_type from i2b2
where c_hlevel = 0
order by c_fullname
*/

	-- create etl program if necessary
	begin
		select folder_id
		  into etl_program_id
		  from fmapp.fm_folder
		 where folder_name = 'etl-program'
		   and folder_type = 'PROGRAM';
		exception
		when no_data_found then
		etl_program_id := null;
	end;

	if (etl_program_id is null) then
		insert into fmapp.fm_folder (folder_id, folder_name, folder_level, folder_type, active_ind, description)
			 values (fmapp.seq_fm_id.nextval,'etl-program', 0, 'PROGRAM', 1,
						'Special program. Create automatically when tmDataloader load metadata for study. ' ||
						'Necessary for support study filters')
		  returning folder_id
			   into etl_program_id ;

		stepCt := stepCt + 1;
		cz_write_audit(jobId,databaseName,procedureName,'Create etl program',SQL%ROWCOUNT,stepCt,'Done');
		commit;
	end if;

	--	Update existing bio_experiment data
	
	update biomart.bio_experiment b
	set (title
	    ,description
		,design
		,completion_date
		,primary_investigator
		,overall_design
		,institution
		,biomarker_type
		,access_type) =
	    (select m.title
		       ,m.description
			   ,case when m.design is null then null
				else 'STUDY_DESIGN:' || upper(m.design) end as design
			   ,decode(is_date(m.completion_date,'YYYYMMDD'),1,null,to_date(m.completion_date,'YYYYMMDD'))
			   ,m.primary_investigator
			   ,substr(decode(m.primary_end_points,null,null,'N/A',null,m.primary_end_points) ||
					    decode(m.inclusion_criteria,null,null,'N/A',null,' Inclusion Criteria: ' || m.inclusion_criteria) ||
						decode(m.exclusion_criteria,null,null,'N/A',null,' Exclusion Criteria: ' || m.exclusion_criteria),1,2000)
				,case when m.institution is null then null
				 else 'STUDY_INSTITUTION:' ||
							upper(replace(m.institution, ' ', '_')) end as institution
				,case when m.biomarker_type is null then null
				 else 'STUDY_BIOMARKER_TYPE:' ||
							upper(replace(m.biomarker_type, ' ', '_')) end as biomarker_type
				,case when m.access_type is null then null
				 else 'STUDY_ACCESS_TYPE:' ||
							upper(replace(m.access_type, ' ', '_')) end as access_type
		 from lt_src_study_metadata m
		 where m.study_id is not null
		   and b.accession = m.study_id)
	where exists
		(select 1 from lt_src_study_metadata x
		 where b.accession = x.study_id
		   and b.etl_id = 'METADATA:' || x.study_id
		   and x.study_id is not null)
	;
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Updated trial data in BIOMART bio_experiment',SQL%ROWCOUNT,stepCt,'Done');
	commit;
	
	--	Add new trial data to bio_experiment
	
	insert into biomart.bio_experiment
	(bio_experiment_type
	,title
	,description
	,design
	,start_date
	,completion_date
	,primary_investigator
	,contact_field
	,etl_id
	,status
	,overall_design
	,accession
	,institution
	,biomarker_type
	,access_type)
	select 'Experiment'
	      ,m.title
		  ,m.description
		  ,case when m.design is null then null
		   else 'STUDY_DESIGN:' || upper(m.design) end as design
		  ,decode(is_date(m.start_date,'YYYYMMDD'),1,null,to_date(m.start_date,'YYYYMMDD'))
		  ,decode(is_date(m.completion_date,'YYYYMMDD'),1,null,to_date(m.completion_date,'YYYYMMDD'))
		  ,m.primary_investigator
		  ,m.contact_field
		  ,'METADATA:' || M.STUDY_ID -- commented by Eugr below
		  ,m.status
		  ,NULL/*decode(m.primary_end_points,null,null,'N/A',null,replace(m.primary_end_points,'"',null)) ||
					    decode(m.inclusion_criteria,null,null,'N/A',null,' Inclusion Criteria: ' || replace(m.inclusion_criteria,'"',null)) ||
						decode(m.exclusion_criteria,null,null,'N/A',null,' Exclusion Criteria: ' || replace(m.exclusion_criteria,'"',null))*/
		  ,m.study_id
		  ,case when m.institution is null then null
		   else 'STUDY_INSTITUTION:' || 
					upper(replace(m.institution, ' ', '_')) end as institution
		  ,case when m.biomarker_type is null then null
		   else 'STUDY_BIOMARKER_TYPE:' ||
					upper(replace(m.biomarker_type, ' ', '_')) end as biomarker_type
		  ,case when m.access_type is null then null
		   else 'STUDY_ACCESS_TYPE:' ||
					upper(replace(m.access_type, ' ', '_')) end as access_type
	from lt_src_study_metadata m
	where m.study_id is not null
	  and not exists
	      (select 1 from biomart.bio_experiment x
		   where m.study_id = x.accession
		     and m.study_id is not null)
	;
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Inserted trial data in BIOMART bio_experiment',SQL%ROWCOUNT,stepCt,'Done');
	commit;
	
	--	Insert new trial into bio_data_uid
	
	insert into biomart.bio_data_uid
	(bio_data_id
	,unique_id
	,bio_data_type
	)
	select distinct b.bio_experiment_id
	      ,'EXP:' || m.study_id
		  ,'EXP'
	from biomart.bio_experiment b
		,lt_src_study_metadata m
	where m.study_id is not null
	  and m.study_id = b.accession
	  and not exists
	      (select 1 from biomart.bio_data_uid x
		   where x.unique_id = 'EXP:' || m.study_id)
	;
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Inserted trial data into BIOMART bio_data_uid',SQL%ROWCOUNT,stepCt,'Done');
	commit;
	
	-- Create study folder
	for bio_experiment_rec in (select dat.unique_id, exp.title, exp.description, met.study_phase
								 from biomart.bio_experiment exp, lt_src_study_metadata met, biomart.bio_data_uid dat
								where exp.accession = met.study_id
								  and exp.bio_experiment_id = dat.bio_data_id
								  and not exists (select 1
													from fmapp.fm_folder_association
												   where object_uid = dat.unique_id)) loop

		insert into fmapp.fm_folder (folder_id, folder_name, folder_level, folder_type, active_ind, parent_id, description)
			 values (fmapp.seq_fm_id.nextval, bio_experiment_rec.title, 1, 'STUDY', 1, etl_program_id, bio_experiment_rec.description)
		  returning folder_id
			   into study_folder_id;

		insert into fmapp.fm_folder_association (folder_id, object_uid, object_type)
			 values (study_folder_id, bio_experiment_rec.unique_id, 'org.transmart.biomart.Experiment');

		begin
			select tag_item_id
			  into study_phase_tag_item_id
			  from amapp.am_tag_item
			 where code_type_name = 'STUDY_PHASE';
			exception
			when no_data_found then
			study_phase_tag_item_id := null;
		end;

		if (bio_experiment_rec.study_phase is not null and study_phase_tag_item_id is not null) then
			select count(object_uid)
			  into lcount
			  from amapp.am_tag_association
			 where subject_uid = 'FOL:' || study_folder_id
			   and tag_item_id = study_phase_tag_item_id;

			if (lcount = 0) then
				insert into amapp.am_tag_association (subject_uid, object_uid, object_type, tag_item_id)
					 values ('FOL:' || study_folder_id, 
							'STUDY_PHASE:' || upper(replace(bio_experiment_rec.study_phase, ' ', '_')), 
							'BIO_CONCEPT_CODE', study_phase_tag_item_id);
			else
				update amapp.am_tag_association
				   set object_uid =  'STUDY_PHASE:' || upper(replace(bio_experiment_rec.study_phase, ' ', '_'))
				 where subject_uid = 'FOL:' || study_folder_id
				   and tag_item_id = study_phase_tag_item_id;
			end if;
		end if;

		stepCt := stepCt + 1;
		cz_write_audit(jobId,databaseName,procedureName,'Add study folder:' || bio_experiment_rec.title,SQL%ROWCOUNT,stepCt,'Done');
		commit;
	end loop;

	--	delete existing compound data for study, compound list may change
	
	delete from bio_data_compound dc 
    where exists (select dc.bio_data_id
      from bio_experiment x, lt_src_study_metadata y 
      where x.accession = y.study_id
      and x.etl_id = 'METADATA:' || y.study_id
      and dc.bio_data_id=x.bio_experiment_id)
  ;

	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Delete existing data from bio_data_compound',SQL%ROWCOUNT,stepCt,'Done');
	commit;

	select distinct study_id, compound
	bulk collect into study_compound_array
	from lt_src_study_metadata
	where compound is not null;
	
	if SQL%ROWCOUNT > 0 then 
		for i in study_compound_array.first .. study_compound_array.last
		loop
		
			select length(study_compound_array(i).compound) -
				   length(replace(study_compound_array(i).compound,';',null))+1
				into dcount
			from dual;
	 
			while dcount > 0
			Loop	
		
				select parse_nth_value(study_compound_array(i).compound,dcount,';') into tmp_compound
				from dual;
				   
				--	add new compound
				
				insert into bio_compound bc
				(generic_name)
				select tmp_compound
				from dual
				where not exists
					 (select 1 from bio_compound x
					  where upper(x.generic_name) = upper(tmp_compound))
				  and tmp_compound is not null;
				stepCt := stepCt + 1;
				cz_write_audit(jobId,databaseName,procedureName,'Added compound to bio_compound',SQL%ROWCOUNT,stepCt,'Done');
				commit;
							
				--	Insert new trial data into bio_data_compound

				insert into bio_data_compound
				(bio_data_id
				,bio_compound_id
				,etl_source
				)
				select b.bio_experiment_id
					  ,c.bio_compound_id
					  ,'METADATA:' || study_compound_array(i).study_id
				from biomart.bio_experiment b
					,biomart.bio_compound c
				where upper(tmp_compound) = upper(c.generic_name) 
				  and tmp_compound is not null
				  and b.accession = study_compound_array(i).study_id
				  and not exists
						 (select 1 from biomart.bio_data_compound x
						  where b.bio_experiment_id = x.bio_data_id
							and c.bio_compound_id = x.bio_compound_id);

				stepCt := stepCt + 1;
				cz_write_audit(jobId,databaseName,procedureName,'Inserted trial data in BIOMART bio_data_compound',SQL%ROWCOUNT,stepCt,'Done');
				commit;
				
				dcount := dcount - 1;
			end loop;
		end loop;
	end if;

	--	delete existing disease data for studies
	
	delete from bio_data_disease dc 
    where exists (select dc.bio_data_id
      from bio_experiment x, lt_src_study_metadata y 
      where x.accession = y.study_id
      and x.etl_id = 'METADATA:' || y.study_id
      and dc.bio_data_id=x.bio_experiment_id)
  ;

	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Delete existing data from bio_data_disease',SQL%ROWCOUNT,stepCt,'Done');
	commit;

	select distinct study_id, disease
	bulk collect into study_disease_array
	from lt_src_study_metadata
	where disease is not null;
	
	if SQL%ROWCOUNT > 0 then 
		for i in study_disease_array.first .. study_disease_array.last
		loop
		
			select length(study_disease_array(i).disease) -
				   length(replace(study_disease_array(i).disease,';',null))+1
				into dcount
			from dual;
	 
			while dcount > 0
			Loop	
		
				select parse_nth_value(study_disease_array(i).disease,dcount,';') into tmp_disease
				from dual;
				   
				--	add new disease
				
				insert into bio_disease bc
				(disease
				,prefered_name)
				select tmp_disease
					  ,tmp_disease
				from dual
				where not exists
					 (select 1 from bio_disease x
					  where upper(x.disease) = upper(tmp_disease))
				  and tmp_compound is not null;
				stepCt := stepCt + 1;
				cz_write_audit(jobId,databaseName,procedureName,'Added disease to bio_disease',SQL%ROWCOUNT,stepCt,'Done');
				commit;
							
				--	Insert new trial data into bio_data_disease

				insert into bio_data_disease
				(bio_data_id
				,bio_disease_id
				,etl_source
				)
				select b.bio_experiment_id
					  ,c.bio_disease_id
					  ,'METADATA:' || study_disease_array(i).study_id
				from biomart.bio_experiment b
					,biomart.bio_disease c
				where upper(tmp_disease) = upper(c.disease) 
				  and tmp_disease is not null
				  and b.accession = study_disease_array(i).study_id
				  and not exists
						 (select 1 from biomart.bio_data_disease x
						  where b.bio_experiment_id = x.bio_data_id
							and c.bio_disease_id = x.bio_disease_id);

				stepCt := stepCt + 1;
				cz_write_audit(jobId,databaseName,procedureName,'Inserted trial data in BIOMART bio_data_disease',SQL%ROWCOUNT,stepCt,'Done');
				commit;
				
				dcount := dcount - 1;
			end loop;
		end loop;
	end if;

	--	delete existing taxonomy data for studies

	delete from biomart.bio_data_taxonomy dc
	 where dc.bio_data_id in
	 (select x.bio_experiment_id
		  from biomart.bio_experiment x, lt_src_study_metadata y
		 where x.accession = y.study_id
		   and x.etl_id = 'METADATA:' || y.study_id);

	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Delete existing data from bio_data_taxonomy',SQL%ROWCOUNT,stepCt,'Done');
	commit;

	-- add study organism to taxonomy

	select distinct study_id, organism
	bulk collect into study_organism_array
	from lt_src_study_metadata
	where organism is not null;

	if SQL%ROWCOUNT > 0 then
		for i in study_organism_array.first .. study_organism_array.last
		loop
			select length(study_organism_array(i).organism) -
						 length(replace(study_organism_array(i).organism,';',null))+1
			into dcount
			from dual;

			while dcount > 0
			loop
				select parse_nth_value(study_organism_array(i).organism,dcount,';') into tmp_organism
				from dual;

				--	add new organism
				insert into biomart.bio_taxonomy
					(taxon_name
						,taxon_label)
				select tmp_organism
							,tmp_organism
				  from dual
				 where not exists
					(select 1 from biomart.bio_taxonomy x
					  where upper(x.taxon_name) = upper(tmp_organism))
					    and tmp_organism is not null;
				stepCt := stepCt + 1;
				cz_write_audit(jobId,databaseName,procedureName,'Added organism to bio_taxonomy',SQL%ROWCOUNT,stepCt,'Done');
				commit;

				--	Insert new trial data into bio_data_taxonomy
				insert into biomart.bio_data_taxonomy
					(bio_data_id
						,bio_taxonomy_id
						,etl_source
					)
						select b.bio_experiment_id
								,c.bio_taxonomy_id
								,'METADATA:' || study_organism_array(i).study_id
						  from biomart.bio_experiment b
									,biomart.bio_taxonomy c
						 where upper(tmp_organism) = upper(c.taxon_name)
							 and tmp_organism is not null
							 and b.accession = study_organism_array(i).study_id
							 and not exists
							  (select 1 from biomart.bio_data_taxonomy x
								  where b.bio_experiment_id = x.bio_data_id
									  and c.bio_taxonomy_id = x.bio_taxonomy_id);

				stepCt := stepCt + 1;
				cz_write_audit(jobId,databaseName,procedureName,'Inserted trial data in BIOMART bio_data_taxonomy',SQL%ROWCOUNT,stepCt,'Done');
				commit;

				dcount := dcount - 1;
			end loop;
		end loop;
	end if;
	
	--	add ncbi/GEO linking
	
	--	check if ncbi exists in bio_content_repository, if not, add
	
	select count(*) into dcount
	from bio_content_repository
	where repository_type = 'NCBI'
	  and location_type = 'URL';
	
	if dcount = 0 then
		insert into bio_content_repository
		(location
		,active_y_n
		,repository_type
		,location_type) 
		values ('http://www.ncbi.nlm.nih.gov/','Y','NCBI','URL');
		
		stepCt := stepCt + 1;
		cz_write_audit(jobId,databaseName,procedureName,'Inserted NCBI URL in bio_content_repository',SQL%ROWCOUNT,stepCt,'Done');
		commit;
		
	end if;
	
	--	insert GSE studies into bio_content
	
	insert into bio_content
	(repository_id
	,location
	,file_type
	,etl_id_c
	)
	select bcr.bio_content_repo_id
		  ,'geo/query/acc.cgi?acc=' || m.study_id
		  ,'Experiment Web Link'
		  ,'METADATA:' || m.study_id
	from lt_src_study_metadata m
		,bio_content_repository bcr
	where m.study_id like 'GSE%'
	  and bcr.repository_type = 'NCBI'
	  and bcr.location_type = 'URL'
	  and not exists
		 (select 1 from bio_content x
		  where x.etl_id_c like '%' || m.study_id || '%'
		    and x.file_type = 'Experiment Web Link'
			and x.location = 'geo/query/acc.cgi?acc=' || m.study_id);

	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Inserted GEO study into bio_content',SQL%ROWCOUNT,stepCt,'Done');
	commit;
	
	--	insert GSE studies into bio_content_reference
	
	insert into bio_content_reference
	(bio_content_id
	,bio_data_id
	,content_reference_type
	,etl_id_c
	)
	select bc.bio_file_content_id
		  ,be.bio_experiment_id
		  ,'Experiment Web Link'
		  ,'METADATA:' || m.study_id
	from lt_src_study_metadata m
		,bio_experiment be
		,bio_content bc
	where m.study_id like 'GSE%'
	  and m.study_id = be.accession
	  and bc.file_type = 'Experiment Web Link'
	  and bc.etl_id_c = 'METADATA:' || m.study_id
	  and bc.location = 'geo/query/acc.cgi?acc=' || m.study_id
	  and not exists
		 (select 1 from bio_content_reference x
		  where bc.bio_file_content_id = x.bio_content_id
		    and be.bio_experiment_id = x.bio_data_id);

	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Inserted GEO study into bio_content_reference',SQL%ROWCOUNT,stepCt,'Done');
	commit;

	--	add PUBMED linking
	
	select distinct study_id, pubmed_ids
	bulk collect into study_pubmed_array
	from lt_src_study_metadata
	where pubmed_ids is not null;
	
	if SQL%ROWCOUNT > 0 then
		--	check if PubMed url exists in bio_content_repository, if not, add
		select count(*) into dcount
		from bio_content_repository
		where repository_type = 'PubMed';	
	
		if dcount = 0 then
			insert into bio_content_repository
			(location
			,active_y_n
			,repository_type
			,location_type) 
			values ('http://www.ncbi.nlm.nih.gov/pubmed/','Y','PubMed','URL');
			stepCt := stepCt + 1;
			cz_write_audit(jobId,databaseName,procedureName,'Inserted GEO study into bio_content_reference',SQL%ROWCOUNT,stepCt,'Done');
			commit;
		end if;

		for i in study_pubmed_array.first .. study_pubmed_array.last
		loop
			select length(study_pubmed_array(i).pubmed)-length(replace(study_pubmed_array(i).pubmed,';',null))+1
			into dcount
			from dual;
 
			while dcount > 0
			Loop	
				-- multiple pubmed id can be separated by ;, pubmed id and title are separated by :
				
				select parse_nth_value(study_pubmed_array(i).pubmed,dcount,';') into tmp_pubmed from dual;			
				select instr(tmp_pubmed,':') into lcount from dual;
				
				if lcount = 0 then
					pubmed_id := tmp_pubmed;
					pubmed_title := null;
				else
					pubmed_id := substr(tmp_pubmed,1,instr(tmp_pubmed,':')-1);
					cz_write_audit(jobId,databaseName,procedureName,'pubmed_id: ' || pubmed_id,1,stepCt,'Done');	
					pubmed_title := substr(tmp_pubmed,instr(tmp_pubmed,':')+1);
					cz_write_audit(jobId,databaseName,procedureName,'pubmed_title: ' || pubmed_title,1,stepCt,'Done');
				end if;
	
				insert into bio_content
				(repository_id
				,location
				,title
				,file_type
				,etl_id_c
				)
				select bcr.bio_content_repo_id
					  ,pubmed_id
					  ,pubmed_title
					  ,'Publication Web Link'
					  ,'METADATA:' || study_pubmed_array(i).study_id
				from bio_content_repository bcr
				where bcr.repository_type = 'PubMed'
				  and not exists
					 (select 1 from bio_content x
					  where x.etl_id_c like '%' || study_pubmed_array(i).study_id || '%'
					    and x.file_type = 'Publication Web Link'
						and x.location = pubmed_id);

				stepCt := stepCt + 1;
				cz_write_audit(jobId,databaseName,procedureName,'Inserted pubmed for study into bio_content',SQL%ROWCOUNT,stepCt,'Done');
				commit;				
		
				insert into bio_content_reference
				(bio_content_id
				,bio_data_id
				,content_reference_type
				,etl_id_c
				)
				select bc.bio_file_content_id
					  ,be.bio_experiment_id
					  ,'Publication Web Link'
					  ,'METADATA:' || study_pubmed_array(i).study_id
				from bio_experiment be
					,bio_content bc
				where be.accession = study_pubmed_array(i).study_id
				  and bc.file_type = 'Publication Web Link'
				  and bc.etl_id_c = 'METADATA:' || study_pubmed_array(i).study_id
				  and bc.location = pubmed_id
				  and not exists
					 (select 1 from bio_content_reference x
					  where bc.bio_file_content_id = x.bio_content_id
						and be.bio_experiment_id = x.bio_data_id);	

				stepCt := stepCt + 1;
				cz_write_audit(jobId,databaseName,procedureName,'Inserted pubmed for study into bio_content_reference',SQL%ROWCOUNT,stepCt,'Done');
				commit;				
	
				dcount := dcount - 1;
			end loop;
		end loop;
	end if;

	-- Insert trial tags
	select min(b.c_fullname)
	  into tag_path
	  from lt_src_study_metadata m,i2b2 b
	 where m.study_id = b.sourcesystem_cd
		 and m.study_id is not null;

	delete from i2b2_tags t
	 where t.tag_type in ('Trial', 'Compound', 'Disease')
	   and t.path = tag_path;

	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Delete existing tags in i2b2_tags',SQL%ROWCOUNT,stepCt,'Done');
	commit;
	
	-- PP: Some pre-1.1 version systems of tranSMART have Trial as tags_idx=0 and some have tags_idx=1.  There was a change
  -- in the tranSMART Application source code which refer 0 or 1 depending on which version of tranSMART is used.  The 
  -- tranSMART Application fails to show MetaData when it doesn't have what it is expecting, to avoid this issue, we will 
  -- add a row for both 0 and 1.  This duplication does not harm the application.
  insert into i2b2_tags
	(path, tag, tag_type, tags_idx)
	select tag_path as path
		  ,be.accession as tag
		  ,'Trial' as tag_type
		  ,0 as tags_idx
	from bio_experiment be
		,i2b2 b
	where be.accession = b.sourcesystem_cd
				and b.c_fullname = tag_path
	group by be.accession;
	
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Add Trial tags in i2b2_tags',SQL%ROWCOUNT,stepCt,'Done');
	commit;
					 
	--	Insert trial data tags - COMPOUND
	
	insert into i2b2_tags
	(path, tag, tag_type, tags_idx)
	select distinct tag_path as path
		  ,coalesce(c.generic_name,c.brand_name) as tag
		  ,'Compound' as tag_type
		  ,1 as tags_idx
	from bio_experiment be
		,bio_data_compound bc
		,bio_compound c
		,i2b2 o
	where be.bio_experiment_id = bc.bio_data_id
       and bc.bio_compound_id = c.bio_compound_id
       and be.accession = o.sourcesystem_cd
       and coalesce(c.generic_name,c.brand_name) is not null
				and o.c_fullname = tag_path
	group by coalesce(c.generic_name,c.brand_name);

	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Insert Compound tags in I2B2METADATA i2b2_tags',SQL%ROWCOUNT,stepCt,'Done');
	commit;	
					 
	--	Insert trial data tags - DISEASE

	insert into i2b2_tags
	(path, tag, tag_type, tags_idx)
	select distinct tag_path as path
		   ,c.prefered_name
		   ,'Disease' as tag_type
		   ,2 as tags_idx
	from bio_experiment be
		,bio_data_disease bc
		,bio_disease c
		,i2b2 o
	where be.bio_experiment_id = bc.bio_data_id
      and bc.bio_disease_id = c.bio_disease_id
      and be.accession = o.sourcesystem_cd
			and o.c_fullname = tag_path
	group by c.prefered_name;

	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Insert Disease tags in I2B2METADATA i2b2_tags',SQL%ROWCOUNT,stepCt,'Done');
	commit;	
	
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'End i2b2_load_study_metadata',SQL%ROWCOUNT,stepCt,'Done');
	commit;

    ---Cleanup OVERALL JOB if this proc is being run standalone
	IF newJobFlag = 1
	THEN
		cz_end_audit (jobID, 'SUCCESS');
	END IF;

	EXCEPTION
	WHEN OTHERS THEN
		--Handle errors.
		cz_error_handler (jobID, procedureName);
		
		--End Proc
		cz_end_audit (jobID, 'FAIL');
	
END;
/

/*	ignore for now
	--	Add trial/study to search_secure_object
	
	insert into searchapp.search_secure_object
	(bio_data_id
	,display_name
	,data_type
	,bio_data_unique_id
	)
	select b.bio_experiment_id
	      ,parse_nth_value(md.c_fullname,2,'\') || ' - ' || b.accession as display_name
		  ,'BIO_CLINICAL_TRIAL' as data_type
		  ,'EXP:' || b.accession as bio_data_unique_id
	from i2b2metadata.i2b2 md
		,biomart.bio_experiment b
	where b.accession = md.sourcesystem_cd
	  and md.c_hlevel = 0
	  and md.c_fullname not like '\Public Studies\%'
	  and md.c_fullname not like '\Internal Studies\%'
	  and md.c_fullname not like '\Experimental Medicine Study\NORMALS\%'
	  and not exists
		 (select 1 from searchapp.search_secure_object so
		  where b.bio_experiment_id = so.bio_data_id)
	;
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Inserted new trial/study into SEARCHAPP search_secure_object',SQL%ROWCOUNT,stepCt,'Done');
	commit;
*/


/*	not used	
	--	Insert WORKFLOW tags
	
	delete from i2b2_tags
	where tag_type = 'WORKFLOW';
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Delete existing trial WORKFLOW in I2B2METADATA i2b2_tags',SQL%ROWCOUNT,stepCt,'Done');
	commit;	
	
	insert into i2b2_tags
	(path
	,tag_type
	,tag
	)
	select distinct b.c_fullname
		  ,'WORKFLOW'
		  ,decode(d.platform,'MRNA_AFFYMETRIX','Gene Expression','RBM','RBM','Protein','Protein',null) as tag
	from deapp.de_subject_sample_mapping d
		,i2b2 b
	where d.platform is not null
	  and d.trial_name = b.sourcesystem_cd
	  and b.c_hlevel = 0
	  and b.c_fullname not like '%Across Trials%';
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Inserted heatmap WORKFLOW in I2B2METADATA i2b2_tags',SQL%ROWCOUNT,stepCt,'Done');
	commit;	

	insert into i2b2_tags
	(path
	,tag_type
	,tag
	)
	select distinct c.c_fullname
		  ,'WORKFLOW'
		  ,'SNP'
	from deapp.de_snp_data_by_patient snp
	,i2b2 c
	where snp.trial_name = c.sourcesystem_cd
	  and c.c_hlevel = 0;
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Inserted SNP WORKFLOW in I2B2METADATA i2b2_tags',SQL%ROWCOUNT,stepCt,'Done');
	commit;		
*/
