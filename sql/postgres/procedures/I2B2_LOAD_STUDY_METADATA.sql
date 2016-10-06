--
-- Name: i2b2_load_study_metadata(numeric); Type: FUNCTION; Schema: tm_dataloader; Owner: -
--
CREATE OR REPLACE FUNCTION i2b2_load_study_metadata(currentjobid numeric DEFAULT (-1)) RETURNS bigint
    LANGUAGE plpgsql SECURITY DEFINER SET SEARCH_PATH FROM CURRENT
    AS $$
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
declare

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

	dcount 				int;
	lcount 				int;
	upload_date			timestamp;
	tmp_compound		varchar(200);
	tmp_disease			varchar(200);
	tmp_organism		varchar(200);
	tmp_pubmed			varchar(2000);
	pubmed_id				varchar(200);
	pubmed_title		varchar(2000);
	tag_path				varchar(400);
	etl_program_id 	int;
	study_folder_id int;
	study_phase_tag_item_id int;

	study_compound_rec	record;
	study_disease_rec	record;
	study_taxonomy_rec  record;
	study_pubmed_rec 	record;
	bio_experiment_rec record;

BEGIN

	--Set Audit Parameters
	newJobFlag := 0; -- False (Default)
	jobID := currentJobID;
	databaseName := current_schema();
	procedureName := 'i2b2_load_study_metadata';

	--Audit JOB Initialization
	--If Job ID does not exist, then this is a single procedure run and we need to create it

	select case when coalesce(currentjobid, -1) < 1 then cz_start_audit(procedureName, databaseName) else currentjobid end into jobId;

	stepCt := 0;
	stepCt := stepCt + 1;
	select cz_write_audit(jobId,databaseName,procedureName,'Starting ' || procedureName,0,stepCt,'Done') into rtnCd;

	select clock_timestamp() into upload_date;

	-- create etl program if necessary
	select folder_id
	  into etl_program_id
	  from fmapp.fm_folder
	 where folder_name = 'etl-program'
		 and folder_type = 'PROGRAM';

	if (etl_program_id is null) then
		begin
			insert into fmapp.fm_folder (folder_id, folder_name, folder_level, folder_type, active_ind, description)
				 values (nextval('FMAPP.SEQ_FM_ID'),'etl-program', 0, 'PROGRAM', true,
				'Special program. Create automatically when tmDataloader load metadata for study. Necessary for support study filters')
			  returning folder_id
				   into etl_program_id ;
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
    select cz_write_audit(jobId,databaseName,procedureName,'Create etl program',rowCt,stepCt,'Done') into rtnCd;
	end if;

	--	Update existing bio_experiment data
	begin
		with upd as (select m.study_id
									 ,m.title
									 ,m.description
									 ,case when m.design is null then null
									  else 'STUDY_DESIGN:' ||
										upper(regexp_replace(m.design, ' ', '_', 'g')) end as design
									 ,case when is_date(m.start_date,'YYYYMMDD') = 1 then null
										else to_date(m.start_date,'YYYYMMDD') end as start_date
									 ,case when is_date(m.completion_date,'YYYYMMDD') = 1 then null
										else to_date(m.completion_date,'YYYYMMDD') end as completion_date
									 ,coalesce(m.primary_investigator,m.study_owner) as primary_investigator
									 ,m.overall_design
									 ,case when m.institution is null then null
									  else 'STUDY_INSTITUTION:' ||
										upper(regexp_replace(m.institution, ' ', '_', 'g')) end as institution
									 ,case when m.country is null then null
								 	  else 'COUNTRY:' || upper(m.country) end as country
									 ,case when m.biomarker_type is null then null
									  else 'STUDY_BIOMARKER_TYPE:' ||
										upper(regexp_replace(m.biomarker_type, ' ', '_', 'g')) end as biomarker_type
									 ,case when m.access_type is null then null
									  else 'STUDY_ACCESS_TYPE:' ||
								 		upper(regexp_replace(m.access_type, ' ', '_', 'g')) end as access_type
    from lt_src_study_metadata m
								 where m.study_id is not null)
		update biomart.bio_experiment b
		set title=upd.title
			,description=upd.description
			,design=upd.design
			,start_date=upd.start_date
			,completion_date=upd.completion_date
			,primary_investigator=upd.primary_investigator
			,overall_design=upd.overall_design
			,institution=upd.institution
			,country=upd.country
			,biomarker_type = upd.biomarker_type
			,access_type = upd.access_type
		from upd
		where b.accession = upd.study_id
					and b.etl_id = 'METADATA:' || upd.study_id;
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
	select cz_write_audit(jobId,databaseName,procedureName,'Updated trial data in BIOMART bio_experiment',rowCt,stepCt,'Done') into rtnCd;

	--	Add new trial data to bio_experiment

	begin
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
			,country
			,institution
			,access_type
			,biomarker_type)
			select 'Experiment'
				,m.title
				,m.description
				,case when m.design is null then null
				 else 'STUDY_DESIGN:' ||
					upper(regexp_replace(m.design, ' ', '_', 'g')) end as design
				,case when is_date(m.start_date,'YYYYMMDD') = 1 then null
				 else to_date(m.start_date,'YYYYMMDD') end as start_date
				,case when is_date(m.completion_date,'YYYYMMDD') = 1 then null
				 else to_date(m.completion_date,'YYYYMMDD') end as completion_date
				,coalesce(m.primary_investigator,m.study_owner) as primary_investigator
				,m.contact_field
				,'METADATA:' || m.study_id
				,m.study_id
				,m.overall_design
				,m.study_id
				,case when m.country is null then null
				 else 'COUNTRY:' || upper(m.country) end as country
				,case when m.institution is null then null
				 else 'STUDY_INSTITUTION:' ||
					upper(regexp_replace(m.institution, ' ', '_', 'g')) end as institution
				,case when m.access_type is null then null
				 else 'STUDY_ACCESS_TYPE:' ||
					upper(regexp_replace(m.access_type, ' ', '_', 'g')) end as access_type
				,case when m.biomarker_type is null then null
				 else 'STUDY_BIOMARKER_TYPE:' ||
					upper(regexp_replace(m.biomarker_type, ' ', '_', 'g')) end as biomarker_type
			from lt_src_study_metadata m
			where m.study_id is not null
						and not exists
			(select 1 from biomart.bio_experiment x
					where m.study_id = x.accession
								and m.study_id is not null);
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
	select cz_write_audit(jobId,databaseName,procedureName,'Add study to BIOMART bio_experiment',rowCt,stepCt,'Done') into rtnCd;

	--	Insert new trial into bio_data_uid

	begin
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
					where x.unique_id = 'EXP:' || m.study_id);
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
	select cz_write_audit(jobId,databaseName,procedureName,'Added study to bio_data_uid',rowCt,stepCt,'Done') into rtnCd;

	-- Create study folder
	begin
		for bio_experiment_rec in (select dat.unique_id, exp.title, exp.description, met.study_phase
									 from biomart.bio_experiment exp, lt_src_study_metadata met, biomart.bio_data_uid dat
									where exp.accession = met.study_id
									  and exp.bio_experiment_id = dat.bio_data_id
									  and not exists (select 1
														from fmapp.fm_folder_association
													   where object_uid = dat.unique_id)) loop

			insert into fmapp.fm_folder (folder_id, folder_name, folder_level, folder_type, active_ind, parent_id, description)
				 values (nextval('FMAPP.SEQ_FM_ID'), bio_experiment_rec.title, 1,'STUDY', true, etl_program_id, bio_experiment_rec.description)
			  returning folder_id
				   into study_folder_id;

			insert into fmapp.fm_folder_association (folder_id, object_uid, object_type)
				 values (study_folder_id, bio_experiment_rec.unique_id, 'org.transmart.biomart.Experiment');

			select tag_item_id
			  into study_phase_tag_item_id
			  from amapp.am_tag_item
			 where code_type_name = 'STUDY_PHASE';

			if (bio_experiment_rec.study_phase is not null and study_phase_tag_item_id is not null) then
				select count(object_uid)
				  into lcount
				  from amapp.am_tag_association
				 where subject_uid = 'FOL:' || study_folder_id
				   and tag_item_id = study_phase_tag_item_id;

				if (lcount = 0) then
					insert into amapp.am_tag_association (subject_uid, object_uid, object_type, tag_item_id)
					   	 values ('FOL:' || study_folder_id, 
								'STUDY_PHASE:' || upper(regexp_replace(bio_experiment_rec.study_phase, ' ', '_', 'g')),
								'BIO_CONCEPT_CODE', study_phase_tag_item_id);
				else
					update amapp.am_tag_association
					   set object_uid =  'STUDY_PHASE:' || upper(regexp_replace(bio_experiment_rec.study_phase, ' ', '_', 'g'))
					 where subject_uid = 'FOL:' || study_folder_id
					   and tag_item_id = study_phase_tag_item_id;
				end if;
			end if;

			stepCt := stepCt + 1;
			select cz_write_audit(jobId,databaseName,procedureName,'Add study folder:' || bio_experiment_rec.title,rowCt,stepCt,'Done') into rtnCd;
		end loop;
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

	--	delete existing compound data for study, compound list may change

	begin
		delete from biomart.bio_data_compound dc
		where dc.bio_data_id in
					(select x.bio_experiment_id
					 from biomart.bio_experiment x
						 ,lt_src_study_metadata y
					 where x.accession = y.study_id
								 and x.etl_id = 'METADATA:' || y.study_id);
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
	select cz_write_audit(jobId,databaseName,procedureName,'Delete existing study data from bio_compound',rowCt,stepCt,'Done') into rtnCd;

	--	add study compound data

	for study_compound_rec in
	select distinct study_id
		,compound
	from lt_src_study_metadata
	where compound is not null
	loop
		select length(study_compound_rec.compound)-length(replace(study_compound_rec.compound,';',''))+1 into dcount;
		while dcount > 0
		Loop
			select parse_nth_value(study_compound_rec.compound,dcount,';') into tmp_compound;

			--	add new compound
			begin
				insert into biomart.bio_compound
				(generic_name)
					select tmp_compound
					where not exists
					(select 1 from biomart.bio_compound x
							where upper(x.generic_name) = upper(tmp_compound))
								and tmp_compound is not null;
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
			select cz_write_audit(jobId,databaseName,procedureName,'Add study compound to bio_compound',rowCt,stepCt,'Done') into rtnCd;

			--	Insert new trial data into bio_data_compound
			begin
				insert into biomart.bio_data_compound
				(bio_data_id
					,bio_compound_id
					,etl_source
				)
					select b.bio_experiment_id
						,c.bio_compound_id
						,'METADATA:' || study_compound_rec.study_id
					from biomart.bio_experiment b
						,biomart.bio_compound c
					where upper(tmp_compound) = upper(c.generic_name)
								and tmp_compound is not null
								and b.accession = study_compound_rec.study_id
								and not exists
					(select 1 from biomart.bio_data_compound x
							where b.bio_experiment_id = x.bio_data_id
										and c.bio_compound_id = x.bio_compound_id);
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
			select cz_write_audit(jobId,databaseName,procedureName,'Add study compound to bio_data_compound',rowCt,stepCt,'Done') into rtnCd;
			dcount := dcount - 1;
		end loop;
	end loop;

	--	delete existing disease data for studies

	begin
		delete from biomart.bio_data_disease dc
		where dc.bio_data_id in
					(select x.bio_experiment_id
					 from biomart.bio_experiment x
						 ,lt_src_study_metadata y
					 where x.accession = y.study_id
								 and x.etl_id = 'METADATA:' || y.study_id);
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
	select cz_write_audit(jobId,databaseName,procedureName,'Delete existing study data from bio_data_disease',rowCt,stepCt,'Done') into rtnCd;

	--	add study disease data

	for study_disease_rec in
	select distinct study_id, disease
	from lt_src_study_metadata
	where disease is not null
	loop
		select length(study_disease_rec.disease)-length(replace(study_disease_rec.disease,';',''))+1 into dcount;
		while dcount > 0
		Loop
			select parse_nth_value(study_disease_rec.disease,dcount,';') into tmp_disease;

			--	add new disease
			begin
				insert into biomart.bio_disease
				(disease
					,prefered_name)
					select tmp_disease
						,tmp_disease
					where not exists
					(select 1 from biomart.bio_disease x
							where upper(x.disease) = upper(tmp_disease))
								and tmp_disease is not null;
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
			select cz_write_audit(jobId,databaseName,procedureName,'Add study disease to bio_disease',rowCt,stepCt,'Done') into rtnCd;

			--	Insert new trial data into bio_data_disease
			begin
				insert into biomart.bio_data_disease
				(bio_data_id
					,bio_disease_id
					,etl_source
				)
					select b.bio_experiment_id
						,c.bio_disease_id
						,'METADATA:' || study_disease_rec.study_id
					from biomart.bio_experiment b
						,biomart.bio_disease c
					where upper(tmp_disease) = upper(c.disease)
								and tmp_disease is not null
								and b.accession = study_disease_rec.study_id
								and not exists
					(select 1 from biomart.bio_data_disease x
							where b.bio_experiment_id = x.bio_data_id
										and c.bio_disease_id = x.bio_disease_id);
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
			select cz_write_audit(jobId,databaseName,procedureName,'Add study disease to bio_data_disease',rowCt,stepCt,'Done') into rtnCd;
			dcount := dcount - 1;
		end loop;
	end loop;

	--	delete existing taxonomy data for studies

	begin
		delete from biomart.bio_data_taxonomy dc
		where dc.bio_data_id in
					(select x.bio_experiment_id
					 from biomart.bio_experiment x
						 ,lt_src_study_metadata y
					 where x.accession = y.study_id
								 and x.etl_id = 'METADATA:' || y.study_id);
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
	select cz_write_audit(jobId,databaseName,procedureName,'Delete existing study data from bio_data_taxonomy',rowCt,stepCt,'Done') into rtnCd;

	--	add study organism to taxonomy

	for study_taxonomy_rec in
	select distinct study_id, organism
	from lt_src_study_metadata
	where organism is not null
	loop
		select length(study_taxonomy_rec.organism)-length(replace(study_taxonomy_rec.organism,';',''))+1 into dcount;
		while dcount > 0
		Loop
			select parse_nth_value(study_taxonomy_rec.organism,dcount,';') into tmp_organism;

			--	add new organism
			begin
				insert into biomart.bio_taxonomy
				(taxon_name
					,taxon_label)
					select tmp_organism
						,tmp_organism
					where not exists
					(select 1 from biomart.bio_taxonomy x
							where upper(x.taxon_name) = upper(tmp_organism))
								and tmp_organism is not null;
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
			select cz_write_audit(jobId,databaseName,procedureName,'Add study organism to bio_taxonomy',rowCt,stepCt,'Done') into rtnCd;

			--	Insert new trial data into bio_data_taxonomy
			begin
				insert into biomart.bio_data_taxonomy
				(bio_data_id
					,bio_taxonomy_id
					,etl_source
				)
					select b.bio_experiment_id
						,c.bio_taxonomy_id
						,'METADATA:' || study_disease_rec.study_id
					from biomart.bio_experiment b
						,biomart.bio_taxonomy c
					where upper(tmp_organism) = upper(c.taxon_name)
								and tmp_organism is not null
								and b.accession = study_disease_rec.study_id
								and not exists
					(select 1 from biomart.bio_data_taxonomy x
							where b.bio_experiment_id = x.bio_data_id
										and c.bio_taxonomy_id = x.bio_taxonomy_id);
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
			select cz_write_audit(jobId,databaseName,procedureName,'Add study organism to bio_data_taxonomy',rowCt,stepCt,'Done') into rtnCd;

			dcount := dcount - 1;
		end loop;
	end loop;

	--	add ncbi/GEO linking

	--	check if ncbi exists in bio_content_repository, if not, add

	select count(*) into dcount
	from biomart.bio_content_repository
	where repository_type = 'NCBI'
				and location_type = 'URL';

	if dcount = 0 then
		begin
			insert into biomart.bio_content_repository
			(location
				,active_y_n
				,repository_type
				,location_type)
			values ('http://www.ncbi.nlm.nih.gov/','Y','NCBI','URL');
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
		select cz_write_audit(jobId,databaseName,procedureName,'Insert link to NCBI into bio_content_repository',rowCt,stepCt,'Done') into rtnCd;
	end if;

	--	insert GSE studies into bio_content

	begin
		insert into biomart.bio_content
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
				,biomart.bio_content_repository bcr
			where m.study_id like 'GSE%'
						and bcr.repository_type = 'NCBI'
						and bcr.location_type = 'URL'
						and not exists
			(select 1 from biomart.bio_content x
					where x.etl_id_c like '%' || m.study_id || '%'
								and x.file_type = 'Experiment Web Link'
								and x.location = 'geo/query/acc.cgi?acc=' || m.study_id);
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
	select cz_write_audit(jobId,databaseName,procedureName,'Add GEO study to bio_cotent',rowCt,stepCt,'Done') into rtnCd;

	--	insert GSE studies into bio_content_reference

	begin
		insert into biomart.bio_content_reference
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
				,biomart.bio_experiment be
				,biomart.bio_content bc
			where m.study_id like 'GSE%'
						and m.study_id = be.accession
						and bc.file_type = 'Experiment Web Link'
						and bc.etl_id_c = 'METADATA:' || m.study_id
						and bc.location = 'geo/query/acc.cgi?acc=' || m.study_id
						and not exists
			(select 1 from biomart.bio_content_reference x
					where bc.bio_file_content_id = x.bio_content_id
								and be.bio_experiment_id = x.bio_data_id);
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
	select cz_write_audit(jobId,databaseName,procedureName,'Added GEO study to bio_content_reference',rowCt,stepCt,'Done') into rtnCd;

	--	add PUBMED linking

	--	delete existing pubmed data for studies

	begin
		delete from biomart.bio_content_reference dc
		where dc.bio_content_id in
					(select x.bio_file_content_id
					 from biomart.bio_content x
						 ,lt_src_study_metadata y
					 where x.file_type = 'Publication Web Link'
								 and x.etl_id_c = 'METADATA:' || y.study_id);
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
	select cz_write_audit(jobId,databaseName,procedureName,'Delete existing study pubmed from bio_content_reference',rowCt,stepCt,'Done') into rtnCd;

	begin
		delete from biomart.bio_content dc
		where dc.bio_file_content_id in
					(select x.bio_file_content_id
					 from biomart.bio_content x
						 ,lt_src_study_metadata y
					 where x.file_type = 'Publication Web Link'
								 and x.etl_id_c = 'METADATA:' || y.study_id);
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
	select cz_write_audit(jobId,databaseName,procedureName,'Delete existing study pubmed from bio_content',rowCt,stepCt,'Done') into rtnCd;

	--	add study pubmed ids'

	select count(*) into dcount
	from biomart.bio_content_repository
	where repository_type = 'PubMed';

	if dcount = 0 then
		begin
			insert into biomart.bio_content_repository
			(location
				,active_y_n
				,repository_type
				,location_type)
			values ('http://www.ncbi.nlm.nih.gov/pubmed/','Y','PubMed','URL');
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
		select cz_write_audit(jobId,databaseName,procedureName,'Add pubmed url to bio_content_repository',rowCt,stepCt,'Done') into rtnCd;
	end if;

	for study_pubmed_rec in
	select distinct study_id, pubmed_ids
	from lt_src_study_metadata
	where pubmed_ids is not null
	loop
		select length(study_pubmed_rec.pubmed_ids)-length(replace(study_pubmed_rec.pubmed_ids,'|',''))+1 into dcount;
		while dcount > 0
		Loop
			-- multiple pubmed id can be separated by |, pubmed id and title are separated by :

			select parse_nth_value(study_pubmed_rec.pubmed_ids,dcount,'|') into tmp_pubmed;
			select instr(tmp_pubmed,'@') into lcount;

			if lcount = 0 then
				pubmed_id := tmp_pubmed;
				pubmed_title := null;
			else
				pubmed_id := substr(tmp_pubmed,1,instr(tmp_pubmed,'@')-1);
				pubmed_title := substr(tmp_pubmed,instr(tmp_pubmed,'@')+1);
			end if;

			begin
				insert into biomart.bio_content
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
						,'METADATA:' || study_pubmed_rec.study_id
					from biomart.bio_content_repository bcr
					where bcr.repository_type = 'PubMed'
								and not exists
					(select 1 from biomart.bio_content x
							where x.etl_id_c like '%' || study_pubmed_rec.study_id || '%'
										and x.file_type = 'Publication Web Link'
										and x.location = pubmed_id);
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
			select cz_write_audit(jobId,databaseName,procedureName,'Add study pubmed to bio_content',rowCt,stepCt,'Done') into rtnCd;

			begin
				insert into biomart.bio_content_reference
				(bio_content_id
					,bio_data_id
					,content_reference_type
					,etl_id_c
				)
					select bc.bio_file_content_id
						,be.bio_experiment_id
						,'Publication Web Link'
						,'METADATA:' || study_pubmed_rec.study_id
					from biomart.bio_experiment be
						,biomart.bio_content bc
					where be.accession = study_pubmed_rec.study_id
								and bc.file_type = 'Publication Web Link'
								and bc.etl_id_c = 'METADATA:' || study_pubmed_rec.study_id
								and bc.location = pubmed_id
								and not exists
					(select 1 from biomart.bio_content_reference x
							where bc.bio_file_content_id = x.bio_content_id
										and be.bio_experiment_id = x.bio_data_id);
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
			select cz_write_audit(jobId,databaseName,procedureName,'Add study pubmed to bio_content_reference',rowCt,stepCt,'Done') into rtnCd;
			dcount := dcount - 1;
		end loop;
	end loop;

	--	Create i2b2_tags

  select min(b.c_fullname)
	  into tag_path
    from lt_src_study_metadata m,i2b2 b
   where m.study_id = b.sourcesystem_cd
     and m.study_id is not null;

	begin
		delete from i2b2metadata.i2b2_tags t
		      where upper(t.tag_type) in ('TRIAL','COMPOUND','DISEASE')
						and t.path = tag_path;
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
	select cz_write_audit(jobId,databaseName,procedureName,'Delete study from i2b2_tags',rowCt,stepCt,'Done') into rtnCd;

	begin
		insert into i2b2metadata.i2b2_tags
		(tag_id, path, tag, tag_type, tags_idx)
			select nextval('i2b2metadata.sq_i2b2_tag_id')
				,tag_path as path
				,be.accession as tag
				,'Trial' as tag_type
				,0 as tags_idx
			from biomart.bio_experiment be
				,i2b2metadata.i2b2 b
			where be.accession = b.sourcesystem_cd
			      and b.c_fullname = tag_path
			group by be.accession;
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
	select cz_write_audit(jobId,databaseName,procedureName,'Add study to i2b2_tags',rowCt,stepCt,'Done') into rtnCd;

		--	Insert trial data tags - COMPOUND
	begin
    insert into i2b2_tags
    (path, tag, tag_type, tags_idx)
      select distinct tag_path as path
        ,coalesce(c.generic_name,c.brand_name) as tag
        ,'Compound' as tag_type
        ,1 as tags_idx
      from biomart.bio_experiment be
        ,biomart.bio_data_compound bc
        ,biomart.bio_compound c
        ,i2b2 o
      where be.bio_experiment_id = bc.bio_data_id
            and bc.bio_compound_id = c.bio_compound_id
            and be.accession = o.sourcesystem_cd
            and coalesce(c.generic_name,c.brand_name) is not null
            and o.c_fullname = tag_path
      group by coalesce(c.generic_name,c.brand_name);
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
	select cz_write_audit(jobId,databaseName,procedureName,'Insert Compound tags in I2B2METADATA i2b2_tags',rowCt,stepCt,'Done') into rtnCd;

		--	Insert trial data tags - DISEASE

	begin
		insert into i2b2_tags
			(path, tag, tag_type, tags_idx)
			select distinct tag_path as path
					 ,c.prefered_name
					 ,'Disease' as tag_type
					 ,2 as tags_idx
			from biomart.bio_experiment be
				,biomart.bio_data_disease bc
				,biomart.bio_disease c
				,i2b2 o
			where be.bio_experiment_id = bc.bio_data_id
					and bc.bio_disease_id = c.bio_disease_id
					and be.accession = o.sourcesystem_cd
					and c.prefered_name is not null
					and o.c_fullname = tag_path
			group by c.prefered_name;
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
	select cz_write_audit(jobId,databaseName,procedureName,'Insert Disease tags in I2B2METADATA i2b2_tags',rowCt,stepCt,'Done') into rtnCd;

	---Cleanup OVERALL JOB if this proc is being run standalone

	stepCt := stepCt + 1;
	select cz_write_audit(jobId,databaseName,procedureName,'End ' || procedureName,0,stepCt,'Done') into rtnCd;

	---Cleanup OVERALL JOB if this proc is being run standalone
	perform cz_end_audit (jobID, 'SUCCESS') where coalesce(currentJobId, -1) <> jobId;

	return 1;

END;

$$;

