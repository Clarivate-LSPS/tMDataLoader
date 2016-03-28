-- Permissions for tm_dataloader
grant usage on schema tm_dataloader to tm_dataloader;
alter schema tm_dataloader owner to tm_dataloader;
grant execute on all functions in schema tm_dataloader to tm_dataloader;

grant truncate, select, insert, delete on tm_dataloader.lt_src_clinical_data to tm_dataloader;
grant truncate, select, insert, delete on tm_dataloader.lt_src_mrna_subj_samp_map to tm_dataloader;
grant truncate, select, insert, delete on tm_dataloader.lt_src_mrna_data to tm_dataloader;
grant truncate, select, insert, delete on tm_dataloader.lt_src_deapp_annot to tm_dataloader;
grant truncate, select, insert, delete on tm_dataloader.lt_snp_gene_map to tm_dataloader;
grant truncate, select, insert, delete on tm_dataloader.lt_snp_calls_by_gsm to tm_dataloader;
grant truncate, select, insert, delete on tm_dataloader.lt_snp_copy_number to tm_dataloader;
grant truncate, select, insert, delete on tm_dataloader.lt_src_mrna_xml_data to tm_dataloader;
-- grant insert, delete, select on deapp.de_snp_gene_map to tm_dataloader;
-- grant insert, delete, select on deapp.de_snp_calls_by_gsm to tm_dataloader;
-- grant insert, delete, select on deapp.de_snp_copy_number to tm_dataloader;

-- deapp
grant usage on schema deapp to tm_dataloader;
grant select on deapp.de_mrna_annotation to tm_dataloader;
grant select, insert on deapp.de_gpl_info to tm_dataloader;
grant insert, delete, select on deapp.de_variant_dataset to tm_dataloader;
grant insert, delete, select on deapp.de_variant_population_data to tm_dataloader;
grant insert, delete, select on deapp.de_variant_population_info to tm_dataloader;
grant insert, delete, select on deapp.de_variant_subject_detail to tm_dataloader;
grant insert, delete, select on deapp.de_variant_subject_idx to tm_dataloader;
grant insert, delete, select on deapp.de_variant_subject_summary to tm_dataloader;
grant usage on sequence deapp.de_variant_subject_idx_seq to tm_dataloader;
grant usage on sequence deapp.de_variant_population_info_seq to tm_dataloader;
grant usage on sequence deapp.de_variant_population_data_seq to tm_dataloader;
grant usage on sequence deapp.de_variant_subject_summary_seq to tm_dataloader;
grant usage on sequence deapp.de_variant_subject_detail_seq to tm_dataloader;
-- grant usage on sequence i2b2metadata.i2b2_record_id_seq to tm_dataloader;
-- grant select on deapp.de_subject_sample_mapping to tm_dataloader;

grant select on deapp.de_subject_sample_mapping to tm_dataloader;

grant usage on schema i2b2demodata to tm_dataloader;
grant usage on schema deapp to tm_dataloader;

-- tm_cz
grant all on tm_cz.cz_job_audit, tm_cz.cz_job_master, tm_cz.cz_job_error to tm_dataloader;
grant usage on schema tm_cz to tm_dataloader;

-- i2b2metadata
grant usage on schema i2b2metadata to tm_dataloader;
grant select on i2b2metadata.i2b2 to tm_dataloader;

-- i2b2demodata
grant usage on schema i2b2demodata to tm_dataloader;
grant select on i2b2demodata.concept_dimension to tm_dataloader;

-- Permissions for biomart_user
grant select on deapp.de_variant_dataset to biomart_user;
grant select on deapp.de_variant_population_data to biomart_user;
grant select on deapp.de_variant_population_info to biomart_user;
grant select on deapp.de_variant_subject_detail to biomart_user;
grant select on deapp.de_variant_subject_idx to biomart_user;
grant select on deapp.de_variant_subject_summary to biomart_user;

grant usage on schema tm_dataloader to tm_cz;
grant usage on schema tm_cz to biomart_user;
grant SELECT, INSERT, UPDATE, DELETE on all tables in SCHEMA tm_dataloader to tm_cz;