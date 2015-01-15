set define on;
define TM_CZ_SCHEMA=TM_CZ;
define TM_LZ_SCHEMA=TM_LZ;
define TM_WZ_SCHEMA=TM_WZ;

grant select, insert, update, delete on i2b2metadata.table_access to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on i2b2demodata.observation_fact to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on i2b2demodata.concept_dimension to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on i2b2demodata.concept_counts to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on i2b2demodata.modifier_dimension to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on i2b2demodata.patient_dimension to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on i2b2demodata.patient_trial to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on i2b2metadata.i2b2 to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on i2b2metadata.i2b2_tags to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on i2b2metadata.i2b2_secure to "&TM_CZ_SCHEMA";

grant select, insert, update, delete on biomart.bio_assay_data_annotation to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on biomart.bio_assay_feature_group to "&TM_CZ_SCHEMA"; 
grant select, insert, update, delete on biomart.bio_data_uid to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on biomart.bio_experiment to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on biomart.bio_compound to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on biomart.bio_content to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on biomart.bio_data_compound to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on biomart.bio_data_disease to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on biomart.bio_data_taxonomy to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on biomart.bio_disease to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on biomart.bio_clinical_trial to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on biomart.bio_content_reference to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on biomart.bio_content_repository to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on biomart.bio_marker to "&TM_CZ_SCHEMA";

grant select, insert, update, delete on deapp.de_subject_sample_mapping to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on deapp.de_subject_microarray_data to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on deapp.de_snp_data_dataset_loc to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on deapp.de_snp_data_by_patient to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on deapp.de_snp_copy_number to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on deapp.de_snp_subject_sorted_def to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on deapp.de_snp_data_by_probe to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on deapp.de_subject_snp_dataset to "&TM_CZ_SCHEMA";

grant select, insert, update, delete on deapp.de_variant_subject_summary to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on deapp.de_variant_population_data to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on deapp.de_variant_population_info to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on deapp.de_variant_subject_detail to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on deapp.de_variant_subject_idx to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on deapp.de_variant_dataset to "&TM_CZ_SCHEMA";

grant select, insert, update, delete on searchapp.search_secure_object to "&TM_CZ_SCHEMA";

grant select, insert, update, delete on "&TM_LZ_SCHEMA".lt_snp_calls_by_gsm to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on "&TM_LZ_SCHEMA".lt_snp_copy_number to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on "&TM_LZ_SCHEMA".lt_snp_gene_map to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on "&TM_LZ_SCHEMA".lz_src_clinical_data to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on "&TM_LZ_SCHEMA".lt_src_study_metadata to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on "&TM_LZ_SCHEMA".lt_src_mrna_subj_samp_map to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on "&TM_LZ_SCHEMA".lt_src_mrna_xml_data to "&TM_CZ_SCHEMA";

grant select, insert, update, delete on "&TM_WZ_SCHEMA".wt_mrna_nodes to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on "&TM_WZ_SCHEMA".wt_mrna_node_values to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on "&TM_WZ_SCHEMA".wt_trial_nodes to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on "&TM_WZ_SCHEMA".wrk_clinical_data to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on "&TM_WZ_SCHEMA".wt_num_data_types to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on "&TM_WZ_SCHEMA".wt_clinical_data_dups to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on "&TM_WZ_SCHEMA".wt_subject_mrna_probeset to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on "&TM_WZ_SCHEMA".wt_subject_microarray_logs to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on "&TM_WZ_SCHEMA".wt_subject_microarray_calcs to "&TM_CZ_SCHEMA";
grant select, insert, update, delete on "&TM_WZ_SCHEMA".wt_subject_microarray_med to "&TM_CZ_SCHEMA";

grant select on sys.all_tables to "&TM_CZ_SCHEMA";
-- the following 2 grants are done for ETL performance improvements, tm_cz_schema user needs
-- to truncate tables and alter indexes in another users schema (DEAPP, I2B2DEMODATA)
grant alter any table to "&TM_CZ_SCHEMA";
grant alter any index to "&TM_CZ_SCHEMA";

grant select on i2b2metadata.i2b2_id_seq to "&TM_CZ_SCHEMA";
grant select on i2b2demodata.concept_id to "&TM_CZ_SCHEMA";
grant select on i2b2demodata.seq_patient_num to "&TM_CZ_SCHEMA";

grant execute on tm_cz.i2b2_load_clinical_data to "&TM_CZ_SCHEMA";
grant execute on tm_cz.i2b2_create_full_tree to "&TM_CZ_SCHEMA";
grant execute on tm_cz.i2b2_create_concept_counts to "&TM_CZ_SCHEMA";

grant analyze any to "&TM_CZ_SCHEMA";
