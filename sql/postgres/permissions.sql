-- Permissions for tm_cz
grant truncate, select, insert, delete on tm_lz.lt_src_clinical_data to tm_cz;
grant truncate, select, insert, delete on tm_lz.lt_src_mrna_subj_samp_map to tm_cz;
grant truncate, select, insert, delete on tm_lz.lt_src_mrna_data to tm_cz;
grant truncate, select, insert, delete on tm_lz.lt_src_deapp_annot to tm_cz;
grant truncate, select, insert, delete on tm_lz.lt_snp_gene_map to tm_cz;
grant truncate, select, insert, delete on tm_lz.lt_snp_calls_by_gsm to tm_cz;
grant truncate, select, insert, delete on tm_lz.lt_snp_copy_number to tm_cz;
grant insert, delete, select on deapp.de_snp_gene_map to tm_cz;
grant insert, delete, select on deapp.de_snp_calls_by_gsm to tm_cz;
grant insert, delete, select on deapp.de_snp_copy_number to tm_cz;
grant usage on schema deapp to tm_cz;
grant select, insert on deapp.de_gpl_info to tm_cz;
grant insert, delete, select on deapp.de_variant_dataset to tm_cz;
grant insert, delete, select on deapp.de_variant_population_data to tm_cz;
grant insert, delete, select on deapp.de_variant_population_info to tm_cz;
grant insert, delete, select on deapp.de_variant_subject_detail to tm_cz;
grant insert, delete, select on deapp.de_variant_subject_idx to tm_cz;
grant insert, delete, select on deapp.de_variant_subject_summary to tm_cz;
grant usage on sequence deapp.de_variant_subject_idx_seq to tm_cz;
grant usage on sequence deapp.de_variant_population_info_seq to tm_cz;
grant usage on sequence deapp.de_variant_population_data_seq to tm_cz;
grant usage on sequence deapp.de_variant_subject_summary_seq to tm_cz;
grant usage on sequence deapp.de_variant_subject_detail_seq to tm_cz;
grant select on deapp.de_subject_sample_mapping to tm_cz;

-- Permissions for biomart_user
grant select on deapp.de_variant_dataset to biomart_user;
grant select on deapp.de_variant_population_data to biomart_user;
grant select on deapp.de_variant_population_info to biomart_user;
grant select on deapp.de_variant_subject_detail to biomart_user;
grant select on deapp.de_variant_subject_idx to biomart_user;
grant select on deapp.de_variant_subject_summary to biomart_user;