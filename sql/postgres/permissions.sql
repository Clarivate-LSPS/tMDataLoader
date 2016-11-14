-- Permissions for tm_dataloader
alter schema gwas_plink owner to tm_dataloader;
alter schema tm_dataloader owner to tm_dataloader;
grant execute on all functions in schema tm_dataloader to tm_dataloader;

grant usage on schema deapp to tm_dataloader;
grant usage on schema i2b2demodata to tm_dataloader;
grant usage on schema tm_cz to tm_dataloader;
grant usage on schema i2b2metadata to tm_dataloader;
grant usage on schema i2b2demodata to tm_dataloader;

-- Permissions for biomart_user
grant select on deapp.de_variant_dataset to biomart_user;
grant select on deapp.de_variant_population_data to biomart_user;
grant select on deapp.de_variant_population_info to biomart_user;
grant select on deapp.de_variant_subject_detail to biomart_user;
grant select on deapp.de_variant_subject_idx to biomart_user;
grant select on deapp.de_variant_subject_summary to biomart_user;
grant usage on schema gwas_plink to biomart_user;
grant select on gwas_plink.plink_data to biomart_user;

grant usage on schema tm_dataloader to tm_cz;
grant usage on schema tm_cz to biomart_user;
grant SELECT, INSERT, UPDATE, DELETE on all tables in SCHEMA tm_dataloader to tm_cz;