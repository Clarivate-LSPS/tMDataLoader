DO $$
begin
if current_schema() = 'public' then
  set SEARCH_PATH = tm_dataloader, tm_cz, tm_lz, tm_wz, i2b2demodata, i2b2metadata, deapp, pg_temp;
end if;
end;
$$;

\i procedures/median.sql
\i procedures/AUDIT_FUNCTIONS.sql
\i procedures/I2B2_ADD_NODE.sql
\i procedures/I2B2_ADD_ROOT_NODE.sql
\i procedures/I2B2_CREATE_CONCEPT_COUNTS.sql
\i procedures/I2B2_DELETE_1_NODE.sql
\i procedures/I2B2_LOAD_SECURITY_DATA.sql
\i procedures/I2B2_LOAD_SAMPLES.sql
\i procedures/I2B2_LOAD_ANNOTATION_DEAPP.sql
\i procedures/I2B2_PROCESS_MRNA_DATA.sql
\i procedures/I2B2_MIRNA_ZSCORE_CALC.sql
\i procedures/I2B2_CREATE_FULL_TREE.sql
\i procedures/I2B2_LOAD_CLINICAL_DATA.sql
\i procedures/I2B2_PROCESS_SNP_DATA.sql
\i procedures/I2B2_PROCESS_VCF_DATA.sql
\i procedures/I2B2_DELETE_ALL_DATA.sql
\i procedures/I2B2_MOVE_STUDY_BY_PATH.sql
\i procedures/I2B2_ADD_ROOT_NODE.sql
\i procedures/I2B2_LOAD_PROTEOMICS_ANNOT.sql
\i procedures/I2B2_PROCESS_PROTEOMICS_DATA.sql
\i procedures/I2B2_LOAD_METABOLOMICS_ANNOT.sql
\i procedures/I2B2_METABOLOMICS_ZSCORE_CALC.sql
\i procedures/I2B2_PROCESS_METABOLOMIC_DATA.sql
\i procedures/I2B2_PROCESS_QPCR_MIRNA_DATA.sql
\i procedures/I2B2_LOAD_MIRNA_ANNOT_DEAPP.sql
\i procedures/I2B2_PROCESS_RNA_SEQ_DATA.sql
\i procedures/I2B2_LOAD_RBM_DATA.sql
\i procedures/I2B2_LOAD_RBM_ANNOTATION.sql
\i procedures/I2B2_RBM_ZSCORE_CALC_NEW.sql
\i procedures/I2B2_RNA_SEQ_ANNOTATION.sql