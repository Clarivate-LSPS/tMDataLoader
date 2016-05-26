------------------------------------------------------------------------------
-- Run this script as user with DBA role before running run_as_tm_cz.sql .
-- Make sure the correct schema is specified in definitions of TM_CZ_SCHEMA:
--   $ grep -i 'define.*TM_CZ_SCHEMA' *
------------------------------------------------------------------------------
@1_run_first.sql
@SERIAL_HDD_Create_Tables.sql
@RBM_Create_Tables.sql
@2_permissions.sql
@3_synonyms.sql
@4_refresh_views.sql
@"migrations/1_2_4/20160308180000000_widen_category_cd_to_250_characters.sql"
@"migrations/1_2_4/20160313180000000_delete_cascade_from_search_secure_object.sql"
@"migrations/1_2_4/20160328180000000_add_cz_form_layout_synonym.sql"
@"migrations/1_2_4/20160524172500000_change_async_job_alt_viewer_url.sql"
@"migrations/gwas_plink/migrations.sql"