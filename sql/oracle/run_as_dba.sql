------------------------------------------------------------------------------
-- Run this script as user with DBA role before running run_as_tm_dataloader.sql .
------------------------------------------------------------------------------
@1_run_first.sql
@SERIAL_HDD_Create_Tables.sql
@RBM_Create_Tables.sql
@2_permissions.sql
@3_synonyms.sql
@4_refresh_views.sql
@"migrations/1_2_4/20160328180000000_add_cz_form_layout_synonym.sql"
