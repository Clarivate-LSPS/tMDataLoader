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
