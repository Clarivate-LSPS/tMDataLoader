set define on;

define TM_CZ_SCHEMA=TM_CZ;

grant select, insert on i2b2metadata.table_access to "&TM_CZ_SCHEMA";
grant select, insert, delete on i2b2demodata.observation_fact to "&TM_CZ_SCHEMA";
grant select, insert, delete on i2b2demodata.concept_dimension to "&TM_CZ_SCHEMA";
grant select, insert, delete on i2b2metadata.i2b2 to "&TM_CZ_SCHEMA";
grant select on i2b2metadata.i2b2_id_seq to "&TM_CZ_SCHEMA";
grant select on i2b2demodata.concept_id to "&TM_CZ_SCHEMA";