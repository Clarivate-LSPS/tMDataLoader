set define on;

DEFINE TM_WZ_SCHEMA='TM_WZ';
DEFINE TM_LZ_SCHEMA='TM_LZ';
DEFINE TM_CZ_SCHEMA='TM_CZ';

drop table "&TM_CZ_SCHEMA".STG_SUBJECT_RBM_DATA;
drop table "&TM_CZ_SCHEMA".STG_SUBJECT_RBM_DATA_RAW;

create table "&TM_CZ_SCHEMA".STG_SUBJECT_RBM_DATA
(
  TRIAL_NAME varchar(100),
  ANTIGEN_NAME varchar(100),
  VALUE_TEXT varchar(100),
  VALUE_NUMBER NUMBER,
  TIMEPOINT varchar(100),
  ASSAY_ID varchar(100),
  SAMPLE_ID varchar(100),
  SUBJECT_ID varchar(100),
  SITE_ID varchar(100)
);

create table "&TM_CZ_SCHEMA".STG_SUBJECT_RBM_DATA_RAW
(
  TRIAL_NAME varchar(100),
  ANTIGEN_NAME varchar(100),
  VALUE_TEXT varchar(100),
  VALUE_NUMBER NUMBER,
  TIMEPOINT varchar(100),
  ASSAY_ID varchar(100),
  SAMPLE_ID varchar(100),
  SUBJECT_ID varchar(100),
  SITE_ID varchar(100)
);

create view "&TM_CZ_SCHEMA".PATIENT_INFO
as select TRIAL_NAME as STUDY_ID, SUBJECT_ID, SITE_ID, REGEXP_REPLACE(TRIAL_NAME || ':' || SITE_ID || ':' || SUBJECT_ID,
                   '(::){1,}', ':') as usubjid from "&TM_CZ_SCHEMA".stg_subject_rbm_data;


create table "&TM_CZ_SCHEMA".STG_RBM_ANTIGEN_GENE
(
  ANTIGEN_NAME varchar(255),
  GENE_SYMBOL varchar(100),
  gene_id varchar(100)
);

create table "&TM_WZ_SCHEMA".tmp_subject_rbm_logs as
				  select trial_name
                  ,antigen_name
                  ,n_value
                  ,patient_id
                  ,gene_symbol
                  ,gene_id
                  ,assay_id
                  ,normalized_value
                  ,concept_cd
                  ,timepoint
                  ,value
                  ,n_value as log_intensity
                  from deapp.de_subject_rbm_data
                  where 1=2;

create table "&TM_WZ_SCHEMA".tmp_subject_rbm_calcs as
               select trial_name
				,gene_symbol
				,antigen_name
				,log_intensity as mean_intensity
				,log_intensity as median_intensity
				,log_intensity as stddev_intensity
				from "&TM_WZ_SCHEMA".tmp_subject_rbm_logs
				where 1=2;

create table "&TM_WZ_SCHEMA".tmp_subject_rbm_med as
				select trial_name
                    ,antigen_name
	                ,n_value
	                ,patient_id
                    ,gene_symbol
                    ,gene_id
	                ,assay_id
	                ,normalized_value
	                ,concept_cd
	                ,timepoint
                    ,log_intensity
	                ,value
                    ,log_intensity as mean_intensity
	                ,log_intensity as stddev_intensity
	                ,log_intensity as median_intensity
                    ,LOG_INTENSITY as ZSCORE
                   from "&TM_WZ_SCHEMA".TMP_SUBJECT_RBM_LOGS
				   where 1=2;

create or replace synonym "&TM_CZ_SCHEMA"."tmp_subject_rbm_logs" for "&TM_WZ_SCHEMA"."tmp_subject_rbm_logs";
create or replace synonym "&TM_CZ_SCHEMA"."tmp_subject_rbm_calcs" for "&TM_WZ_SCHEMA"."tmp_subject_rbm_calcs";
create or replace synonym "&TM_CZ_SCHEMA"."tmp_subject_rbm_med" for "&TM_WZ_SCHEMA"."tmp_subject_rbm_med";
           
grant insert,update,delete,select on "&TM_WZ_SCHEMA".TMP_SUBJECT_RBM_CALCS to "&TM_CZ_SCHEMA";
grant insert,update,delete,select on "&TM_WZ_SCHEMA".TMP_SUBJECT_RBM_LOGS to "&TM_CZ_SCHEMA";
grant insert,update,delete,select on "&TM_WZ_SCHEMA".TMP_SUBJECT_RBM_MED to "&TM_CZ_SCHEMA";

create table "&TM_WZ_SCHEMA".DE_SUBJECT_RBM_DATA
as select * from deapp.de_subject_rbm_data;

grant insert,update,delete,select on "&TM_WZ_SCHEMA".DE_SUBJECT_RBM_DATA to "&TM_CZ_SCHEMA";
