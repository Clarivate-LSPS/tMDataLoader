-- Copy tables from tm_wz schema to tm_dataloader schema
drop table if exists tm_dataloader.wt_subject_mrna_probeset;
create unlogged table tm_dataloader.wt_subject_mrna_probeset as (select * from tm_wz.wt_subject_mrna_probeset where 1=0);
alter table tm_dataloader.wt_subject_mrna_probeset owner to tm_dataloader;

drop table if exists tm_dataloader.wt_subject_microarray_logs;
create unlogged table tm_dataloader.wt_subject_microarray_logs as (select * from tm_wz.wt_subject_microarray_logs where 1=0);
alter table tm_dataloader.wt_subject_microarray_logs owner to tm_dataloader;

drop table if exists tm_dataloader.wt_subject_microarray_calcs;
create unlogged table tm_dataloader.wt_subject_microarray_calcs as (select * from tm_wz.wt_subject_microarray_calcs where 1=0);
alter table tm_dataloader.wt_subject_microarray_calcs owner to tm_dataloader;

-- Alter types for working tables to increase calculations speed (float point arithmetic much faster than numeric)
alter table tm_dataloader.wt_subject_microarray_logs alter column log_intensity type double precision;
alter table tm_dataloader.wt_subject_microarray_logs alter column raw_intensity type double precision;

alter table tm_dataloader.wt_subject_microarray_calcs alter column mean_intensity type double precision;
alter table tm_dataloader.wt_subject_microarray_calcs alter column median_intensity type double precision;
alter table tm_dataloader.wt_subject_microarray_calcs alter column stddev_intensity type double precision;
alter table tm_dataloader.wt_subject_mrna_probeset alter column intensity_value type double precision;