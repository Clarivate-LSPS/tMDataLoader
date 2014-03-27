-- Alter types for temp tables to increase calculations speed (float point arithmetic much faster than numeric)
alter table tm_wz.wt_subject_microarray_logs
alter column log_intensity type double precision;

alter table tm_wz.wt_subject_microarray_logs
alter column raw_intensity type double precision;

alter table tm_wz.wt_subject_microarray_calcs
alter column mean_intensity type double precision;

alter table tm_wz.wt_subject_microarray_calcs
alter column median_intensity type double precision;

alter table tm_wz.wt_subject_microarray_calcs
alter column stddev_intensity type double precision;

alter table tm_wz.wt_subject_mrna_probeset
alter column intensity_value type double precision;

create unlogged table tm_wz.wt_subject_mrna_probeset_tmp as (select * from tm_wz.wt_subject_mrna_probeset);
create unlogged table tm_wz.wt_subject_microarray_logs_tmp as (select * from tm_wz.wt_subject_microarray_logs);

drop table tm_wz.wt_subject_mrna_probeset;
drop table tm_wz.wt_subject_microarray_logs;

alter table tm_wz.wt_subject_mrna_probeset_tmp rename to wt_subject_mrna_probeset;
alter table tm_wz.wt_subject_microarray_logs_tmp rename to wt_subject_microarray_logs;