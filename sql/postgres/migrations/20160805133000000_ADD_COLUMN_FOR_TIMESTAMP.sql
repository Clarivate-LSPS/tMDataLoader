ALTER TABLE tm_dataloader.lt_src_clinical_data
  ADD COLUMN timestamp_baseline character varying(250);
ALTER TABLE tm_dataloader.wrk_clinical_data
  ADD COLUMN timestamp_baseline character varying(250);
ALTER TABLE tm_dataloader.wt_trial_nodes
  ADD COLUMN timestamp_baseline character varying(250);
