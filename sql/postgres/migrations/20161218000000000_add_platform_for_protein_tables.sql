DROP TABLE IF EXISTS TM_DATALOADER.WT_SUBJECT_PROTEOMICS_LOGS;
CREATE UNLOGGED TABLE TM_DATALOADER.WT_SUBJECT_PROTEOMICS_LOGS AS (SELECT * FROM tm_wz.WT_SUBJECT_PROTEOMICS_LOGS where 1=0);
alter table tm_dataloader.WT_SUBJECT_PROTEOMICS_LOGS owner to tm_dataloader;

DROP TABLE IF EXISTS TM_DATALOADER.WT_SUBJECT_PROTEOMICS_MED;
CREATE UNLOGGED TABLE TM_DATALOADER.WT_SUBJECT_PROTEOMICS_MED AS (SELECT * FROM tm_wz.WT_SUBJECT_PROTEOMICS_MED where 1=0);
alter table tm_dataloader.WT_SUBJECT_PROTEOMICS_MED owner to tm_dataloader;

DO $$
DECLARE
  cnt INTEGER;
BEGIN
  SELECT count(*) INTO cnt
    FROM information_schema.columns
   WHERE table_name = 'wt_subject_proteomics_logs' AND column_name = 'platform' and table_schema = 'tm_dataloader';
  IF (cnt = 0) THEN
    ALTER TABLE tm_dataloader.wt_subject_proteomics_logs ADD COLUMN platform CHARACTER VARYING(200);
  END IF;
  SELECT count(*) INTO cnt
    FROM information_schema.columns
   WHERE table_name = 'wt_subject_proteomics_med' AND column_name = 'platform' and table_schema = 'tm_dataloader';
  IF (cnt = 0) THEN
    ALTER TABLE tm_dataloader.wt_subject_proteomics_med ADD COLUMN platform CHARACTER VARYING(200);
  END IF;
END $$;

