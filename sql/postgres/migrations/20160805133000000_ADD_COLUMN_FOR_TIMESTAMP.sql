DO $$
DECLARE
  cnt INTEGER;
BEGIN
  SELECT count(*)
  INTO cnt
  FROM information_schema.columns
  WHERE table_name = 'lt_src_clinical_data' AND column_name = 'baseline_value' and table_schema = 'tm_dataloader';
  IF (cnt = 0)
  THEN
    ALTER TABLE tm_dataloader.lt_src_clinical_data
      ADD COLUMN baseline_value CHARACTER VARYING(250);
  END IF;

  SELECT count(*)
  INTO cnt
  FROM information_schema.columns
  WHERE table_name = 'wrk_clinical_data' AND column_name = 'baseline_value' and table_schema = 'tm_dataloader';
  IF (cnt = 0)
  THEN
    ALTER TABLE tm_dataloader.wrk_clinical_data
      ADD COLUMN baseline_value CHARACTER VARYING(250);
  END IF;

  SELECT count(*)
  INTO cnt
  FROM information_schema.columns
  WHERE table_name = 'wt_trial_nodes' AND column_name = 'baseline_value' and table_schema = 'tm_dataloader';
  IF (cnt = 0)
  THEN
    ALTER TABLE tm_dataloader.wt_trial_nodes
      ADD COLUMN baseline_value CHARACTER VARYING(250);
  END IF;
END $$;

