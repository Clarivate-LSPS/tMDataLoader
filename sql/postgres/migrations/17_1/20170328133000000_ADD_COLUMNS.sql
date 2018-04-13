DO $$
DECLARE
  cnt INTEGER;
BEGIN
  SELECT count(*)
  INTO cnt
  FROM information_schema.columns
  WHERE table_name = 'lt_src_clinical_data' AND column_name = 'start_date' and table_schema = 'tm_dataloader';
  IF (cnt = 0)
  THEN
    ALTER TABLE tm_dataloader.lt_src_clinical_data
      ADD COLUMN start_date CHARACTER VARYING(250);
  END IF;

  SELECT count(*)
  INTO cnt
  FROM information_schema.columns
  WHERE table_name = 'lt_src_clinical_data' AND column_name = 'instance_num' and table_schema = 'tm_dataloader';
  IF (cnt = 0)
  THEN
    ALTER TABLE tm_dataloader.lt_src_clinical_data
      ADD COLUMN instance_num CHARACTER VARYING(250);
  END IF;

  SELECT count(*)
  INTO cnt
  FROM information_schema.columns
  WHERE table_name = 'lt_src_clinical_data' AND column_name = 'trial_visit_label' and table_schema = 'tm_dataloader';
  IF (cnt = 0)
  THEN
    ALTER TABLE tm_dataloader.lt_src_clinical_data
      ADD COLUMN trial_visit_label CHARACTER VARYING(250);
  END IF;

  SELECT count(*)
  INTO cnt
  FROM information_schema.columns
  WHERE table_name = 'wrk_clinical_data' AND column_name = 'start_date' and table_schema = 'tm_dataloader';
  IF (cnt = 0)
  THEN
    ALTER TABLE tm_dataloader.wrk_clinical_data
      ADD COLUMN start_date CHARACTER VARYING(250);
  END IF;

  SELECT count(*)
  INTO cnt
  FROM information_schema.columns
  WHERE table_name = 'wrk_clinical_data' AND column_name = 'instance_num' and table_schema = 'tm_dataloader';
  IF (cnt = 0)
  THEN
    ALTER TABLE tm_dataloader.wrk_clinical_data
      ADD COLUMN instance_num CHARACTER VARYING(250);
  END IF;

  SELECT count(*)
  INTO cnt
  FROM information_schema.columns
  WHERE table_name = 'wrk_clinical_data' AND column_name = 'trial_visit_label' and table_schema = 'tm_dataloader';
  IF (cnt = 0)
  THEN
    ALTER TABLE tm_dataloader.wrk_clinical_data
      ADD COLUMN trial_visit_label CHARACTER VARYING(250);
  END IF;

END $$;

