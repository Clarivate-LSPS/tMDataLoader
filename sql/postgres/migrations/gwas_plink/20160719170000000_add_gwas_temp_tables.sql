DO $$
BEGIN
  IF EXISTS(SELECT 1
            FROM pg_tables
            WHERE schemaname = 'tm_dataloader'
                  AND tablename = 'lt_src_gwas_data')
  THEN
    RETURN;
  END IF;
  CREATE TABLE tm_dataloader.lt_src_gwas_data
  (
    study_id    CHARACTER VARYING(25),
    subject_id  CHARACTER VARYING(30),
    category_cd CHARACTER VARYING(250),
    data_label  CHARACTER VARYING(500)
  )
  WITH (
  OIDS =FALSE
  );
  ALTER TABLE tm_dataloader.lt_src_gwas_data
    OWNER TO postgres;

END
$$;