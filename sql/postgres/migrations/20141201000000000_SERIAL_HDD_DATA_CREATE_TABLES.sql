DO $$
BEGIN

IF NOT EXISTS (select 1 from pg_tables where schemaname = 'tm_dataloader' and tablename='lt_src_mrna_xml_data') THEN
  CREATE UNLOGGED TABLE IF NOT EXISTS tm_dataloader.lt_src_mrna_xml_data (
      STUDY_ID character varying(50) NOT NULL,
      CATEGORY_CD character varying(2000) NOT NULL,
      C_METADATAXML text NOT NULL
  );
END IF;

END;
$$;

alter table tm_dataloader.lt_src_mrna_xml_data owner to tm_dataloader;
