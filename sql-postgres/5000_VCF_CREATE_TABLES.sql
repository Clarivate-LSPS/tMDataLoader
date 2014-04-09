DO $$
BEGIN

IF NOT EXISTS (select 1 from pg_tables where schemaname = 'deapp' and tablename='de_variant_dataset') THEN
  --
  -- Name: de_variant_dataset; Type: TABLE; Schema: deapp; Owner: -
  --
  CREATE TABLE IF NOT EXISTS deapp.de_variant_dataset (
      dataset_id character varying(50) NOT NULL PRIMARY KEY,
      datasource_id character varying(200),
      etl_id character varying(20),
      etl_date date,
      genome character varying(50) NOT NULL,
      metadata_comment text,
      variant_dataset_type character varying(50)
  )
  TABLESPACE "transmart";
END IF;

IF NOT EXISTS (select 1 from pg_tables where schemaname = 'deapp' and tablename='de_variant_subject_idx') THEN
  --
  -- Name: de_variant_subject_idx_seq; Type: SEQUENCE; Schema: deapp; Owner: -
  --
  IF NOT EXISTS(SELECT 1
                FROM   pg_class     c
                JOIN   pg_namespace n ON n.oid = c.relnamespace
                WHERE  c.relname = 'de_variant_subject_idx_seq'
                AND    n.nspname = 'deapp') THEN
    CREATE SEQUENCE deapp.de_variant_subject_idx_seq
        START WITH 1
        INCREMENT BY 1
        NO MINVALUE
        NO MAXVALUE
        CACHE 1;
  END IF;

  --
  -- Name: de_variant_subject_idx; Type: TABLE; Schema: deapp; Owner: -
  --
  CREATE TABLE IF NOT EXISTS deapp.de_variant_subject_idx (
      dataset_id character varying(50) DEFAULT nextval('deapp.de_variant_subject_idx_seq'::regclass),
      subject_id character varying(50),
      "position" bigint,
      variant_subject_idx_id bigint
  )
  TABLESPACE "transmart";

  --
  -- Name: variant_subject_idx_uk; Type: INDEX; Schema: deapp; Owner: -
  --
  IF NOT EXISTS(SELECT 1
                FROM   pg_class     c
                JOIN   pg_namespace n ON n.oid = c.relnamespace
                WHERE  c.relname = 'variant_subject_idx_uk'
                AND    n.nspname = 'deapp') THEN
    CREATE UNIQUE INDEX variant_subject_idx_uk
    ON deapp.de_variant_subject_idx
    USING btree (dataset_id, subject_id, "position");
  END IF;


  --
  -- Name: variant_subject_idx_fk; Type: FK CONSTRAINT; Schema: deapp; Owner: -
  --
  IF NOT EXISTS(SELECT 1
                FROM   pg_constraint     c
                WHERE  c.conname = 'variant_subject_idx_fk') THEN
    ALTER TABLE ONLY deapp.de_variant_subject_idx
      ADD CONSTRAINT variant_subject_idx_fk FOREIGN KEY (dataset_id) REFERENCES deapp.de_variant_dataset(dataset_id);
  END IF;
END IF;

IF NOT EXISTS (select 1 from pg_tables where schemaname = 'deapp' and tablename='de_variant_subject_summary') THEN
  --
  -- Name: de_variant_subject_summary_seq; Type: SEQUENCE; Schema: deapp; Owner: -
  --
  CREATE SEQUENCE deapp.de_variant_subject_summary_seq
      START WITH 1
      INCREMENT BY 1
      NO MINVALUE
      NO MAXVALUE
      CACHE 1;

  --
  -- Name: de_variant_subject_summary; Type: TABLE; Schema: deapp; Owner: -
  --
  CREATE TABLE deapp.de_variant_subject_summary (
      variant_subject_summary_id bigint DEFAULT nextval('deapp.de_variant_subject_summary_seq'::regclass) NOT NULL,
      chr character varying(50),
      pos bigint,
      dataset_id character varying(50) NOT NULL,
      subject_id character varying(50) NOT NULL,
      rs_id character varying(50),
      variant character varying(1000),
      variant_format character varying(100),
      variant_type character varying(100),
      reference boolean,
      allele1 integer,
      allele2 integer,
      assay_id bigint
  )
  TABLESPACE "transmart";

  --
  -- Name: COLUMN de_variant_subject_summary.reference; Type: COMMENT; Schema: deapp; Owner: -
  --
  COMMENT ON COLUMN deapp.de_variant_subject_summary.reference
  IS 'This column contains a flag whether this subject has a reference value on this variant, or not.';

  --
  -- Name: COLUMN de_variant_subject_summary.assay_id; Type: COMMENT; Schema: deapp; Owner: -
  --
  COMMENT ON COLUMN deapp.de_variant_subject_summary.assay_id
  IS 'Reference to deapp.de_subject_sample_mapping';

  --
  -- Name: variant_subject_summary_id; Type: CONSTRAINT; Schema: deapp; Owner: -
  --
  ALTER TABLE ONLY deapp.de_variant_subject_summary
  ADD CONSTRAINT variant_subject_summary_id PRIMARY KEY (variant_subject_summary_id);

  --
  -- Name: variant_subject_summary_uk; Type: INDEX; Schema: deapp; Owner: -
  --
  CREATE UNIQUE INDEX variant_subject_summary_uk
  ON deapp.de_variant_subject_summary
  USING btree (dataset_id, chr, pos, rs_id, subject_id);

  --
  -- Name: variant_subject_summary_fk; Type: FK CONSTRAINT; Schema: deapp; Owner: -
  --
  ALTER TABLE ONLY deapp.de_variant_subject_summary
  ADD CONSTRAINT variant_subject_summary_fk FOREIGN KEY (dataset_id) REFERENCES deapp.de_variant_dataset(dataset_id);
END IF;

IF NOT EXISTS (select 1 from pg_tables where schemaname = 'deapp' and tablename='de_variant_subject_detail') THEN
  --
  -- Name: de_variant_subject_detail_seq; Type: SEQUENCE; Schema: deapp; Owner: -
  --
  CREATE SEQUENCE deapp.de_variant_subject_detail_seq
      START WITH 1
      INCREMENT BY 1
      NO MINVALUE
      NO MAXVALUE
      CACHE 1;

  --
  -- Name: deapp.de_variant_subject_detail; Type: TABLE; Schema: deapp; Owner: -
  --
  CREATE TABLE deapp.de_variant_subject_detail (
      variant_subject_detail_id bigint DEFAULT nextval('deapp.de_variant_subject_detail_seq'::regclass) NOT NULL,
      dataset_id character varying(50),
      chr character varying(50),
      pos bigint,
      rs_id character varying(50),
      ref character varying(500),
      alt character varying(500),
      qual character varying(100),
      filter character varying(50),
      info character varying(5000),
      format character varying(500),
      variant_value text
  )
  TABLESPACE "transmart";

  --
  -- Name: variant_subject_detail_id; Type: CONSTRAINT; Schema: deapp; Owner: -
  --
  ALTER TABLE ONLY deapp.de_variant_subject_detail
      ADD CONSTRAINT variant_subject_detail_id PRIMARY KEY (variant_subject_detail_id);

  --
  -- Name: de_variant_sub_detail_idx2; Type: INDEX; Schema: deapp; Owner: -
  --
  CREATE INDEX de_variant_sub_detail_idx2 ON deapp.de_variant_subject_detail USING btree (dataset_id, chr);

  --
  -- Name: de_variant_sub_dt_idx1; Type: INDEX; Schema: deapp; Owner: -
  --
  CREATE INDEX de_variant_sub_dt_idx1 ON deapp.de_variant_subject_detail USING btree (dataset_id, rs_id);

  --
  -- Name: variant_subject_detail_uk; Type: INDEX; Schema: deapp; Owner: -
  --
  CREATE UNIQUE INDEX variant_subject_detail_uk ON deapp.de_variant_subject_detail USING btree (dataset_id, chr, pos, rs_id);

  --
  -- Name: variant_subject_detail_fk; Type: FK CONSTRAINT; Schema: deapp; Owner: -
  --
  ALTER TABLE ONLY deapp.de_variant_subject_detail
      ADD CONSTRAINT variant_subject_detail_fk FOREIGN KEY (dataset_id) REFERENCES deapp.de_variant_dataset(dataset_id);

END IF;

END;
$$;