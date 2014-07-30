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
  TABLESPACE "deapp";
END IF;

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


IF NOT EXISTS (select 1 from pg_tables where schemaname = 'deapp' and tablename='de_variant_subject_idx') THEN
  --
  -- Name: de_variant_subject_idx; Type: TABLE; Schema: deapp; Owner: -
  --
  CREATE TABLE IF NOT EXISTS deapp.de_variant_subject_idx (
      dataset_id character varying(50),
      subject_id character varying(50),
      "position" bigint,
      variant_subject_idx_id bigint DEFAULT nextval('deapp.de_variant_subject_idx_seq'::regclass)
  )
  TABLESPACE "deapp";

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
    USING btree (dataset_id, subject_id, "position")
    TABLESPACE "indx";
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

IF NOT EXISTS(SELECT 1
              FROM   pg_class     c
              JOIN   pg_namespace n ON n.oid = c.relnamespace
              WHERE  c.relname = 'de_variant_subject_summary_seq'
              AND    n.nspname = 'deapp') THEN
  --
  -- Name: de_variant_subject_summary_seq; Type: SEQUENCE; Schema: deapp; Owner: -
  --
  CREATE SEQUENCE deapp.de_variant_subject_summary_seq
      START WITH 1
      INCREMENT BY 1
      NO MINVALUE
      NO MAXVALUE
      CACHE 1;
END IF;

IF NOT EXISTS (select 1 from pg_tables where schemaname = 'deapp' and tablename='de_variant_subject_summary') THEN
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
  TABLESPACE "deapp";

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
  USING btree (dataset_id, chr, pos, rs_id, subject_id)
  TABLESPACE "indx";

  --
  -- Name: variant_subject_summary_fk; Type: FK CONSTRAINT; Schema: deapp; Owner: -
  --
  ALTER TABLE ONLY deapp.de_variant_subject_summary
  ADD CONSTRAINT variant_subject_summary_fk FOREIGN KEY (dataset_id) REFERENCES deapp.de_variant_dataset(dataset_id);
END IF;

IF NOT EXISTS(SELECT 1
              FROM   pg_class     c
              JOIN   pg_namespace n ON n.oid = c.relnamespace
              WHERE  c.relname = 'de_variant_subject_detail_seq'
              AND    n.nspname = 'deapp') THEN
  --
  -- Name: de_variant_subject_detail_seq; Type: SEQUENCE; Schema: deapp; Owner: -
  --
  CREATE SEQUENCE deapp.de_variant_subject_detail_seq
      START WITH 1
      INCREMENT BY 1
      NO MINVALUE
      NO MAXVALUE
      CACHE 1;
END IF;

IF NOT EXISTS (select 1 from pg_tables where schemaname = 'deapp' and tablename='de_variant_subject_detail') THEN
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
  TABLESPACE "deapp";

  --
  -- Name: variant_subject_detail_id; Type: CONSTRAINT; Schema: deapp; Owner: -
  --
  ALTER TABLE ONLY deapp.de_variant_subject_detail
      ADD CONSTRAINT variant_subject_detail_id PRIMARY KEY (variant_subject_detail_id);

  --
  -- Name: de_variant_sub_detail_idx2; Type: INDEX; Schema: deapp; Owner: -
  --
  CREATE INDEX de_variant_sub_detail_idx2
  ON deapp.de_variant_subject_detail
  USING btree (dataset_id, chr)
  TABLESPACE "indx";

  --
  -- Name: de_variant_sub_dt_idx1; Type: INDEX; Schema: deapp; Owner: -
  --
  CREATE INDEX de_variant_sub_dt_idx1
  ON deapp.de_variant_subject_detail
  USING btree (dataset_id, rs_id)
  TABLESPACE "indx";

  --
  -- Name: variant_subject_detail_uk; Type: INDEX; Schema: deapp; Owner: -
  --
  CREATE UNIQUE INDEX variant_subject_detail_uk
  ON deapp.de_variant_subject_detail
  USING btree (dataset_id, chr, pos, rs_id)
  TABLESPACE "indx";

  --
  -- Name: variant_subject_detail_fk; Type: FK CONSTRAINT; Schema: deapp; Owner: -
  --
  ALTER TABLE ONLY deapp.de_variant_subject_detail
      ADD CONSTRAINT variant_subject_detail_fk FOREIGN KEY (dataset_id) REFERENCES deapp.de_variant_dataset(dataset_id);

END IF;

IF NOT EXISTS(SELECT 1
              FROM   pg_class     c
              JOIN   pg_namespace n ON n.oid = c.relnamespace
              WHERE  c.relname = 'de_variant_population_data_seq'
              AND    n.nspname = 'deapp') THEN
  --
  -- Name: deapp.de_variant_population_data_seq; Type: SEQUENCE; Schema: deapp; Owner: -
  --
  CREATE SEQUENCE deapp.de_variant_population_data_seq
      START WITH 1
      INCREMENT BY 1
      NO MINVALUE
      NO MAXVALUE
      CACHE 1;
END IF;

IF NOT EXISTS (select 1 from pg_tables where schemaname = 'deapp' and tablename='de_variant_population_data') THEN
  --
  -- Name: deapp.de_variant_population_data; Type: TABLE; Schema: deapp; Owner: -
  --
  CREATE TABLE deapp.de_variant_population_data (
      variant_population_data_id bigint DEFAULT nextval('deapp.de_variant_population_data_seq'::regclass) NOT NULL,
      dataset_id character varying(50),
      chr character varying(50),
      pos bigint,
      info_name character varying(100),
      info_index integer DEFAULT 0,
      integer_value bigint,
      float_value double precision,
      text_value character varying(5000)
  )
  TABLESPACE "deapp";

  --
  -- Name: deapp.de_variant_population_data_id_idx; Type: CONSTRAINT; Schema: deapp; Owner: -
  --
  ALTER TABLE ONLY deapp.de_variant_population_data
      ADD CONSTRAINT de_variant_population_data_id_idx PRIMARY KEY (variant_population_data_id);

  --
  -- Name: de_variant_population_data_default_idx; Type: INDEX; Schema: deapp; Owner: -
  --
  CREATE INDEX de_variant_population_data_default_idx
  ON deapp.de_variant_population_data
  USING btree (dataset_id, chr, pos, info_name)
  TABLESPACE "indx";

  --
  -- Name: de_variant_population_data_fk; Type: FK CONSTRAINT; Schema: deapp; Owner: -
  --
  ALTER TABLE ONLY deapp.de_variant_population_data
      ADD CONSTRAINT de_variant_population_data_fk FOREIGN KEY (dataset_id) REFERENCES deapp.de_variant_dataset(dataset_id);
END IF;

IF NOT EXISTS(SELECT 1
              FROM   pg_class     c
              JOIN   pg_namespace n ON n.oid = c.relnamespace
              WHERE  c.relname = 'de_variant_population_info_seq'
              AND    n.nspname = 'deapp') THEN
  --
  -- Name: deapp.de_variant_population_info_seq; Type: SEQUENCE; Schema: deapp; Owner: -
  --
  CREATE SEQUENCE deapp.de_variant_population_info_seq
      START WITH 1
      INCREMENT BY 1
      NO MINVALUE
      NO MAXVALUE
      CACHE 1;
END IF;

IF NOT EXISTS (select 1 from pg_tables where schemaname = 'deapp' and tablename='de_variant_population_info') THEN
  --
  -- Name: deapp.de_variant_population_info; Type: TABLE; Schema: deapp; Owner: -
  --
  CREATE TABLE deapp.de_variant_population_info (
      variant_population_info_id bigint DEFAULT nextval('deapp.de_variant_population_info_seq'::regclass) NOT NULL,
      dataset_id character varying(50),
      info_name character varying(100),
      description text,
      type character varying(30),
      number character varying(10)
  )
  TABLESPACE "deapp";

  --
  -- Name: de_variant_population_info_id_idx; Type: CONSTRAINT; Schema: deapp; Owner: -
  --
  ALTER TABLE ONLY deapp.de_variant_population_info
      ADD CONSTRAINT de_variant_population_info_id_idx PRIMARY KEY (variant_population_info_id);

  --
  -- Name: variant_population_info_dataset_name; Type: INDEX; Schema: deapp; Owner: -
  --
  CREATE INDEX variant_population_info_dataset_name
  ON deapp.de_variant_population_info
  USING btree (dataset_id, info_name)
  TABLESPACE "indx";

  --
  -- Name: de_variant_population_info_fk; Type: FK CONSTRAINT; Schema: deapp; Owner: -
  --
  ALTER TABLE ONLY deapp.de_variant_population_info
      ADD CONSTRAINT de_variant_population_info_fk FOREIGN KEY (dataset_id) REFERENCES deapp.de_variant_dataset(dataset_id);
END IF;

END;
$$;

alter table deapp.de_variant_dataset owner to deapp;
alter table deapp.de_variant_population_data owner to deapp;
alter table deapp.de_variant_population_info owner to deapp;
alter table deapp.de_variant_subject_detail owner to deapp;
alter table deapp.de_variant_subject_idx owner to deapp;
alter table deapp.de_variant_subject_summary owner to deapp;