DO $$
BEGIN
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

END;
$$;