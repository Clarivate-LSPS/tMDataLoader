DO $$
BEGIN

--
-- Name: wt_qpcr_mirna_nodes; Type: TABLE; Schema: tm_wz; Owner: -
--
IF NOT EXISTS (select 1 from pg_tables where schemaname = 'tm_wz' and tablename='wt_qpcr_mirna_nodes') THEN
    CREATE TABLE IF NOT EXISTS tm_wz.wt_qpcr_mirna_nodes (
        leaf_node character varying(2000),
        category_cd character varying(2000),
        platform character varying(2000),
        tissue_type character varying(2000),
        attribute_1 character varying(2000),
        attribute_2 character varying(2000),
        title character varying(2000),
        node_name character varying(2000),
        concept_cd character varying(100),
        transform_method character varying(2000),
        node_type character varying(50)
    ) TABLESPACE "transmart";
END IF;

--
-- Name: lt_src_mirna_subj_samp_map; Type: TABLE; Schema: tm_lz; Owner: -
--
IF NOT EXISTS (select 1 from pg_tables where schemaname = 'tm_lz' and tablename='lt_src_mirna_subj_samp_map') THEN
    CREATE TABLE IF NOT EXISTS tm_lz.lt_src_mirna_subj_samp_map (
        trial_name character varying(100),
        site_id character varying(100),
        subject_id character varying(100),
        sample_cd character varying(100),
        platform character varying(100),
        tissue_type character varying(100),
        attribute_1 character varying(256),
        attribute_2 character varying(200),
        category_cd character varying(200),
        source_cd character varying(200)
    ) TABLESPACE "transmart";
END IF;

--
-- Name: de_qpcr_mirna_annotation; Type: TABLE; Schema: deapp; Owner: -
--
IF NOT EXISTS (select 1 from pg_tables where schemaname = 'deapp' and tablename='de_qpcr_mirna_annotation') THEN
    CREATE TABLE IF NOT EXISTS deapp.de_qpcr_mirna_annotation (
        id_ref character varying(100),
        probe_id character varying(100),
        mirna_symbol character varying(100),
        mirna_id character varying(100),
        probeset_id numeric(38,0),
        organism character varying(200),
        gpl_id character varying(20)
    ) TABLESPACE "deapp";
END IF;

--
-- Name: de_subject_mirna_data; Type: TABLE; Schema: deapp; Owner: -
--
IF NOT EXISTS (select 1 from pg_tables where schemaname = 'deapp' and tablename='de_subject_mirna_data') THEN
    CREATE TABLE IF NOT EXISTS deapp.de_subject_mirna_data (
        trial_source character varying(200),
        trial_name character varying(50),
        assay_id numeric(18,0),
        patient_id numeric(18,0),
        raw_intensity numeric,
        log_intensity numeric,
        probeset_id numeric(38,0),
        zscore numeric(18,9)
    ) TABLESPACE "deapp";
END IF;

--
-- Name: wt_qpcr_mirna_node_values; Type: TABLE; Schema: tm_wz; Owner: -
--
IF NOT EXISTS (select 1 from pg_tables where schemaname = 'tm_wz' and tablename='wt_qpcr_mirna_node_values') THEN
    CREATE TABLE IF NOT EXISTS tm_wz.wt_qpcr_mirna_node_values (
        category_cd character varying(2000),
        platform character varying(2000),
        tissue_type character varying(2000),
        attribute_1 character varying(2000),
        attribute_2 character varying(2000),
        title character varying(2000),
        transform_method character varying(2000)
    ) TABLESPACE "transmart";
END IF;

--
-- Name: lt_src_qpcr_mirna_data; Type: TABLE; Schema: tm_lz; Owner: -
--
IF NOT EXISTS (select 1 from pg_tables where schemaname = 'tm_lz' and tablename='lt_src_qpcr_mirna_data') THEN
    CREATE TABLE IF NOT EXISTS tm_lz.lt_src_qpcr_mirna_data (
        trial_name character varying(25),
        probeset character varying(100),
        expr_id character varying(100),
        intensity_value character varying(50)
    ) TABLESPACE "transmart";
END IF;

--
-- Name: lt_src_mirna_display_mapping; Type: TABLE; Schema: tm_lz; Owner: -
--
IF NOT EXISTS (select 1 from pg_tables where schemaname = 'tm_lz' and tablename='lt_src_mirna_display_mapping') THEN
    CREATE TABLE IF NOT EXISTS tm_lz.lt_src_mirna_display_mapping (
        category_cd character varying(200) NOT NULL,
        display_value character varying(100),
        display_label character varying(200),
        display_unit character varying(20)
    ) TABLESPACE "transmart";
END IF;

--
-- Name: lt_qpcr_mirna_annotation; Type: TABLE; Schema: tm_lz; Owner: -
--
IF NOT EXISTS (select 1 from pg_tables where schemaname = 'tm_lz' and tablename='lt_qpcr_mirna_annotation') THEN
    CREATE TABLE tm_lz.lt_qpcr_mirna_annotation (
        id_ref character varying(100),
        mirna_id character varying(100),
        sn_id character varying(100),
        organism character varying(1000),
        gpl_id character varying(20)
    ) TABLESPACE "transmart";
END IF;

                  --
-- Name: mirna_annotation_deapp; Type: TABLE; Schema: tm_cz; Owner: -
--
IF NOT EXISTS (select 1 from pg_tables where schemaname = 'tm_cz' and tablename='mirna_annotation_deapp') THEN
    CREATE TABLE tm_cz.mirna_annotation_deapp (
        id_ref character varying(100),
        probe_id character varying(100),
        mirna_symbol character varying(100),
        mirna_id character varying(100),
        probeset_id numeric(38,0),
        organism character varying(200),
        gpl_id character varying(20)
    ) TABLESPACE "transmart";
END IF;


END;
$$;

alter table tm_wz.wt_qpcr_mirna_nodes owner to tm_wz;
alter table tm_lz.lt_src_mirna_subj_samp_map owner to tm_lz;
alter table deapp.de_qpcr_mirna_annotation owner to deapp;
alter table deapp.de_subject_mirna_data owner to deapp;
alter table tm_wz.wt_qpcr_mirna_node_values owner to tm_wz;
alter table tm_lz.lt_src_qpcr_mirna_data owner to tm_lz;
alter table tm_lz.lt_src_mirna_display_mapping owner to tm_lz;
alter table tm_lz.lt_qpcr_mirna_annotation owner to tm_lz;
alter table tm_cz.mirna_annotation_deapp owner to tm_cz;
