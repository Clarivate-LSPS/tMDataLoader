DO $$
BEGIN

CREATE TABLE IF NOT EXISTS tm_lz.lt_src_metabolomics_display_mapping (
    category_cd character varying(200) NOT NULL,
    display_value character varying(100),
    display_label character varying(200),
    display_unit character varying(20)
) TABLESPACE transmart;

END;
$$;

alter table tm_lz.lt_src_metabolomics_display_mapping owner to tm_lz;
grant all on tm_lz.lt_src_metabolomics_display_mapping to tm_cz;
