CREATE TABLE deapp.de_variant_dataset (
    dataset_id varchar2(50) NOT NULL PRIMARY KEY,
    datasource_id varchar2(200),
    etl_id varchar2(20),
    etl_date date,
    genome varchar2(50) NOT NULL,
    metadata_comment clob,
    variant_dataset_type varchar2(50)
)
TABLESPACE "TRANSMART";

CREATE SEQUENCE deapp.de_variant_subject_idx_seq
      START WITH 1
      INCREMENT BY 1
      NOMINVALUE
      NOMAXVALUE
      CACHE 2;

CREATE TABLE deapp.de_variant_subject_idx (
    dataset_id varchar2(50),
    subject_id varchar(50),
    position numeric(18),
    variant_subject_idx_id numeric(18)
)
TABLESPACE "TRANSMART";
