DROP TABLE IF EXISTS TM_DATALOADER.I2B2_LOAD_PATH;

CREATE UNLOGGED TABLE TM_DATALOADER.I2B2_LOAD_PATH
(
  PATH character varying(700) PRIMARY KEY,
  PATH50 character varying(50),
  PATH100 character varying(100),
  PATH150 character varying(150),
  PATH200 character varying(200),
  PATH_LEN integer,
  RECORD_ID integer
);
alter table tm_dataloader.i2b2_load_path owner to tm_dataloader;

CREATE INDEX TM_WZ_IDX_PATH_LEN ON TM_DATALOADER.I2B2_LOAD_PATH (PATH_LEN, PATH varchar_pattern_ops, RECORD_ID);
CREATE INDEX TM_WZ_IDX_PATH_LEN50 ON TM_DATALOADER.I2B2_LOAD_PATH (PATH_LEN, PATH50 varchar_pattern_ops, RECORD_ID);
CREATE INDEX TM_WZ_IDX_PATH_LEN100 ON TM_DATALOADER.I2B2_LOAD_PATH (PATH_LEN, PATH100 varchar_pattern_ops, RECORD_ID);
CREATE INDEX TM_WZ_IDX_PATH_LEN150 ON TM_DATALOADER.I2B2_LOAD_PATH (PATH_LEN, PATH150 varchar_pattern_ops, RECORD_ID);
CREATE INDEX TM_WZ_IDX_PATH_LEN200 ON TM_DATALOADER.I2B2_LOAD_PATH (PATH_LEN, PATH200 varchar_pattern_ops, RECORD_ID);

CREATE INDEX TM_WZ_IDX_PATH ON TM_DATALOADER.I2B2_LOAD_PATH (PATH varchar_pattern_ops, RECORD_ID);
CREATE INDEX TM_WZ_IDX_PATH50 ON TM_DATALOADER.I2B2_LOAD_PATH (PATH50 varchar_pattern_ops, RECORD_ID);
CREATE INDEX TM_WZ_IDX_PATH100 ON TM_DATALOADER.I2B2_LOAD_PATH (PATH100 varchar_pattern_ops, RECORD_ID);
CREATE INDEX TM_WZ_IDX_PATH150 ON TM_DATALOADER.I2B2_LOAD_PATH (PATH150 varchar_pattern_ops, RECORD_ID);
CREATE INDEX TM_WZ_IDX_PATH200 ON TM_DATALOADER.I2B2_LOAD_PATH (PATH200 varchar_pattern_ops, RECORD_ID);

DROP TABLE IF EXISTS TM_DATALOADER.I2B2_LOAD_TREE_FULL;
CREATE UNLOGGED TABLE TM_DATALOADER.I2B2_LOAD_TREE_FULL
(
  IDROOT          integer,
  IDCHILD         integer
);
alter table tm_dataloader.i2b2_load_tree_full owner to tm_dataloader;

CREATE INDEX TM_WZ_IDX_ROOT ON TM_DATALOADER.I2B2_LOAD_TREE_FULL (IDROOT, IDCHILD);
CREATE INDEX TM_WZ_IDX_CHILD ON TM_DATALOADER.I2B2_LOAD_TREE_FULL (IDCHILD, IDROOT);

DROP TABLE IF EXISTS TM_DATALOADER.I2B2_LOAD_PATH_WITH_COUNT;
CREATE UNLOGGED TABLE TM_DATALOADER.I2B2_LOAD_PATH_WITH_COUNT
(
  C_FULLNAME          character varying(2000) PRIMARY KEY,
  NBR_CHILDREN        INTEGER
);
alter table tm_dataloader.i2b2_load_path_with_count owner to tm_dataloader;

CREATE INDEX TM_WZ_IDX_PATH_COUNT ON TM_DATALOADER.I2B2_LOAD_PATH_WITH_COUNT (C_FULLNAME);

DROP TABLE IF EXISTS TM_DATALOADER.wt_trial_nodes;
CREATE UNLOGGED TABLE TM_DATALOADER.wt_trial_nodes AS (SELECT * FROM tm_wz.wt_trial_nodes where 1=0);
alter table tm_dataloader.wt_trial_nodes owner to tm_dataloader;

CREATE INDEX IDX_WTN_LOAD_CLINICAL ON TM_DATALOADER.WT_TRIAL_NODES(LEAF_NODE,CATEGORY_CD,DATA_LABEL);
CREATE INDEX IDX_WT_TRIALNODES ON TM_DATALOADER.WT_TRIAL_NODES(LEAF_NODE,NODE_NAME);

ALTER TABLE TM_DATALOADER.wt_trial_nodes ALTER COLUMN category_cd TYPE character varying(250);

DROP TABLE IF EXISTS TM_DATALOADER.wt_num_data_types;
CREATE UNLOGGED TABLE TM_DATALOADER.wt_num_data_types AS (SELECT * FROM tm_wz.wt_num_data_types where 1=0);
alter table tm_dataloader.wt_num_data_types owner to tm_dataloader;

ALTER TABLE TM_DATALOADER.wt_num_data_types ALTER COLUMN category_cd TYPE character varying(250);

DROP TABLE IF EXISTS TM_DATALOADER.WRK_CLINICAL_DATA;
CREATE UNLOGGED TABLE TM_DATALOADER.WRK_CLINICAL_DATA AS (SELECT * FROM tm_wz.wrk_clinical_data where 1=0);
alter table tm_dataloader.wrk_clinical_data owner to tm_dataloader;

CREATE INDEX IDX_WRK_CLN_ID_VALUE ON TM_DATALOADER.WRK_CLINICAL_DATA(usubjid, data_value, data_type);

DROP TABLE IF EXISTS TM_DATALOADER.lt_src_clinical_data;
CREATE UNLOGGED TABLE TM_DATALOADER.lt_src_clinical_data AS (SELECT * FROM tm_lz.lt_src_clinical_data where 1=0);
alter table tm_dataloader.lt_src_clinical_data owner to tm_dataloader;

DROP TABLE IF EXISTS TM_DATALOADER.lt_src_mrna_subj_samp_map;
CREATE UNLOGGED TABLE TM_DATALOADER.lt_src_mrna_subj_samp_map AS (SELECT * FROM tm_lz.lt_src_mrna_subj_samp_map where 1=0);
alter table tm_dataloader.lt_src_mrna_subj_samp_map owner to tm_dataloader;

DROP TABLE IF EXISTS TM_DATALOADER.lt_src_mrna_data;
CREATE UNLOGGED TABLE TM_DATALOADER.lt_src_mrna_data AS (SELECT * FROM tm_lz.lt_src_mrna_data where 1=0);
alter table tm_dataloader.lt_src_mrna_data owner to tm_dataloader;

DROP TABLE IF EXISTS TM_DATALOADER.lt_src_deapp_annot;
CREATE UNLOGGED TABLE TM_DATALOADER.lt_src_deapp_annot AS (SELECT * FROM tm_lz.lt_src_deapp_annot where 1=0);
alter table tm_dataloader.lt_src_deapp_annot owner to tm_dataloader;

