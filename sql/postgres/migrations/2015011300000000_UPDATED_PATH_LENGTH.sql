ALTER TABLE i2b2demodata.concept_counts
   ALTER COLUMN concept_path TYPE character varying(2000);
ALTER TABLE i2b2demodata.concept_counts
   ALTER COLUMN parent_concept_path TYPE character varying(2000);
ALTER TABLE i2b2demodata.concept_dimenstion
	ALTER COLUMN concept_path TYPE character varying(2000);

DROP VIEW i2b2metadata.i2b2_trial_nodes;
ALTER TABLE i2b2metadata.i2b2 ALTER COLUMN c_fullname TYPE character varying(2000);
ALTER TABLE i2b2metadata.i2b2 ALTER COLUMN m_applied_path TYPE character varying(2000);
ALTER TABLE i2b2metadata.i2b2
   ALTER COLUMN c_dimcode TYPE character varying(2000);

ALTER TABLE i2b2metadata.i2b2
   ALTER COLUMN c_path TYPE character varying(2000);
   
CREATE OR REPLACE VIEW i2b2metadata.i2b2_trial_nodes AS 
 SELECT DISTINCT ON (i2b2.c_comment) i2b2.c_fullname,
    "substring"(i2b2.c_comment, 7) AS trial
   FROM i2b2metadata.i2b2
  WHERE i2b2.c_comment IS NOT NULL
  ORDER BY i2b2.c_comment, char_length(i2b2.c_fullname::text);

ALTER TABLE i2b2metadata.i2b2_trial_nodes
  OWNER TO i2b2metadata;
GRANT ALL ON TABLE i2b2metadata.i2b2_trial_nodes TO i2b2metadata;
GRANT ALL ON TABLE i2b2metadata.i2b2_trial_nodes TO tm_cz;
GRANT SELECT ON TABLE i2b2metadata.i2b2_trial_nodes TO biomart_user;	

ALTER TABLE i2b2metadata.i2b2_secure
   ALTER COLUMN c_fullname TYPE character varying(2000);
ALTER TABLE i2b2metadata.i2b2_secure
   ALTER COLUMN m_applied_path TYPE character varying(2000);
ALTER TABLE i2b2metadata.i2b2_secure
   ALTER COLUMN c_path TYPE character varying(2000);
