CREATE SEQUENCE tm_dataloader.modifier_dimension_seq
  INCREMENT 1
  MINVALUE 0
  MAXVALUE 9223372036854775807
  START 0
  CACHE 1;
ALTER TABLE tm_dataloader.modifier_dimension_seq
  OWNER TO i2b2demodata;
GRANT ALL ON SEQUENCE tm_dataloader.modifier_dimension_seq TO i2b2demodata;
GRANT ALL ON SEQUENCE tm_dataloader.modifier_dimension_seq TO tm_cz;
GRANT ALL ON SEQUENCE tm_dataloader.modifier_dimension_seq TO biomart_user;