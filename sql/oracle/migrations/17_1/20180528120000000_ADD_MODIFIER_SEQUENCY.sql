select '20180528120000000_ADD_MODIFIER_SEQUENCY.sql'
FROM dual;

CREATE SEQUENCE tm_dataloader.modifier_dimension_seq  MINVALUE 1 MAXVALUE 999999999999999999999999 INCREMENT BY 1 START WITH 1 CACHE 2 NOORDER NOCYCLE;
/

GRANT SELECT ON tm_dataloader.modifier_dimension_seq TO TM_DATALOADER;
GRANT SELECT ON tm_dataloader.modifier_dimension_seq TO biomart_user;
GRANT SELECT ON tm_dataloader.modifier_dimension_seq TO i2b2demodata;
/
