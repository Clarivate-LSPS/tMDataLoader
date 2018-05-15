select '20170319100000000_CREATE_VISIT_DIMENSION_SEQUENCE.sql'
FROM dual;

CREATE SEQUENCE tm_dataloader.visit_dimension_seq  MINVALUE 1 MAXVALUE 999999999999999999999999 INCREMENT BY 1 START WITH 1 CACHE 2 NOORDER NOCYCLE;
/

GRANT SELECT ON tm_dataloader.visit_dimension_seq TO TM_DATALOADER;
GRANT SELECT ON tm_dataloader.visit_dimension_seq TO biomart_user;
GRANT SELECT ON tm_dataloader.visit_dimension_seq TO i2b2demodata;
/
