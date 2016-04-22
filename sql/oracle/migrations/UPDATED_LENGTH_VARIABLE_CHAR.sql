-- ----------------------------------
-- UPDATED_LENGTH_VARIABLE_CHAR.sql
-- ----------------------------------
select 'UPDATED_LENGTH_VARIABLE_CHAR.sql' FROM DUAL;

ALTER TABLE tm_dataloader.lt_src_deapp_annot
   modify gene_symbol VARCHAR2(400);

ALTER TABLE tm_cz.annotation_deapp
   MODIFY gene_symbol VARCHAR2(400);

ALTER TABLE deapp.de_mrna_annotation
   MODIFY gene_symbol VARCHAR2(400);

ALTER TABLE tm_dataloader.lt_src_deapp_annot
   MODIFY probe_id VARCHAR2(200);

ALTER TABLE deapp.de_mrna_annotation
   MODIFY probe_id VARCHAR2(200);

ALTER TABLE tm_cz.annotation_deapp
   MODIFY probe_id VARCHAR2(200);

