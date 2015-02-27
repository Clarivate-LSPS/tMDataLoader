ALTER TABLE tm_dataloader.lt_src_deapp_annot
   ALTER COLUMN gene_symbol TYPE character varying(400);

ALTER TABLE tm_cz.annotation_deapp
   ALTER COLUMN gene_symbol TYPE character varying(400);

ALTER TABLE deapp.de_mrna_annotation
   ALTER COLUMN gene_symbol TYPE character varying(400);

ALTER TABLE tm_dataloader.lt_src_deapp_annot
   ALTER COLUMN probe_id TYPE character varying(200);

ALTER TABLE deapp.de_mrna_annotation
   ALTER COLUMN probe_id TYPE character varying(200);

ALTER TABLE tm_cz.annotation_deapp
   ALTER COLUMN probe_id TYPE character varying(200);

