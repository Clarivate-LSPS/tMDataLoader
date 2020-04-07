CREATE UNLOGGED TABLE IF NOT EXISTS tm_dataloader.lt_snp_calls_by_gsm
(
	GSM_NUM VARCHAR(10),
	SNP_NAME VARCHAR(100),
	SNP_CALLS VARCHAR(4)
);

alter table tm_dataloader.lt_snp_calls_by_gsm owner to tm_dataloader;

CREATE UNLOGGED TABLE IF NOT EXISTS tm_dataloader.lt_snp_copy_number
(
	GSM_NUM VARCHAR(10),
	SNP_NAME VARCHAR(50),
	CHROM VARCHAR(2),
	CHROM_POS NUMERIC(20,0),
	COPY_NUMBER DOUBLE PRECISION
);

alter table tm_dataloader.lt_snp_copy_number owner to tm_dataloader;

create UNLOGGED table IF NOT EXISTS tm_dataloader.lt_snp_gene_map(
  snp_name varchar,
  entrez_gene_id int
);

alter table tm_dataloader.lt_snp_gene_map owner to tm_dataloader;
