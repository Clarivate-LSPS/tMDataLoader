CREATE UNLOGGED TABLE tm_lz.lt_snp_calls_by_gsm
(	GSM_NUM VARCHAR(10),
	SNP_NAME VARCHAR(100),
	SNP_CALLS VARCHAR(4)
)
TABLESPACE "transmart" ;

CREATE UNLOGGED TABLE tm_lz.lt_snp_copy_number
(
GSM_NUM VARCHAR(10),
	SNP_NAME VARCHAR(50),
	CHROM VARCHAR(2),
	CHROM_POS NUMERIC(20,0),
	COPY_NUMBER DOUBLE PRECISION
  
)
TABLESPACE "transmart";

create UNLOGGED table tm_lz.lt_snp_gene_map(
  snp_name varchar,
  entrez_gene_id bigint
)
TABLESPACE "transmart";