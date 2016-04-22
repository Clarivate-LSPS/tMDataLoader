-- -------------------------------------------
-- 20140724000004000_SNP_CREATE_TMP_TABLES.SQL
-- -------------------------------------------

SET SERVEROUTPUT ON SIZE UNLIMITED
SET HEAD OFF
SET LINESIZE 180

SELECT 'Create temp clinical data tables.' FROM DUAL

DECLARE
rows int;
drop_sql VARCHAR2(1000);
BEGIN
	SELECT COUNT(*)
	INTO rows
	FROM dba_tables
	WHERE owner='TM_DATALOADER'
	 and table_name = upper('lt_snp_calls_by_gsm');
 
 	IF rows > 0
 	THEN
	 	drop_sql := 'DROP TABLE tm_dataloader.lt_snp_calls_by_gsm';
		dbms_output.put_line(drop_sql);
 	    EXECUTE IMMEDIATE drop_sql;
 	END IF;
 END;
/

SELECT 'Create tm_dataloader.lt_snp_calls_by_gsm' FROM DUAL;

CREATE TABLE tm_dataloader.lt_snp_calls_by_gsm
(
	GSM_NUM VARCHAR2(10),
	SNP_NAME VARCHAR2(255),
	SNP_CALLS VARCHAR2(4)
) NOLOGGING;

DECLARE
rows int;
drop_sql VARCHAR2(1000);
BEGIN
	SELECT COUNT(*)
	INTO rows
	FROM dba_tables
	WHERE owner='TM_DATALOADER'
	 and table_name = upper('lt_snp_copy_number');
 
 	IF rows > 0
 	THEN
	 	drop_sql := 'DROP TABLE tm_dataloader.lt_snp_copy_number';
		dbms_output.put_line(drop_sql);
 	    EXECUTE IMMEDIATE drop_sql;
 	END IF;
 END;
/

SELECT 'Create table lt_snp_copy_number' FROM DUAL;

CREATE TABLE tm_dataloader.lt_snp_copy_number
(
	GSM_NUM VARCHAR2(10),
	SNP_NAME VARCHAR2(255),
	CHROM VARCHAR2(2),
	CHROM_POS NUMBER(20,0),
	COPY_NUMBER NUMBER
) NOLOGGING;

DECLARE
rows int;
drop_sql VARCHAR2(1000);
BEGIN
	SELECT COUNT(*)
	INTO rows
	FROM dba_tables
	WHERE owner='TM_DATALOADER'
	 and table_name = upper('lt_snp_gene_map');
 
 	IF rows > 0
 	THEN
	 	drop_sql := 'DROP TABLE tm_dataloader.lt_snp_gene_map';
		dbms_output.put_line(drop_sql);
 	    EXECUTE IMMEDIATE drop_sql;
 	END IF;
 END;
/

SELECT 'Creating table lt_snp_gene_map' FROM DUAL;

create table tm_dataloader.lt_snp_gene_map(
  snp_name VARCHAR2(255),
  entrez_gene_id NUMBER
) NOLOGGING;
