DO $$
BEGIN
	IF EXISTS(SELECT schema_name
								FROM information_schema.schemata
								WHERE schema_name = 'gwas_plink')
	THEN
		RETURN;
	END IF;

	CREATE SCHEMA gwas_plink;

	CREATE TABLE gwas_plink.plink_data (
		PLINK_DATA_ID SERIAL,
		STUDY_ID      VARCHAR(50) NOT NULL UNIQUE,
		BED           BYTEA,
		BIM           BYTEA,
		FAM           BYTEA
	);

	GRANT INSERT, DELETE, SELECT ON gwas_plink.plink_data TO tm_dataloader;
	GRANT SELECT ON gwas_plink.plink_data TO biomart_user;
END
$$;