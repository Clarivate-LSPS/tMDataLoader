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
	alter table gwas_plink.plink_data owner to tm_dataloader;
END
$$;