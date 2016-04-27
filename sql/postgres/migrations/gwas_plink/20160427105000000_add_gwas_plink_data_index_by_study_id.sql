DO $$
BEGIN
	IF NOT EXISTS (
			SELECT 1
			FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE  c.relname = 'idx_plink_data_study_id' AND n.nspname = 'gwas_plink'
	) THEN
		CREATE UNIQUE INDEX idx_plink_data_study_id ON gwas_plink.plink_data(study_id);
	END IF;
END
$$;