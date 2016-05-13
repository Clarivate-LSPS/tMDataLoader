DO $$
BEGIN
	IF NOT EXISTS(
			SELECT a.attname
			FROM pg_index i
				JOIN pg_attribute a ON a.attrelid = i.indrelid
															 AND a.attnum = ANY (i.indkey)
			WHERE i.indrelid = 'gwas_plink.plink_data' :: REGCLASS
						AND i.indisprimary)
	THEN
		ALTER TABLE gwas_plink.plink_data
			ADD PRIMARY KEY (plink_data_id);
	END IF;
END
$$;