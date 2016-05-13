DO $$
DECLARE
	pd      gwas_plink.plink_data%ROWTYPE;
	bed_oid OID;
	bim_oid OID;
	fam_oid OID;
BEGIN
	IF NOT EXISTS(SELECT *
								FROM information_schema.columns
								WHERE table_schema = 'gwas_plink' AND table_name = 'plink_data' AND column_name = 'bed' AND
											data_type = 'bytea')
	THEN
		RETURN;
	END IF;

	ALTER TABLE gwas_plink.plink_data
		ADD bed_new OID,
		ADD bim_new OID,
		ADD fam_new OID;

	FOR pd IN (SELECT *
						 FROM gwas_plink.plink_data
						 FOR UPDATE)
	LOOP
		SELECT oid
		INTO bed_oid
		FROM (SELECT
						oid,
						lowrite(lo_open(oid, x'20000' :: INT), pd.bed)
					FROM lo_create(0) o(oid)) x;

		SELECT oid
		INTO bim_oid
		FROM (SELECT
						oid,
						lowrite(lo_open(oid, x'20000' :: INT), pd.bim)
					FROM lo_create(0) o(oid)) x;

		SELECT oid
		INTO fam_oid
		FROM (SELECT
						oid,
						lowrite(lo_open(oid, x'20000' :: INT), pd.fam)
					FROM lo_create(0) o(oid)) x;

		UPDATE gwas_plink.plink_data
		SET bed_new = bed_oid, bim_new = bim_oid, fam_new = fam_oid
		WHERE plink_data_id = pd.plink_data_id;
	END LOOP;

	ALTER TABLE gwas_plink.plink_data
		DROP bed,
		DROP bim,
		DROP fam;

	ALTER TABLE gwas_plink.plink_data
		RENAME bed_new TO bed;

	ALTER TABLE gwas_plink.plink_data
		RENAME bim_new TO bim;

	ALTER TABLE gwas_plink.plink_data
		RENAME fam_new TO fam;

	CREATE FUNCTION gwas_plink._cleanup_plink_data()
		RETURNS TRIGGER AS $cleanup$
	BEGIN
		IF (TG_OP = 'DELETE')
		THEN
			PERFORM lo_unlink(OLD.bed),
				lo_unlink(OLD.bim),
				lo_unlink(OLD.fam);
		ELSIF TG_OP = 'UPDATE'
			THEN
				IF OLD.bed <> NEW.bed
				THEN
					PERFORM lo_unlink(OLD.bed);
				END IF;
				IF OLD.bim <> NEW.bim
				THEN
					PERFORM lo_unlink(OLD.bim);
				END IF;
				IF OLD.fam <> NEW.fam
				THEN
					PERFORM lo_unlink(OLD.fam);
				END IF;
		END IF;
		RETURN NULL;
	END;
	$cleanup$ LANGUAGE plpgsql;

	CREATE TRIGGER trg_plink_data_delete_lobs
	AFTER DELETE OR UPDATE ON gwas_plink.plink_data
	FOR EACH ROW
	EXECUTE PROCEDURE gwas_plink._cleanup_plink_data();

	CREATE FUNCTION gwas_plink._plink_data_add_lob_permissions()
		RETURNS TRIGGER AS $add_lob_permissions$
	BEGIN
		EXECUTE 'GRANT SELECT ON LARGE OBJECT ' || NEW.bed || ',' || NEW.bim || ',' || NEW.fam || ' TO biomart_user';
		RETURN NULL;
	END;
	$add_lob_permissions$ LANGUAGE plpgsql;

	CREATE TRIGGER trg_plink_data_add_lob_permissions
	AFTER INSERT OR UPDATE ON gwas_plink.plink_data
	FOR EACH ROW
	EXECUTE PROCEDURE gwas_plink._plink_data_add_lob_permissions();
END
$$;
