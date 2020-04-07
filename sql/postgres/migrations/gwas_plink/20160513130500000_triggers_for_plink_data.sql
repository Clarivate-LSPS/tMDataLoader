DO $$
BEGIN
    CREATE OR REPLACE FUNCTION gwas_plink._cleanup_plink_data()
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
    
    DROP TRIGGER IF EXISTS trg_plink_data_delete_lobs ON gwas_plink.plink_data;
    CREATE TRIGGER trg_plink_data_delete_lobs
    AFTER DELETE OR UPDATE ON gwas_plink.plink_data
    FOR EACH ROW
    EXECUTE PROCEDURE gwas_plink._cleanup_plink_data();
    
    CREATE OR REPLACE FUNCTION gwas_plink._plink_data_add_lob_permissions()
    	RETURNS TRIGGER AS $add_lob_permissions$
    BEGIN
    	EXECUTE 'GRANT SELECT ON LARGE OBJECT ' || NEW.bed || ',' || NEW.bim || ',' || NEW.fam || ' TO biomart_user';
    	RETURN NULL;
    END;
    $add_lob_permissions$ LANGUAGE plpgsql;
    
    DROP TRIGGER IF EXISTS trg_plink_data_add_lob_permissions ON gwas_plink.plink_data;
    CREATE TRIGGER trg_plink_data_add_lob_permissions
    AFTER INSERT OR UPDATE ON gwas_plink.plink_data
    FOR EACH ROW
    EXECUTE PROCEDURE gwas_plink._plink_data_add_lob_permissions();
END
$$;
