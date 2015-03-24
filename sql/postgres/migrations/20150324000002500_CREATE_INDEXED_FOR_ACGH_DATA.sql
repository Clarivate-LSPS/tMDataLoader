DO $$
    BEGIN
        BEGIN
            CREATE INDEX IDX_DE_CHROMOSAL_REGION ON DEAPP.de_chromosomal_region (GPL_ID, GENE_SYMBOL);
        EXCEPTION
            WHEN duplicate_table THEN RAISE NOTICE 'Index IDX_DE_CHROMOSAL_REGION already exists.';
        END;
    END;
$$;