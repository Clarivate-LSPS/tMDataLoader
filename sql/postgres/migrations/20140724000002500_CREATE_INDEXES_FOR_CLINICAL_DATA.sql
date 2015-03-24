DO $$
    BEGIN
        BEGIN
            ALTER TABLE I2B2Metadata.i2b2 ADD COLUMN RECORD_ID SERIAL;
        EXCEPTION
            WHEN duplicate_column THEN RAISE NOTICE 'Column RECORD_ID already exists in I2B2Metadata.i2b2.';
        END;
    END;
$$;

DO $$
    BEGIN
        BEGIN
            CREATE INDEX I2B2META_IDX_RECORD_ID ON I2B2Metadata.i2b2 (RECORD_ID);
        EXCEPTION
            WHEN duplicate_table THEN RAISE NOTICE 'Index I2B2META_IDX_RECORD_ID already exists.';
        END;
    END;
$$;

DO $$
    BEGIN
        BEGIN
            CREATE INDEX idx_concept_path ON i2b2demodata.concept_counts(concept_path);
        EXCEPTION
            WHEN duplicate_table THEN RAISE NOTICE 'Index idx_concept_path already exists.';
        END;
    END;
$$;

DO $$
    BEGIN
        BEGIN
            CREATE INDEX idx_pd_sourcesystemcd_pnum ON i2b2demodata.patient_dimension(sourcesystem_cd, patient_num);
        EXCEPTION
            WHEN duplicate_table THEN RAISE NOTICE 'Index idx_pd_sourcesystemcd_pnum already exists.';
        END;
    END;
$$;

DO $$
    BEGIN
        BEGIN
            CREATE INDEX IX_I2B2_SOURCE_SYSTEM_CD ON i2b2metadata.i2b2(sourcesystem_cd);
        EXCEPTION
            WHEN duplicate_table THEN RAISE NOTICE 'Index IX_I2B2_SOURCE_SYSTEM_CD already exists.';
        END;
    END;
$$;

DO $$
    BEGIN
        BEGIN
            CREATE INDEX IDX_I2B2_FULLNAME_BASECODE ON i2b2metadata.i2b2(c_fullname, c_basecode);
        EXCEPTION
            WHEN duplicate_table THEN RAISE NOTICE 'Index IDX_I2B2_FULLNAME_BASECODE already exists.';
        END;
    END;
$$;

DO $$
    BEGIN
        BEGIN
            CREATE INDEX IDX_I2B2_BASECODE ON i2b2metadata.i2b2(c_basecode, record_id, c_visualattributes);
        EXCEPTION
            WHEN duplicate_table THEN RAISE NOTICE 'Index IDX_I2B2_BASECODE already exists.';
        END;
    END;
$$;

DO $$
    BEGIN
        BEGIN
            CREATE INDEX IDX_DE_SUBJ_SMPL_TRIAL_CCODE ON DEAPP.DE_SUBJECT_SAMPLE_MAPPING(TRIAL_NAME, CONCEPT_CODE);
        EXCEPTION
            WHEN duplicate_table THEN RAISE NOTICE 'Index IDX_DE_SUBJ_SMPL_TRIAL_CCODE already exists.';
        END;
    END;
$$;

DO $$
    BEGIN
        BEGIN
            CREATE INDEX IDX_I2B2_SECURE_FULLNAME ON I2B2METADATA.I2B2_SECURE(C_FULLNAME);
        EXCEPTION
            WHEN duplicate_table THEN RAISE NOTICE 'Index IDX_I2B2_SECURE_FULLNAME already exists.';
        END;
    END;
$$;

DO $$
    BEGIN
        BEGIN
            CREATE INDEX IDX_FACT_PATIENT_NUM ON I2B2DEMODATA.OBSERVATION_FACT(PATIENT_NUM);            
        EXCEPTION
            WHEN duplicate_table THEN RAISE NOTICE 'Index IDX_FACT_PATIENT_NUM already exists.';
        END;
    END;
$$;

DO $$
    BEGIN
        BEGIN
            CREATE INDEX IDX_DE_CHROMOSAL_REGION ON DEAPP.de_chromosomal_region (GPL_ID, GENE_SYMBOL);
        EXCEPTION
            WHEN duplicate_table THEN RAISE NOTICE 'Index IDX_DE_CHROMOSAL_REGION already exists.';
        END;
    END;
$$;
