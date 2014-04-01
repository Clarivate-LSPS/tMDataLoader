DO $$
BEGIN

IF NOT EXISTS (
    SELECT 1
    FROM   pg_class c
    JOIN   pg_namespace n ON n.oid = c.relnamespace
    WHERE  c.relname = 'de_snp_calls_by_gsm_patient_num_idx'
    AND    n.nspname = 'deapp'
    ) THEN

    create index de_snp_calls_by_gsm_patient_num_idx on deapp.de_snp_calls_by_gsm(patient_num);
END IF;

IF NOT EXISTS (
    SELECT 1
    FROM   pg_class c
    JOIN   pg_namespace n ON n.oid = c.relnamespace
    WHERE  c.relname = 'de_snp_copy_number_num_idx'
    AND    n.nspname = 'deapp'
    ) THEN

    create index de_snp_copy_number_num_idx on deapp.de_snp_copy_number(patient_num);
END IF;

END$$;
