DO $$
BEGIN

alter table deapp.de_variant_subject_summary drop constraint if exists variant_subject_sumds_uk;
drop index if exists deapp.variant_subject_sumds_uk;

END;
$$;
