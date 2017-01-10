create schema if not exists tm_dataloader;

DO
$body$
BEGIN
  IF NOT EXISTS (
      SELECT *
      FROM   pg_catalog.pg_user
      WHERE  usename = 'tm_dataloader') THEN

    CREATE ROLE tm_dataloader LOGIN UNENCRYPTED PASSWORD 'tm_dataloader';

    RAISE WARNING 'tm_dataloader was created with default password. Please, change it to more secure';
  END IF;
END
$body$;

alter schema tm_dataloader owner to tm_dataloader;

alter user tm_dataloader set search_path= "$user", tm_cz, tm_lz, tm_wz, i2b2demodata, i2b2metadata, deapp, public;

grant amapp, biomart, biomart_stage, biomart_user, deapp, fmapp, galaxy, i2b2demodata, i2b2metadata, searchapp, tm_cz, tm_lz, tm_wz to tm_dataloader;