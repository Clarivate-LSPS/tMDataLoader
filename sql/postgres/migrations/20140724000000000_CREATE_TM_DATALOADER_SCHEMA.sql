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
$body$