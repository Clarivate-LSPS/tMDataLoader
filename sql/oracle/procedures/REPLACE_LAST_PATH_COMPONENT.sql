create or replace FUNCTION "REPLACE_LAST_PATH_COMPONENT"(
  node_path VARCHAR2,
  new_value VARCHAR2)
  RETURN VARCHAR2 IS
  lastComp        VARCHAR2(4000);
  modString       VARCHAR2(4000);
  BEGIN
    lastComp := GET_LAST_PATH_COMPONENT(node_path);

    modString := regexp_replace(node_path, lastComp || '\\$', new_value || '\\');
    RETURN modString;
  END;
  /