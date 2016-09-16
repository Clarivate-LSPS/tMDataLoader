create or replace FUNCTION "REPLACE_LAST_PATH_COMPONENT"(
  node_path VARCHAR2,
  new_value VARCHAR2)
  RETURN VARCHAR2 IS
    previousBackSlash INT;
  BEGIN
		previousBackSlash := INSTR(node_path, '\', -1, 2);

    RETURN SUBSTR(node_path, 1, previousBackSlash) || new_value || '\';
  END;
  /