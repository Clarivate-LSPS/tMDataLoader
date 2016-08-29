create or replace FUNCTION "GET_LASH_PATH_COMPONENT"(
  node_path VARCHAR2)
  RETURN VARCHAR2 IS
  previousBracket INT;
  lastComp        VARCHAR2(4000);
  BEGIN
    previousBracket := INSTR(node_path, '\', -1, 2);
    lastComp := substr(node_path, previousBracket + 1, length(node_path) - previousBracket - 1);

    RETURN lastComp;
  END;
  /