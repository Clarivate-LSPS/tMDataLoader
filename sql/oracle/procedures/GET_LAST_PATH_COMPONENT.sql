CREATE OR REPLACE FUNCTION "GET_LAST_PATH_COMPONENT"(
  node_path VARCHAR2
)
RETURN VARCHAR2 IS
  previousBackSlash INT;
  lastComp        VARCHAR2(4000);
  BEGIN
    previousBackSlash := INSTR(node_path, '\', -1, 2);
    lastComp := substr(node_path, previousBackSlash + 1, length(node_path) - previousBackSlash - 1);
    RETURN lastComp;
  END;
  /