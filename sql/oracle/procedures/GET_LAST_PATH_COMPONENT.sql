CREATE OR REPLACE FUNCTION "GET_LAST_PATH_COMPONENT"(
  node_path VARCHAR2
)
  RETURN VARCHAR2 IS
  previousBracket INT;
  lastComp        VARCHAR2(4000);
  BEGIN
    previousBracket := INSTR(node_path, '\', -1, 2);
    IF (previousBracket = 0)
    THEN
      previousBracket := INSTR(node_path, '+', -1, 1);
      lastComp := substr(node_path, previousBracket + 1, length(node_path) - previousBracket);
    ELSE
      lastComp := substr(node_path, previousBracket + 1, length(node_path) - previousBracket - 1);
    END IF;


    RETURN lastComp;
  END;
  /