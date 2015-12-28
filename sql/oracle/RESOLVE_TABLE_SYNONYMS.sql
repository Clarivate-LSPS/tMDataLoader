CREATE OR REPLACE FUNCTION resolve_table_synonyms (p_name VARCHAR2, p_owner VARCHAR2 := null, p_maxdepth NUMBER := 8)
RETURN VARCHAR2 AS
    l_count NUMBER;
    l_name VARCHAR2(32);
    l_owner VARCHAR2(32);
BEGIN
    IF p_maxdepth < 0 THEN
        raise_application_error(-20000, 'Recursion too deep');
    END IF;
    IF p_name IS NULL THEN
        RAISE value_error;
    END IF;
    l_name := upper(p_name);
    IF p_owner IS NULL THEN
        l_owner := sys_context('userenv', 'current_schema');
    ELSE
        l_owner := upper(p_owner);
    END IF;
    SELECT count(*) INTO l_count FROM all_tables WHERE owner=l_owner AND table_name=l_name;
    IF l_count > 0 THEN
        RETURN l_owner||'.'||l_name;
    END IF;
    BEGIN
        SELECT table_owner, table_name INTO l_owner, l_name FROM all_synonyms WHERE owner=l_owner AND synonym_name=l_name;
    EXCEPTION
        -- Explicitly raising exceptions because some may be silently ignored and function returns null.
        WHEN no_data_found THEN raise_application_error(-20001, 'Not a table');
        WHEN OTHERS THEN raise_application_error(-20002, 'Select error');
    END;
    RETURN resolve_table_synonyms(l_name, l_owner, p_maxdepth - 1);
END;
/
