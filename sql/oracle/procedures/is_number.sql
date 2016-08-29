--
-- Type: FUNCTION; Owner: TM_DATALOADER; Name: IS_NUMBER
--
  CREATE OR REPLACE FUNCTION "IS_NUMBER"
 ( p_string in varchar2)
 return number
    as
        l_number number;
        t_string VARCHAR2(250);
        session_decimal_separator VARCHAR2(1);
    begin
        select substr(value,1,1) session_decimal_separator into session_decimal_separator
        from  nls_session_parameters
        where parameter = 'NLS_NUMERIC_CHARACTERS';

        t_string := regexp_replace(p_string, '\.|,',session_decimal_separator);
        l_number := t_string;
        return 0;
   exception
       when others then
           return 1;
   end;




/
 
