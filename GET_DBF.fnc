CREATE OR REPLACE FUNCTION GET_DBF(p_sql varchar2,
                 nls_codepage          varchar2 default 'RU8PC866'
                ) return blob
is
   -- author = sparshukov
   -- goal   = получение в BLOB результата запроса в формате DBF
   l_header       varchar2(5000) := ''; -- для 1000 полей
   l_rowCounter   number   := 0;
   l_colCnt       number   := 0;   -- кол-во колонок
   l_line_length  number   := 0;
   l_colValue_str varchar2(500);   -- значение столбца
   l_colValue_dat date;            -- значение столбца
   l_colValue_num number;          -- значение столбца
   
   l_theCursor    number   := dbms_sql.open_cursor;
   l_status       number   :=0;            -- результат выполнения запроса
   l_descTbl      dbms_sql.desc_tab;       -- таблица описаний
   l_delimiter    varchar2(10)   := '';
   reportClob     blob;

   buff_size      number := 32696;  
   l_buff         varchar2(32696):='';  -- строка результата
   l_line         varchar2(32696):='';  -- строка результата

   local_sql      varchar2(32696):='';  -- для реального размера VARCHAR полей
   loc_Cur        number   := dbms_sql.open_cursor;
   loc_descTblLen dbms_sql.desc_tab;    -- таблица описаний
   col_max_len_calc number;
   fetch_val      number;

------------------------------------------------------------------------   
function to_ascii(p_number in number) return varchar2 
is
    l_number number := p_number;
    l_data   varchar2(8);
    l_bytes  number;
    l_byte   number;
begin
    select vsize(l_number) into l_bytes from dual;
    for i in 1 .. l_bytes loop
      l_byte := trunc(mod(l_number, power(2, 8 * i)) /
                      power(2, 8 * (i - 1)));
      l_data := l_data || chr(l_byte);
    end loop;
    return l_data;
end;
  
   
--==============================================================================
begin
--  log_ovart(0,'part','unit='||$$PLSQL_UNIT);
  -- анализируем запрос + получаем описание результатов запроса
  dbms_sql.parse(l_theCursor, p_sql, dbms_sql.native);
  dbms_sql.describe_columns(l_theCursor, l_colCnt, l_descTbl);
  
  -- получаем реальные размеры колонок
  local_sql     := 'select ';
  for i in 1..l_colCnt
  loop
    if l_descTbl(i).col_type = 12 then 
      local_sql := local_sql || chr(13)||chr(10)||'max(length(nvl("'||l_descTbl(i).col_name||'",to_date(''01.01.1900'',''dd.mm.yyyy'')))) "'||l_descTbl(i).col_name||'",'; 
    else
      local_sql := local_sql || chr(13)||chr(10)||'max(length(nvl("'||l_descTbl(i).col_name||'",0))) "'||l_descTbl(i).col_name||'",'; 
    end if;
  end loop;
  local_sql := substr(local_sql,1, length(local_sql)-1)||'from ('||p_sql||')';
  dbms_sql.parse(loc_Cur, local_sql, dbms_sql.native);
  dbms_sql.describe_columns(loc_Cur, l_colCnt, loc_descTblLen);
  for i in 1..l_colCnt
  loop
    dbms_sql.define_column(loc_Cur, i, col_max_len_calc);
  end loop;
  l_status := dbms_sql.execute(loc_Cur);
  fetch_val:= dbms_sql.fetch_rows(loc_Cur);

  --- 1 шаг - фомируем заголовок -----------------------------------------------
    -- №0 Версия/ 1 байт 03 - простая таблица
    l_header := chr(3);
    -- №1,2,3 Дата последнего обновления таблицы в формате YYMMDD/ 3 байта
    l_header := l_header || chr(to_number(to_char(sysdate, 'YY'))) || chr(to_number(to_char(sysdate, 'MM'))) || chr(to_number(to_char(sysdate, 'DD')));
    --№4,5,6,7 Количество записей в таблице/ 32 бита = 4 байта
    l_header := l_header || rpad(to_ascii(0), 4, chr(0)); --------------- !!!! обновить после считывания данных реальным количеством строк
    --№8,9 Количество байтов, занимаемых заголовком
    --/16 бит = 2 байта = 32 + 32*n + 1, где n - количество столбцов, а 1 - ограничительный байт 
    l_header := l_header || rpad(to_ascii(32 + l_colCnt * 32 + 1), 2, chr(0)); 
    --№10,11 Количество байтов, занимаемых записью/16 бит = 2 байта 
    l_header := l_header || rpad(to_ascii(0), 2, chr(0)); --------------- !!!! обновить после считывания данных реальной длиной строки
    --№12,13 Зарезервировано
    l_header := l_header || rpad(chr(0), 2, chr(0));
    --№14 Транзакция, 1-начало, 0-конец(завершена)
    l_header := l_header || chr(0);
    --№15 Кодировка: 1-закодировано, 0-нормальная видимость 
    l_header := l_header || chr(0);
    --№16-27 Использование многопользовательского окружения
    l_header := l_header || rpad(chr(0), 12, chr(0));
    --№28 Использование индекса 0-не использовать
    l_header := l_header || chr(0);
    --№29 Номер драйвера языка
    l_header := l_header || chr(38); -- http://www.autopark.ru/ASBProgrammerGuide/DBFSTRUC.HTM#Table_9
    --№30,31 Зарезервировано
    l_header := l_header || rpad(chr(0), 2, chr(0));
  
    --ОПИСАНИЯ ПОЛЕЙ В ЗАГОЛОВКЕ
    for i in 1..l_colCnt
    loop
      dbms_sql.column_value(loc_Cur, i, col_max_len_calc);
      --№0-10 Имя поля с 0-завершением/11 байт
      l_header := l_header || rpad(substr(replace(l_descTbl(i).col_name,'.',''), 1, 10), 11, chr(0));
      --№11 Тип поля/1 байт
      l_header := l_header || case when l_descTbl(i).col_type=2  then 'N' 
                                   when l_descTbl(i).col_type=12 then 'D' else 'C' end; 
      --№12,13,14,15 Игнорируется/4 байта
      l_header := l_header || rpad(chr(0), 4, chr(0));
      --№16 Размер поля/1 байт
      l_header := l_header || chr(case when l_descTbl(i).col_type=2  then 20 --18
                                       when l_descTbl(i).col_type=12 then 8 else col_max_len_calc end );
      --№17 Количество знаков после запятой/1 байт
      l_header := l_header || chr(case when l_descTbl(i).col_type=2  then 5 else 0 end);
      --№18,19 Зарезервированная область/2 байта
      l_header := l_header || rpad(chr(0), 2, chr(0));
      --№20 Идентификатор рабочей области/1 байт
      l_header := l_header || chr(0);
      --№21,22 Многопользовательский dBase/2 байта
      l_header := l_header || rpad(chr(0), 2, chr(0));
      --№23 Установленные поля/1 байт
      l_header := l_header || chr(0); --psv chr(1);
      --№24 Зарезервировано/7 байт
      l_header := l_header || rpad(chr(0), 7, chr(0));
      --№31 Флаг MDX-поля: 01H если поле имеет метку индекса в MDX-файле, 00H - нет.
      l_header := l_header || chr(0);

      if l_descTbl(i).col_type = 2  then 
        dbms_sql.define_column(l_theCursor, i, l_colValue_num);
      elsif l_descTbl(i).col_type = 12  then 
        dbms_sql.define_column(l_theCursor, i, l_colValue_dat);
      else
        dbms_sql.define_column(l_theCursor, i, l_colValue_str, 500);
      end if;
    end loop;
    --Завершающий заголовок символ 0D
    l_header := l_header || chr(13);
    dbms_lob.createtemporary(reportClob,false);
    dbms_lob.append(reportClob, utl_raw.cast_to_raw(l_header));  
  
    --- 2 шаг - заполняем файл данными -----------------------------------------   
    l_rowCounter := 0;
    -- выполняем запрос
    l_status := dbms_sql.execute(l_theCursor);
  
    -- извлекаем результаты
    l_buff := ''; 
    while (dbms_sql.fetch_rows(l_theCursor) > 0 )
    loop
        l_line := chr(32); -- Символ CHR (32) обозначает, что записи не удалены
        for i in 1..l_colCnt
        loop
          dbms_sql.column_value(loc_Cur, i, col_max_len_calc);
          if    l_descTbl(i).col_type = 2  then 
            dbms_sql.column_value(l_theCursor, i, l_colValue_num);
            l_line := l_line || case when l_colValue_num is null then '                    ' 
                                                                  else replace(to_char(nvl(l_colValue_num,0),'9999999999990D99999'),',','.') end;
          elsif l_descTbl(i).col_type = 12 then 
            dbms_sql.column_value(l_theCursor, i, l_colValue_dat);
            l_line := l_line || case when l_colValue_dat is null then '        ' 
                                                                 else to_char(l_colValue_dat,'YYYYMMDD') end;
          else 
            dbms_sql.column_value(l_theCursor, i, l_colValue_str); 
            l_line := l_line || case when l_colValue_str is null then rpad(' ',col_max_len_calc,' ') 
                                                                 else rpad(CONVERT(l_colValue_str,'RU8PC866'),col_max_len_calc,' ') end;
          end if;
        end loop;
        l_rowCounter := l_rowCounter +1;
        -- строку пишем в буфер, который предварительно может быть сброшен в результат, если строка в него не влазит
        if (length(l_buff)+ length(l_line)) > buff_size then
          dbms_lob.append(reportClob, utl_raw.cast_to_raw(l_buff));  
          l_buff := l_line;
        else    
          l_buff := l_buff || l_line;
        end if;
    end loop;
    -- дописываем буфер в результат
    if length(l_buff)>0 then
       dbms_lob.append(reportClob, utl_raw.cast_to_raw(l_buff));  
       l_buff := '';
    end if;
    dbms_sql.close_cursor(l_theCursor);
    dbms_sql.close_cursor(loc_Cur);
    --Завершающий символ 
    dbms_lob.append(reportClob, utl_raw.cast_to_raw(chr(26)));  

    --- 3 шаг - корректируем заголовок - пишем точное кол-во байт, которое извлекли и длину строки -------------------   

    --№4,5,6,7 Количество записей в таблице/ 32 бита = 4 байта
--    l_header := l_header || rpad(to_ascii(0), 4, chr(0)); --------------- !!!! обновить после считывания данных реальным количеством строк
    dbms_lob.write(reportClob, 4, 5, utl_raw.cast_to_raw(  rpad(to_ascii(l_rowCounter), 4, chr(0))  ));

    --№10,11 Количество байтов, занимаемых записью/16 бит = 2 байта 
--    l_header := l_header || rpad(to_ascii(0), 2, chr(0)); --------------- !!!! обновить после считывания данных реальной длиной строки
    dbms_lob.write(reportClob, 2,11, utl_raw.cast_to_raw(lpad(to_ascii(length(l_line)), 2, chr(0)))  );

  return reportClob;

end;
/
