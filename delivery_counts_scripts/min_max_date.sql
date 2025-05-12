SELECT 'SELECT cast(MIN(min_value) as date) AS min_date,
        cast(MAX(max_value) as date) AS max_date FROM (' as sql_code
UNION ALL
SELECT DISTINCT
    'union SELECT MIN(' || column_name || ') AS min_value, MAX(' || column_name || ') AS max_value FROM '
    || table_schema || '.' || table_name || ' WHERE ' || column_name || ' IS NOT NULL'
    || CASE
         WHEN column_name = 'date_of_birth' THEN ' AND ' || column_name || ' <> ''1901-01-01 00:00:00.000'''
         ELSE ''
       END
     AS sql_code
FROM information_schema.columns
WHERE table_schema IN (#schema_list)
  AND data_type ILIKE '%timestamp%'
UNION ALL
SELECT ');' as sql_code;
