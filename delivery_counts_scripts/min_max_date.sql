SELECT 'SELECT cast(MIN(min_value) as date) AS min_date,
        cast(MAX(max_value) as date) AS max_date FROM (' as sql_code
UNION ALL
SELECT DISTINCT
    'union SELECT MIN(' || column_name || ') AS min_value, MAX(' || column_name || ') AS max_value FROM '
    || table_schema || '.' || table_name || ' WHERE ' || column_name || ' IS NOT NULL'
    || CASE
         WHEN column_name = 'date_of_birth' THEN ' AND ' || column_name || ' not ilike ''1901-01-01%'''
         ELSE ''
       END
     AS sql_code
FROM information_schema.columns
WHERE table_schema IN (#schema_list)
  AND data_type ILIKE '%timestamp%'
  AND  table_name in ('adverse_event','disease_status','biomarker','care_goal','condition','drug','encounter',
'imaging','medication','patient','patient_test','radiation','staging','surgery','tumor_exam','clinical_features','outcomes',
'table1_patient','table2_treatment','table3_labs')
UNION ALL
SELECT ');' as sql_code;
