SELECT DISTINCT
    table_schema,
    table_name,
     (
        'UNION SELECT ' || 
        #iteration || ' AS Iteration, ' ||
        '''#customer''' ||' AS Customer, ' ||
        'CASE ' ||
            'WHEN ''' || table_schema || ''' ILIKE ''%pt360_analytical_layer_%_melanoma_bms_registry%'' THEN ''Patient360-Fast Registry-Analytical Layer'' ' ||
            'WHEN ''' || table_schema || ''' ILIKE ''%pt360_%_melanoma_bms_registry%'' THEN ''Patient360-Fast Registry'' ' ||
            'WHEN ''' || table_schema || ''' ILIKE ''%nov06029%_breast%'' THEN ''Custom Registry'' ' ||
            'WHEN ''' || table_schema || ''' ILIKE ''%pluvicto%'' THEN ''Custom Registry'' ' ||
            'WHEN ''' || table_schema || ''' ILIKE ''%pt360%Analytical%Layer%'' THEN ''Patient360-Analytical Layer'' ' ||
            'WHEN ''' || table_schema || ''' ILIKE ''%rwd360%Analytical%Layer%'' THEN ''RWD360-Analytical Layer'' ' ||
            'WHEN ''' || table_schema || ''' ILIKE ''%Analytical%Layer%'' AND EXISTS (SELECT 1 FROM ' || table_schema || '.' || table_name || ' WHERE LEFT(chai_patient_id, 2) ILIKE ''ch'' LIMIT 1) THEN ''RWD360-Analytical Layer'' ' ||
            'WHEN ''' || table_schema || ''' ILIKE ''%Analytical%Layer%'' AND EXISTS (SELECT 1 FROM ' || table_schema || '.' || table_name || ' WHERE LEFT(chai_patient_id, 2) ILIKE ''pt'' LIMIT 1) THEN ''Patient360-Analytical Layer'' ' ||
            'WHEN ''' || table_schema || ''' NOT ILIKE ''%Analytical%Layer%'' AND EXISTS (SELECT 1 FROM ' || table_schema || '.' || table_name || ' WHERE LEFT(chai_patient_id, 2) ILIKE ''ch'' LIMIT 1) THEN ''RWD360-Analytical Layer'' ' ||
            'WHEN ''' || table_schema || ''' NOT ILIKE ''%Analytical%Layer%'' AND EXISTS (SELECT 1 FROM ' || table_schema || '.' || table_name || ' WHERE LEFT(chai_patient_id, 2) ILIKE ''pt'' LIMIT 1) THEN ''Patient360-Analytical Layer'' ' ||
            'ELSE ''' || table_schema || ''' ' ||
        'END AS delivery_type, ' ||

        'CASE ' ||
            'WHEN ''' || table_schema || ''' ILIKE ''%_pt360%'' THEN ''Patient360'' ' ||
            'WHEN ''' || table_schema || ''' ILIKE ''%_rwd360_%'' THEN ''RWD360'' ' ||
            'WHEN ''' || table_schema || ''' ILIKE ''%_gn360_%'' THEN ''Genome360'' ' ||
            'WHEN EXISTS (SELECT 1 FROM ' || table_schema || '.' || table_name || ' WHERE LEFT(chai_patient_id, 2) ILIKE ''ch'' LIMIT 1) THEN ''RWD360'' ' ||
            'WHEN EXISTS (SELECT 1 FROM ' || table_schema || '.' || table_name || ' WHERE LEFT(chai_patient_id, 2) ILIKE ''pt'' LIMIT 1) THEN ''Patient360'' ' ||
            'ELSE ''' || table_schema || ''' ' ||
        'END AS Product, ' ||

        'CASE ' ||
            'WHEN ''' || table_schema || ''' ILIKE ''%Analytical%Layer%'' THEN ''Analytical Layer'' ' ||
            'WHEN ''' || table_schema || ''' ILIKE ''%nov06029%_breast%'' THEN ''Custom Registry'' ' ||
            'WHEN ''' || table_schema || ''' ILIKE ''%pluvicto%'' THEN ''Custom Registry'' ' ||
            'WHEN ''' || table_schema || ''' ILIKE ''%pt360_analytical_layer_%_melanoma_bms%'' THEN ''Analytical Layer'' ' ||
            'WHEN ''' || table_schema || ''' ILIKE ''%pt360_open_%'' THEN ''Open Claims - Common Data Model (CDM)'' ' ||
            'ELSE ''Electronic Medical Records (EMR) - Common Data Model (CDM)'' ' ||
        'END AS Format, ' ||

        'CASE ' ||
            'WHEN ''' || table_schema || ''' ILIKE ''%Analytical%Layer%'' THEN ''Core Data Product (Base)'' ' ||
            'WHEN ''' || table_schema || ''' ILIKE ''%nov06029%_breast%'' THEN ''Core Data Product (Base)'' ' ||
            'WHEN ''' || table_schema || ''' ILIKE ''%pluvicto_nov06521%'' THEN ''Core Data Product (Base)'' ' ||
            'ELSE ''' || table_name || ''' ' ||
        'END AS Addon, ' ||

        'CASE ' ||
            'WHEN ''' || table_schema || ''' ILIKE ''%_pluvicto%'' OR ''' || table_schema || ''' ILIKE ''%_prostate%'' THEN ''Prostate Cancer'' ' ||
            'WHEN ''' || table_schema || ''' ILIKE ''%c3_rwd360_analytical_layer_%_amgen%'' OR ''' || table_name || ''' ILIKE ''%_lung%'' THEN ''Lung Cancers'' ' ||
            'WHEN ''' || table_schema || ''' ILIKE ''%_breast%'' OR ''' || table_name || ''' ILIKE ''%_breast%'' THEN ''Breast Cancer'' ' ||
            'WHEN ''' || table_schema || ''' ILIKE ''%_bladder%'' OR ''' || table_name || ''' ILIKE ''%bladder%'' THEN ''Bladder Cancer'' ' ||
            'WHEN ''' || table_schema || ''' ILIKE ''%_crc%'' OR ''' || table_name || ''' ILIKE ''%_crc%'' THEN ''Colorectal cancer (CRC)'' ' ||
            'WHEN ''' || table_schema || ''' ILIKE ''%_gec%'' OR ''' || table_name || ''' ILIKE ''%_gec%'' THEN ''Gastroesophageal Cancer (GEC)'' ' ||
            'WHEN ''' || table_schema || ''' ILIKE ''%_hcc%'' OR ''' || table_name || ''' ILIKE ''%_hcc%'' THEN ''Hepatocellular Carcinoma (HCC)'' ' ||
            'WHEN ''' || table_schema || ''' ILIKE ''%_melanoma%'' OR ''' || table_name || ''' ILIKE ''%_melanoma%'' THEN ''Melanoma'' ' ||
            'WHEN ''' || table_schema || ''' ILIKE ''%_nsclc%'' OR ''' || table_name || ''' ILIKE ''%_nsclc%'' THEN ''Non-Small Cell Lung Cancer (NSCLC)'' ' ||
            'WHEN ''' || table_schema || ''' ILIKE ''%_ovarian%'' OR ''' || table_name || ''' ILIKE ''%_ovarian%'' THEN ''Ovarian cancer'' ' ||
            'WHEN ''' || table_schema || ''' ILIKE ''%_prostate%'' OR ''' || table_name || ''' ILIKE ''%_prostate%'' THEN ''Prostate Cancer'' ' ||
            'WHEN ''' || table_schema || ''' ILIKE ''%_sclc%'' OR ''' || table_name || ''' ILIKE ''%_sclc%'' THEN ''Small cell lung cancer (SCLC)'' ' ||
            'ELSE ''' || table_name || ''' ' ||
        'END AS indication, ' ||

        '''Standard Subscription'' AS Subscription_Category, ' ||
        '''Standard'' AS Subscription_Subcategory, ' ||
        'COUNT(DISTINCT chai_patient_id) AS patient_count ' ||
        'FROM ' || table_schema || '.' || table_name
    ) AS sql_code
FROM information_schema.columns 
WHERE table_schema IN (#schema_list)
AND table_name IN ('patient', 'table1_patient', 'outcomes','pluvicto_discontinuation');
