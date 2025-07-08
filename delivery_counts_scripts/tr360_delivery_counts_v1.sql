SELECT DISTINCT 
    table_schema,
    table_name,
    (
        'UNION SELECT ' || 
        #iteration || ' AS Iteration, ' ||
        '''#customer''' ||' AS Customer, ' ||
        CASE 
            WHEN table_schema ILIKE '%_tr360_%' AND (table_name ILIKE '%_claims%' OR table_schema ILIKE '%_open_%') THEN '''Translational360-Claims'''
            WHEN table_schema ILIKE '%_tr360_%' AND (table_name NOT ILIKE '%_claims%' AND table_schema NOT ILIKE '%_open_%') THEN '''Translational360-Claims'''
            ELSE '''' || table_schema || ''''
        END || ' AS delivery_type, ' ||
        CASE 
            WHEN table_schema ILIKE '%_tr360%Analytical%Layer%' THEN '''Translational360-Analytical Layer'''
            WHEN table_schema ILIKE '%_tr360_%' THEN '''Translational360'''
            ELSE '''' || table_schema || ''''
        END || ' AS Product, ' ||
        CASE 
            WHEN table_name ILIKE '%_claims%' OR table_schema ILIKE '%_open_%' THEN '''Open Claims - Common Data Model (CDM)'''
            WHEN table_schema ILIKE '%_tr360%Analytical%Layer%' THEN '''Analytical Layer'''
            ELSE '''Electronic Medical Records (EMR) - Common Data Model (CDM)'''
        END || ' AS Format, ' ||
        CASE 
            WHEN table_name ILIKE 'adj_neo_tx_%_std' THEN '''Model Outputs: Adjuvant & Neo-Adjuvant Treatment Model'''
            WHEN table_name ILIKE 'lot_%_rbs_std' THEN '''Model Outputs: Standard (Regimen-based) Line of Therapy'''
            WHEN table_name ILIKE 'lot_%_pbs_std' THEN '''Model Outputs: Standard (Progression-based) Line of Therapy'''
            WHEN table_name ILIKE 'lot_%_var1' THEN '''Model Outputs: Variant1 Line of Therapy'''
            WHEN table_name ILIKE 'pj_%_claims_pbs_std' THEN '''Model Outputs: Standard (Progression-based) Patient Journey'''
            WHEN table_name ILIKE 'pj_%_claims_rbs_std' THEN '''Model Outputs: Standard (Regimen-based) Patient Journey'''
            WHEN table_name ILIKE 'lot_%_rbs_std_upcuration' THEN '''Model Outputs: Standard (Upcuration Regimen-based) Line of Therapy'''
            WHEN table_schema ILIKE '%_open_%' OR (table_schema ILIKE '%_360_%' AND table_schema NOT ILIKE '%_mo_%') THEN '''Core Data Product (Base)'''
            ELSE '''' || table_name || ''''
        END || ' AS Addon, ' ||
        CASE 
            WHEN table_schema ILIKE '%_breast%' OR table_name ILIKE '%_breast_%' THEN '''Breast Cancer'''
            WHEN table_schema ILIKE '%_bladder%' OR table_name ILIKE '%bladder%' THEN '''Bladder Cancer'''
            WHEN table_schema ILIKE '%_crc%' OR table_name ILIKE '%_crc_%' THEN '''Colorectal cancer (CRC)'''
            WHEN table_schema ILIKE '%_gec%' OR table_name ILIKE '%_gec_%' THEN '''Gastroesophageal Cancer (GEC)'''
            WHEN table_schema ILIKE '%_hcc%' OR table_name ILIKE '%_hcc_%' THEN '''Hepatocellular Carcinoma (HCC)'''
            WHEN table_schema ILIKE '%_melanoma%' OR table_name ILIKE '%_melanoma_%' THEN '''Melanoma'''
            WHEN table_schema ILIKE '%_nsclc%' OR table_name ILIKE '%_nsclc_%' THEN '''Non-Small Cell Lung Cancer (NSCLC)'''
            WHEN table_schema ILIKE '%_ovarian%' OR table_name ILIKE '%_ovarian_%' THEN '''Ovarian cancer'''
            WHEN table_schema ILIKE '%_prostate%' OR table_name ILIKE '%_prostate_%' THEN '''Prostate Cancer'''
            WHEN table_schema ILIKE '%_sclc%' OR table_name ILIKE '%_sclc_%' THEN '''Small cell lung cancer (SCLC)'''
            ELSE '''' || table_name || ''''
        END || ' AS indication, ' ||
        '''Standard Subscription''' || ' AS Subscription_Category, ' ||
        '''Standard''' || ' AS Subscription_Subcategory, ' ||
        'COUNT(DISTINCT chai_patient_id) AS Patient_Count ' ||
        'FROM ' || table_schema || '.' || table_name
    ) AS sql_code
FROM information_schema.columns 
WHERE table_schema IN (#schema_list)
AND (
    table_name ILIKE 'patient' OR 
    table_name ILIKE 'table1_patient' OR 
    table_name ILIKE 'outcomes' OR 
    table_name ILIKE '%_claims_%'
);
