
select * from (
    -- HER2 Status
select * from (
    select distinct
        'UNION select ' ||
        #iteration || ' AS Iteration, '||
		'''#customer''' ||' AS Customer, '||
        '''RWD360-Claims'' as delivery_type,'||
		'''RWD360'' as Product,'||
        '''Electronic Medical Records (EMR) - Common Data Model (CDM)'' as Format, ' ||
        '''HER2 '' || test_result_name as Addon, ' ||
        '''Breast Cancer'' as Indication, ' ||
        '''Standard Subscription'' as Subscription_Category, ' ||
        '''Standard'' as Subscription_Subcategory, ' ||
        'count(distinct chai_patient_id) as Patient_Count ' ||
        'from ' || table_schema || '.' || table_name || ' group by 1,2,3,4,5,6,7' as sql_code
    from information_schema.columns
    where table_schema = '#rwd360_delivery_schema' and table_name = 'her2_status_mo'
)
union all
    -- TNBC Diagnosis
    select * from (
    select distinct
        'UNION select ' ||
        #iteration || ' AS Iteration, '||
		'''#customer''' ||' AS Customer, '||
        '''RWD360-Claims'' as delivery_type,'||
		'''RWD360'' as Product,'||
        '''Electronic Medical Records (EMR) - Common Data Model (CDM)'' as Format, ' ||
        'diagnosis_code_name as Addon, ' ||
        '''Breast Cancer'' as Indication, ' ||
        '''Standard Subscription'' as Subscription_Category, ' ||
        '''Standard'' as Subscription_Subcategory, ' ||
        'count(distinct chai_patient_id) as Patient_Count ' ||
        'from ' || table_schema || '.' || table_name || ' group by 1,2,3,4,5,6,7' as sql_code
    from information_schema.columns
    where table_schema = '#rwd360_delivery_schema' and table_name = 'hr_tnbc_dx_date_mo'
)
    union all
-- Patient Status: pred_met_bc
    select * from (
    select distinct
        'UNION select ' ||
        #iteration || ' AS Iteration, '||
		'''#customer''' ||' AS Customer, '||
        '''RWD360-Claims'' as delivery_type,'||
		'''RWD360'' as Product,'||
        '''Electronic Medical Records (EMR) - Common Data Model (CDM)'' as Format, ' ||
        '''Model Outputs: mBC status Pred_met_bc in patient_status_mo'' as Addon, ' ||
        '''Breast Cancer'' as Indication, ' ||
        '''Standard Subscription'' as Subscription_Category, ' ||
        '''Standard'' as Subscription_Subcategory, ' ||
        'count(distinct chai_patient_id) as Patient_Count ' ||
        'from (select distinct chai_patient_id from ' || table_schema || '.' || table_name || ' where pred_met_bc=1) ' as sql_code
    from information_schema.columns
    where table_schema = '#rwd360_delivery_schema' and table_name = 'patient_status_mo'
)
    union all
    -- Patient Status: All 3 predicted models
    select * from (
    select distinct
        'UNION select ' ||
        #iteration || ' AS Iteration, '||
		'''#customer''' ||' AS Customer, '||
        '''RWD360-Claims'' as delivery_type, ' ||
        '''RWD360'' as Product, ' ||
        '''Electronic Medical Records (EMR) - Common Data Model (CDM)'' as Format, ' ||
        ' Addon, ' ||
        'Indication, ' ||
        '''Standard Subscription'' as Subscription_Category, ' ||
        '''Standard'' as Subscription_Subcategory, ' ||
		'patient_count ' ||
		'from ( '||
		'with '||
        'pred_met_bc as (select distinct chai_patient_id from ' || table_schema || '.' || table_name || ' where pred_met_bc=1), ' ||
        'pred_nsclc as (select distinct chai_patient_id from ' || table_schema || '.' || table_name || ' where pred_nsclc=1), ' ||
        'pred_sclc as (select distinct chai_patient_id from ' || table_schema || '.' || table_name || ' where pred_sclc=1) ' ||
        'select ''Pan-Cancer : pred_met_bc'' as Indication,''patient_status_mo Model Outputs: mBC status'' as addon, count(distinct chai_patient_id) as Patient_Count from pred_met_bc ' ||
        'union select ''Pan-Cancer : pred_nsclc'' as Indication,''patient_status_mo Model Outputs: NSCLC status'' as addon, count(distinct chai_patient_id) as Patient_Count from pred_nsclc ' ||
        'union select ''Pan-Cancer : pred_sclc'' as Indication,''patient_status_mo Model Outputs: SCLC status'' as addon, count(distinct chai_patient_id) as Patient_Count from pred_sclc' ||
        ')'
        as sql_code
    from information_schema.columns
    where table_schema = '#rwd360_delivery_schema'
      and table_name = 'patient_status_mo'
)
    union all
    -- Line of Therapy std (Claims)
    select * from (
    select distinct
        'UNION select ' ||
        #iteration || ' AS Iteration, '||
		'''#customer''' ||' AS Customer, '||
        '''RWD360-Claims'' as delivery_type,'||
		'''RWD360'' as Product,'||
        '''Open Claims - Common Data Model (CDM)'' as Format, ' ||
        'case when diagnosis_code_name ilike ''CLL_SLL'' then ' ||
        '''Model Outputs: Standard (Regimen-based) Line of Therapy–Both CLL & SLL'' ' ||
        'else ''Model Outputs: Standard (Regimen-based) Line of Therapy'' end as Addon, ' ||
        'case ' ||
        'when diagnosis_code_name ilike ''AML'' then ''Acute myeloid leukemia (AML)'' ' ||
        'when diagnosis_code_name ilike ''CLL_SLL'' then ''Chronic lymphocytic leukemia (CLL)'' ' ||
        'when diagnosis_code_name ilike ''Colorectal Cancer'' then ''Colorectal cancer (CRC)'' ' ||
        'when diagnosis_code_name ilike ''DLBCL'' then ''Diffuse large B-Cell lymphoma (DLBCL)'' ' ||
        'when diagnosis_code_name ilike ''Gastric Esophagus Cancer'' then ''Gastroesophageal cancer (GEC)'' ' ||
        'when diagnosis_code_name ilike ''HCC'' then ''Hepatocellular carcinoma (HCC)'' ' ||
        'when diagnosis_code_name ilike ''Head and Neck Cancer'' then ''Head and neck cancer (HNSCC)'' ' ||
        'when diagnosis_code_name ilike ''Lung Cancer'' then ''Lung Cancers'' ' ||
        'when diagnosis_code_name ilike ''MCL'' then ''Mantle cell lymphoma (MCL)'' ' ||
        'when diagnosis_code_name ilike ''Melanoma Cancer'' then ''Melanoma'' ' ||
        'when diagnosis_code_name ilike ''MM'' then ''Multiple myeloma (MM)'' ' ||
        'when diagnosis_code_name ilike ''Ovarian_Fallopian_Peritoneal Cancer'' then ''Ovarian cancer (count includes ovarian, fallopian, & peritoneal)'' ' ||
        'when diagnosis_code_name ilike ''Renal Cancer'' then ''Renal cell carcinoma (RCC)'' ' ||
        'else diagnosis_code_name end as Indication, ' ||
        '''Standard Subscription'' as Subscription_Category, ' ||
        '''Standard'' as Subscription_Subcategory, ' ||
        'count(distinct chai_patient_id) as Patient_Count ' ||
        'from ' || table_schema || '.' || table_name || ' group by 1,2,3,4,5,6,7' as sql_code
    from information_schema.columns
    where table_schema = '#rwd360_delivery_schema' and table_name = 'line_of_therapy_claims_std'
)
    union all
    -- Line of Therapy var1 (Claims)
    select * from (
    select distinct
        'UNION select ' ||
        #iteration || ' AS Iteration, '||
		'''#customer''' ||' AS Customer, '||
        '''RWD360-Claims'' as delivery_type,'||
		'''RWD360'' as Product,'||
        '''Open Claims - Common Data Model (CDM)'' as Format, ' ||
        'case when diagnosis_code_name ilike ''CLL_SLL'' then ' ||
        '''Model Outputs: Variant1 Line of Therapy–Both CLL & SLL'' ' ||
        'else ''Model Outputs: Variant1 Line of Therapy'' end as Addon, ' ||
        'case ' ||
        'when diagnosis_code_name ilike ''AML'' then ''Acute myeloid leukemia (AML)'' ' ||
        'when diagnosis_code_name ilike ''CLL_SLL'' then ''Chronic lymphocytic leukemia (CLL)'' ' ||
        'when diagnosis_code_name ilike ''Colorectal Cancer'' then ''Colorectal cancer (CRC)'' ' ||
        'when diagnosis_code_name ilike ''DLBCL'' then ''Diffuse large B-Cell lymphoma (DLBCL)'' ' ||
        'when diagnosis_code_name ilike ''Gastric Esophagus Cancer'' then ''Gastroesophageal cancer (GEC)'' ' ||
        'when diagnosis_code_name ilike ''HCC'' then ''Hepatocellular carcinoma (HCC)'' ' ||
        'when diagnosis_code_name ilike ''Head and Neck Cancer'' then ''Head and neck cancer (HNSCC)'' ' ||
        'when diagnosis_code_name ilike ''Lung Cancer'' then ''Lung Cancers'' ' ||
        'when diagnosis_code_name ilike ''MCL'' then ''Mantle cell lymphoma (MCL)'' ' ||
        'when diagnosis_code_name ilike ''Melanoma Cancer'' then ''Melanoma'' ' ||
        'when diagnosis_code_name ilike ''MM'' then ''Multiple myeloma (MM)'' ' ||
        'when diagnosis_code_name ilike ''Ovarian_Fallopian_Peritoneal Cancer'' then ''Ovarian cancer (count includes ovarian, fallopian, & peritoneal)'' ' ||
        'when diagnosis_code_name ilike ''Renal Cancer'' then ''Renal cell carcinoma (RCC)'' ' ||
        'else diagnosis_code_name end as Indication, ' ||
        '''Standard Subscription'' as Subscription_Category, ' ||
        '''Standard'' as Subscription_Subcategory, ' ||
        'count(distinct chai_patient_id) as Patient_Count ' ||
        'from ' || table_schema || '.' || table_name || ' group by 1,2,3,4,5,6,7' as sql_code
    from information_schema.columns
    where table_schema = '#rwd360_delivery_schema' and table_name = 'line_of_therapy_claims_var1'
)
    union all
    -- PJ SCLC Claims
    select * from (
    select distinct
        'UNION select ' ||
        #iteration || ' AS Iteration, '||
		'''#customer''' ||' AS Customer, '||
        '''RWD360-Claims'' as delivery_type,'||
		'''RWD360'' as Product,'||
        '''Open Claims - Common Data Model (CDM)'' as Format, ' ||
        '''Model Outputs: Standard (Regimen-based) Patient Journey''' || ' as Addon, ' ||
            '''Small cell lung cancer (SCLC)''' || ' as Indication, ' ||
        '''Standard Subscription'' as Subscription_Category, ' ||
        '''Standard'' as Subscription_Subcategory, ' ||
        'count(distinct chai_patient_id) as Patient_Count ' ||
        'from ' || table_schema || '.' || table_name || ' group by 1,2,3,4,5,6,7' as sql_code
    from information_schema.columns
    where table_schema = '#rwd360_delivery_schema' and table_name = 'pj_sclc_claims_rbs_std'
)
    union all
    -- SCLC Status Model Outputs (EMR)
    select * from (
    select distinct
        'UNION select ' ||
        #iteration || ' AS Iteration, '||
		'''#customer''' ||' AS Customer, '||
        '''RWD360-Claims'' as delivery_type,'||
		'''RWD360'' as Product,'||
        '''Electronic Medical Records (EMR) - Common Data Model (CDM)'' as Format, ' ||
        '''Model Outputs: Patient Status Model'' as Addon, ' ||
        '''Small cell lung cancer (SCLC)'' as Indication, ' ||
        '''Standard Subscription'' as Subscription_Category, ' ||
        '''Standard'' as Subscription_Subcategory, ' ||
        'count(distinct chai_patient_id) as Patient_Count ' ||
        'from ' || table_schema || '.' || table_name || ' where pred_sclc=1 group by 1,2,3,4,5,6,7' as sql_code
    from information_schema.columns
    where table_schema = '#rwd360_delivery_schema' and table_name = 'patient_status_mo'
)
union all
select * from (
    select distinct
        'UNION select ' ||
        #iteration || ' AS Iteration, '||
		'''#customer''' ||' AS Customer, '||
        '''RWD360-Claims'' as delivery_type, ' ||
        '''RWD360'' as Product, ' ||
        '''Electronic Medical Records (EMR) - Common Data Model (CDM)'' as Format, ' ||
        '''Model Outputs: Patient Status Model'' as Addon, ' ||
        'Indication, ' ||
        '''Standard Subscription'' as Subscription_Category, ' ||
        '''Standard'' as Subscription_Subcategory, ' ||
		'patient_count ' ||
		'from ( '||
		'with '||
        'pred_met_bc as (select distinct chai_patient_id from ' || table_schema || '.' || table_name || ' where pred_met_bc=1), ' ||
        'pred_nsclc as (select distinct chai_patient_id from ' || table_schema || '.' || table_name || ' where pred_nsclc=1), ' ||
        'pred_sclc as (select distinct chai_patient_id from ' || table_schema || '.' || table_name || ' where pred_sclc=1) ' ||
        'select ''mBC status'' as Indication, count(distinct chai_patient_id) as Patient_Count from pred_met_bc ' ||
        'union select ''NSCLC histology status'' as Indication, count(distinct chai_patient_id) as Patient_Count from pred_nsclc ' ||
        'union select ''Small cell lung cancer (SCLC)'' as Indication, count(distinct chai_patient_id) as Patient_Count from pred_sclc' ||
        ')'
        as sql_code
    from information_schema.columns
    where table_schema = '#rwd360_delivery_schema'
      and table_name = 'patient_status_mo')
)
;