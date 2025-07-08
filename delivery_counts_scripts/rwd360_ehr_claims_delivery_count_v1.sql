
drop table if exists #data_claims_02;

drop table if exists #data_claims_02;
with
data_claims_02 as (
select * from
(select 'Pan-Cancer' as Indication, chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1
union  select 'Acute Lymphoblastic Leukemia (ALL)' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  acll=1
union  select 'Acute Myeloid Leukemia (AML)' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  aml=1
union  select 'Acute promyelocytic leukemia (APL)' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  acute_promyelocytic_leukemia=1
union  select 'Anal Cancer' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  anal_cancer=1
union  select 'Anaplastic large cell lymphoma (ALCL)' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  anaplastic_large_cell_lymphoma=1
union  select 'Appendix Cancer' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  appendix=1
union  select 'Basal Cell Carcinoma (BCC)' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  bcc=1
union  select 'Biliary Tract Cancer (BTC)' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  btc=1
union  select 'Bladder Cancer' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  bladder=1
union  select 'Bone and Cartilage Cancer' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  bone=1
union  select 'Blastic plasmacytoid dendritic cell neoplasm (BPDCN)' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  bpdcn=1
union  select 'Brain Cancer' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  brain=1
union  select 'Breast Cancer' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  breast=1
union  select 'Burkitt Lymphoma' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  burkitt=1
union  select 'Carcinoid Syndrome' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  carcinoid_syndrome=1
union  select 'Castleman Disease' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  castleman=1
union  select 'Central nervous system (CNS) cancers' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  cns=1
union  select 'Cervical Cancer' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  cervical=1
union  select 'Chronic lymphocytic leukemia (CLL)' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  cll=1
union  select 'Chronic Myeloid Leukemia (CML)' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  cml=1
union  select 'Chronic Myelomonocytic Leukemia (CMML)' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  cmml=1
union  select 'Colon Cancer' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  colon=1
union  select 'Colorectal Cancer (CRC)' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  crc=1
union  select 'Cutaneous Squamous Cell Carcinoma (cSCC)' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  cscc=1
union  select 'Diffuse Large B-Cell Lymphoma (DLBCL)' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  dlbcl=1
union  select 'Endocrine Cancer' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  endocrine=1
union  select 'Endometrial Cancer' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  endometrial=1
union  select 'Esophageal Cancer' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  esophageal=1
union  select 'Essential thrombocythemia (ET)' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  essential_thrombocythemia=1
union  select 'Extramedullary Plasmacytoma' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  ep=1
union  select 'Female genital organ cancers' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  fgo=1
union  select 'Follicular lymphoma (FL)' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  fl=1
union  select 'Gastric Cancer' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  gastric=1
union  select 'Gastrointestinal Stromal Tumor (GIST)' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  gist=1
union  select 'Hairy cell leukemia (HCL)' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  hcl=1
union  select 'Head and neck cancer (HNSCC)' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  hnscc=1
union  select 'Heart, mediastinum, and pleura cancers' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  hmp=1
union  select 'Hepatocellular Carcinoma (HCC)' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  hcc=1
union  select 'Hodgkin''s lymphoma (HL)' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  hodgkins_lymphoma=1
union  select 'Kaposi Sarcoma' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  kaposi_sarcoma=1
union  select 'Lung Cancers' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  lung=1
union  select 'Lymphoblastic lymphoma (LL)' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  ll=1
union  select 'Male genital organ cancers' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  mgo=1
union  select 'Mantle Cell Lymphoma (MCL)' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  mcl=1
union  select 'Marginal Zone Lymphoma (MZL)' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  mzl=1
union  select 'Melanoma' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  melanoma=1
union  select 'Merkel Cell Carcinoma (MCC)' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  mcc=1
union  select 'Mesothelioma' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  mesothelioma=1
union  select 'Monoclonal gammopathy of undetermined significance (MGUS)' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  mgus=1
union  select 'Multiple Myeloma (MM)' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  mmy=1
union  select 'Mycosis Fungoides' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  mycosisf=1
union  select 'Myelodysplastic syndrome (MDS)' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  mds=1
union  select 'Myelofibrosis (MF)' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  mf=1
union  select 'Myeloid Sarcoma' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  myeloids=1
union  select 'Myeloproliferative neoplasms (MPN) - includes ET,PV, & MF' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  mpn=1
union  select 'Neuroendocrine tumors (NET)' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  nets=1
union  select 'Non-Hodgkin''s lymphoma (NHL)' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  nhl=1
union  select 'Ocular cancer' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  ocular=1
union  select 'Other Hematologic Malignancies' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  ohm=1
union  select 'Other solid tumor cancers' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  ost=1
union  select 'Ovarian cancer (count includes ovarian, fallopian, & peritoneal)' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  ovarian=1
union  select 'Pancreatic Cancer' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  pancreatic=1
union  select 'Peripheral nerves and autonomic nervous system (count includes Malignant peripheral nerve sheath tumor)' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  pnans=1
union  select 'Peripheral T cell lymphoma (PTCL)' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  peripheral_t_cell_lymphoma=1
union  select 'Plasma cell leukemia (PCL)' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  pcl=1
union  select 'Polycythemia vera (PV)' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  polycythemia_vera=1
union  select 'Prostate Cancer' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  prostate=1
union  select 'Renal Cell Carcinoma (RCC)' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  rcc=1
union  select 'Sezary disease' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  sd=1
union  select 'Small intestine cancer' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  smalli=1
union  select 'Small Lymphocytic Lymphoma (SLL)' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  sll=1
union  select 'Solitary Plasmacytoma' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  sp=1
union  select 'Systemic Mastocytosis' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  sm=1
union  select 'T-cell lymphoma' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  t_cell_lymphoma=1
union  select 'Testicular Cancer' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  testicular_cancer=1
union  select 'Thymus cancer' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  thymus=1
union  select 'Thyroid Cancer' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  thyroid_cancer=1
union  select 'Urothelial Cancer' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  urothelial=1
union  select 'Uterine cancer' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  uterine=1
union  select 'Waldenstrom macroglobulinemia (WM)' as Indication , chai_patient_id from #patient_metadata_schema.patient_metadata where delivered_bms =1 and  wm=1
union  select 'Fallopian Tube Cancer' as Indication , chai_patient_id from #rwd360_delivery_schema.condition where (diagnosis_code_code ilike 'C57.0%' or diagnosis_code_code ilike '183.2%')and (diagnosis_code_vocab ilike 'ICD10%' or diagnosis_code_vocab ilike 'ICD9%')
union  select 'Peritoneal Cancer' as Indication , chai_patient_id from #rwd360_delivery_schema.condition where (diagnosis_code_code ilike 'C48.1%' or diagnosis_code_code ilike 'C48.2%'	or diagnosis_code_code ilike '158.8%' 	or diagnosis_code_code ilike '158.9%') and (diagnosis_code_vocab ilike 'ICD10%' or diagnosis_code_vocab ilike 'ICD9%')
union  select 'Malignant peripheral nerve sheath tumor (count includes peripheral nerves & autonomic nervous system)' as Indication , chai_patient_id from #rwd360_delivery_schema.condition where (diagnosis_code_code ilike 'C47%' )and (diagnosis_code_vocab ilike 'ICD10%')
union  select 'Gastroesophageal Cancer (GEC)' as Indication , chai_patient_id from #rwd360_delivery_schema.condition where (diagnosis_code_code ilike 'C16%' 	or diagnosis_code_code ilike '151%' or diagnosis_code_code ilike 'C15%' or diagnosis_code_code ilike '150%') and (diagnosis_code_vocab ilike 'ICD10%' or diagnosis_code_vocab ilike 'ICD9%')
) where chai_patient_id in (select distinct chai_patient_id from #rwd360_claims_delivery_schema.patient)
)
select Indication, count(distinct chai_patient_id) as patient_count into #data_claims_02 from data_claims_02 group by 1;

select
#iteration as Iteration,
'#customer' as Customer,
delivery_type,
Product,
Format,
Addon,
Indication,
Subscription_Category,
Subscription_Subcategory,
Patient_Count
from
(
select 'RWD360-Claims' as delivery_type,
'RWD360' as Product,
'Open Claims - Common Data Model (CDM)' as format,
'Core Data Product (Base)' as addon,
indication,
'Standard Subscription' as subscription_category,
'Standard' as subscription_subcategory,
patient_count
from #data_claims_02);