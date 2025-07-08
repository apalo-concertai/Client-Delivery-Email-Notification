drop table if exists #data_labels_04;
with 
data_labels_04 as
(select 
'RWD360-Claims' as delivery_type
,'RWD360' as Product
,'Open Claims - Common Data Model (CDM)' as Format
,'Model Outputs: Standard (Regimen-based) Line of Therapy' as Addon
,'Fallopian tube cancer' as Indication
,'Standard Subscription' as Subscription_Category
,'Standard' as Subscription_Subcategory
, 'Included in ovarian' Patient_Count
union
select 
'RWD360-Claims' as delivery_type
,'RWD360' as Product
,'Open Claims - Common Data Model (CDM)' as Format
,'Model Outputs: Variant1 Line of Therapy' as Addon
,'Fallopian tube cancer' as Indication
,'Standard Subscription' as Subscription_Category
,'Standard' as Subscription_Subcategory
, 'Included in ovarian' Patient_Count
union
select 
'RWD360-Claims' as delivery_type
,'RWD360' as Product
,'Open Claims - Common Data Model (CDM)' as Format
,'Model Outputs: Standard (Regimen-based) Line of Therapy' as Addon
,'Peritoneal cancer' as Indication
,'Standard Subscription' as Subscription_Category
,'Standard' as Subscription_Subcategory
, 'Included in ovarian' Patient_Count
union
select
'RWD360-Claims' as delivery_type
,'RWD360' as Product
,'Open Claims - Common Data Model (CDM)' as Format
,'Model Outputs: Variant1 Line of Therapy' as Addon
,'Peritoneal cancer' as Indication
,'Standard Subscription' as Subscription_Category
,'Standard' as Subscription_Subcategory
, 'Included in ovarian' Patient_Count
union
select 
'RWD360-Claims' as delivery_type
,'RWD360' as Product
,'Open Claims - Common Data Model (CDM)' as Format
,'Model Outputs: Standard (Regimen-based) Line of Therapy' as Addon
,'Small lymphocytic lymphoma (SLL)' as Indication
,'Standard Subscription' as Subscription_Category
,'Standard' as Subscription_Subcategory
, 'Included in CLL LOT' Patient_Count
)select * into #data_labels_04 from data_labels_04;

select
'1' as iteration,
'BMS' as customer,
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
select delivery_type,
Product,
format,
addon,
indication,
subscription_category,
subscription_subcategory,
patient_count  from #data_labels_04
)
order by Indication,Addon,Format,Product
; 

