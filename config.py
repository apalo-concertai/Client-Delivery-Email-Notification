# SMTP Server Configuration
port = 587  # SMTP
smtp_server = "smtp.office365.com"
sender_email = "ses_service@concertai.com"
user_name = 'ses_service@concertai.onmicrosoft.com'  # Your email for login
password = 'AWg&lc87-2$'  # App password (for 2FA-enabled accounts)
client_name_list =['Verana']
cc_emails=["apalo@concertai.com"]
#client_name = 'ABBVIE'  #   Change client
#delivery_products_list = ['RWD360™', 'Patient360™']
release = 202501
csv_file_path = "C:/Users/AbhijeetPalo/OneDrive - ConcertAI/SAI/ClientDeliveryCount/"
delivery_count_file='master_data_delivery_202501.csv'
client_delivery_details='client-delivery-details.csv'
min_max_counts='master_dates_data_delivery_202501.csv'
iteration= 1

dd_attachemnt_path_file = 'C:\\Users\\AbhijeetPalo\\PycharmProjects\\pythonProject\\venv\\DeliveryCount-Email-Generator\\email_templates\\dd_attachemnts.txt'

#add for Amgen --"C:/Users/AbhijeetPalo/OneDrive - ConcertAI/SAI/ClientDeliveryCount/EmailAttachment/202412_ConcertAI Genome360 Data Dictionary.xlsx",



RWD360_DD ="C:/Users/AbhijeetPalo/OneDrive - ConcertAI/SAI/ClientDeliveryCount/EmailAttachment/20241215_ConcertAI RWD360 Data Dictionary.xlsx"
PT360_DD="C:/Users/AbhijeetPalo/OneDrive - ConcertAI/SAI/ClientDeliveryCount/EmailAttachment/202412_ConcertAI Patient360 Data Dictionary.xlsx"
GN360_DD="C:/Users/AbhijeetPalo/OneDrive - ConcertAI/SAI/ClientDeliveryCount/EmailAttachment/202412_ConcertAI Genome360 Data Dictionary.xlsx"
Claims_DD ="C:/Users/AbhijeetPalo/OneDrive - ConcertAI/SAI/ClientDeliveryCount/EmailAttachment/20241125_ConcertAI_Claims_CDM_Data_Dictionary.xlsx"

attachment_files = [
    "C:/Users/AbhijeetPalo/OneDrive - ConcertAI/SAI/ClientDeliveryCount/EmailAttachment/202412_ConcertAI Patient360 Data Dictionary.xlsx",
    "C:/Users/AbhijeetPalo/OneDrive - ConcertAI/SAI/ClientDeliveryCount/EmailAttachment/20241125_ConcertAI_Claims_CDM_Data_Dictionary.xlsx",
    "C:/Users/AbhijeetPalo/OneDrive - ConcertAI/SAI/ClientDeliveryCount/EmailAttachment/20241215_ConcertAI RWD360 Data Dictionary.xlsx",
    #"C:/Users/AbhijeetPalo/OneDrive - ConcertAI/SAI/ClientDeliveryCount/EmailAttachment/202412_ConcertAI Genome360 Data Dictionary.xlsx",
    "C:/Users/AbhijeetPalo/OneDrive - ConcertAI/SAI/ClientDeliveryCount/EmailAttachment/data_delivery_erace_emory_202412.xlsx"
]