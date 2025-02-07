# SMTP Server Configuration
port = 587  # SMTP
smtp_server = "smtp.office365.com"
sender_email = "ses_service@concertai.com"
user_name = 'ses_service@concertai.onmicrosoft.com'  # Your email for login
password = 'AWg&lc87-2$'  # App password (for 2FA-enabled accounts)
client_name = 'ABBVIE'  #   Change client
#delivery_products_list = ['RWD360™', 'Patient360™']
release = 202412
csv_file_path = 'C:/Users/AbhijeetPalo/OneDrive - ConcertAI/SAI/ClientDeliveryCount/'
delivery_count_file='01_All_data_products_delivery_counts_202412_v2.csv'
client_delivery_details='client-delivery-details.csv'
min_max_counts='max_min_counts.csv'
receiver_emails = ["sgoda@concertai.com","apalo@concertai.com", "ssm@concertai.com"]
#cc_emails = ["apalo@concertai.com", "ssm@concertai.com","bkumar@concertai.com","vakki@concertai.com"]
#receiver_emails=["apalo@concertai.com", "ssm@concertai.com"]
cc_emails=["ssm@concertai.com"]

#add for Amgen --"C:/Users/AbhijeetPalo/OneDrive - ConcertAI/SAI/ClientDeliveryCount/EmailAttachment/202412_ConcertAI Genome360 Data Dictionary.xlsx",

attachment_files = [
    "C:/Users/AbhijeetPalo/OneDrive - ConcertAI/SAI/ClientDeliveryCount/EmailAttachment/202412_ConcertAI Patient360 Data Dictionary.xlsx",
    "C:/Users/AbhijeetPalo/OneDrive - ConcertAI/SAI/ClientDeliveryCount/EmailAttachment/20241125_ConcertAI_Claims_CDM_Data_Dictionary.xlsx",
    "C:/Users/AbhijeetPalo/OneDrive - ConcertAI/SAI/ClientDeliveryCount/EmailAttachment/20241215_ConcertAI RWD360 Data Dictionary.xlsx",
    #"C:/Users/AbhijeetPalo/OneDrive - ConcertAI/SAI/ClientDeliveryCount/EmailAttachment/202412_ConcertAI Genome360 Data Dictionary.xlsx",
    "C:/Users/AbhijeetPalo/OneDrive - ConcertAI/SAI/ClientDeliveryCount/EmailAttachment/data_delivery_erace_emory_202412.xlsx"
]