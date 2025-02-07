import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import ssl
import pandas as pd
from jinja2 import Environment, FileSystemLoader
import config as con
import os
from get_receiver_email import  get_emails_by_client

# SMTP Server Configuration
port = con.port  # SMTP
smtp_server = con.smtp_server
sender_email = con.sender_email
user_name = con.user_name  # Your email for login
password = con.password  # App password (for 2FA-enabled accounts)
#client_name = con.client_name
client_name_list = con.client_name_list # List of client names
release = con.release
csv_file_path = con.csv_file_path
delivery_count_file = con.delivery_count_file
client_delivery_details = con.client_delivery_details
min_max_counts = con.min_max_counts

# List of Multiple Recipients
receiver_emails = con.receiver_emails
cc_emails = con.cc_emails
file_path = 'C:\\Users\\AbhijeetPalo\\PycharmProjects\\pythonProject\\venv\\DeliveryCount-Email-Generator\\email_templates\\receiver_list.txt'
#rcv_mail = RcvMail(file_path)
rcv_maillist =get_emails_by_client(client_name)
print (rcv_maillist)
# Subject of the Email


subject = "Delivery Counts:"+client_name+", "+str(release)

# Function to convert DataFrame to HTML table (unchanged)
def convert_table_to_html(df, req_columns=['Product', 'Format', 'Addon', 'Indication', 'Subscription Category',
                                           'Subscription Subcategory', 'Patient Count'],
                          rename_dict={'Product': 'Product', 'Format': 'Format', 'Addon': 'Addon',
                                       'Indication': 'Indication', 'Patient Count': 'Patient Count'}):
    '''Converts pandas dataframe to a basic html table'''

    df.columns = df.columns.str.strip()  # Removes leading/trailing spaces
    df.columns = df.columns.str.title()  # Capitalize column names for consistency

    client_df = df.copy()
    missing_columns = [col for col in req_columns if col not in df.columns]
    if missing_columns:
        print(f"Warning: The following requested columns are missing: {missing_columns}")

    client_df = client_df[req_columns]  # Filter required columns
    client_df = client_df.rename(columns=rename_dict)  # Rename columns

    client_df = client_df.style \
        .set_properties(**{'font-size': '9pt', 'font-family': 'Calibri',
                           'text-align': 'left', 'min-width': '120px'}) \
        .set_table_styles(
        [{'selector': 'th',
          'props': [('color', '#f6f6f6'),
                    ('background', '#000F8C'),
                    ('padding', '0px'), ('border', '1px'), ('margin', '0px'),
                    ('text-align', 'left'), ('font-size', '10pt'),
                    ('min-width', '120px')]},
         {'selector': 'tr',
          'props': [('color', '#18325A'),
                    ('background', '#f6f6f6'),
                    ('padding', '0px'), ('margin', '0px')]},
         {'selector': 'tr:nth-of-type(odd)',
          'props': [('color', '#18325A'),
                    ('background', '#e9e9e9'),
                    ('padding', '0px'), ('margin', '0px')]},
         {'selector': 'table',
          'props': [('border-collapse', 'collapse'), ('margin', '0px')]},
         ]
    ) \
        .hide(axis='index')

    # Convert to a simple HTML table
    table_html = client_df.to_html(index=False, escape=False, border=1, justify='left')

    return table_html


# Function to generate email content (unchanged)
def generate_client_email_text(client_df, client_name, release, delivery_products_list, cs_lead_name, cs_lead_email,
                               delivery_method, min_max_date_df, s3_location):
    '''Generates the HTML file for a client email.'''
    # Same content as provided in the previous code
    try:
        rwd360_min_date = None
        rwd360_max_date = None
        pt360_min_date = None
        pt360_max_date = None
        gn360_min_date = None
        gn360_max_date = None

        if 'RWD360' in client_df['product'].values:
            # RWD360 block: Filter min_max_date_df for 'RWD360' only
            rwd360 = min_max_date_df[(min_max_date_df['product'] == 'RWD360') & (min_max_date_df['client'] ==client_name )]
            rwd360_min_date = rwd360['min_date'].min()  # Min date in 'min_date' column
            rwd360_max_date = rwd360['max_date'].max()
            rwd360_min_date = f"Minimum Date Contained in RWD360 data: {rwd360_min_date}"
            rwd360_max_date = f"Maximum Date Contained in RWD360 data: {rwd360_max_date}"

        if 'Patient360' in client_df['product'].values:
            # RWD360 block: Filter min_max_date_df for 'RWD360' only
            pt360 = min_max_date_df[(min_max_date_df['product'] == 'PT360') & (min_max_date_df['client'] ==client_name )]
            pt_min_date = pt360['min_date'].min()  # Min date in 'min_date' column
            pt_max_date = pt360['max_date'].max()
            pt360_min_date = f"Minimum Date Contained in PT360 data: {pt_min_date}"
            pt360_max_date = f"Maximum Date Contained in PT360 data: {pt_max_date}"

        if 'GN360' in client_df['product'].values:
            # RWD360 block: Filter min_max_date_df for 'RWD360' only
            gn360 = min_max_date_df[(min_max_date_df['product'] == 'GN360') & (min_max_date_df['client'] ==client_name )]
            gn360_min_date = gn360['min_date'].min()  # Min date in 'min_date' column
            gn360_max_date = gn360['max_date'].max()
            gn360_min_date = f"Minimum Date Contained in GN360 data: {gn360_min_date}"
            gn360_max_date = f"Maximum Date Contained in GN360 data: {gn360_max_date}"


        date_list = [rwd360_min_date, rwd360_max_date, pt360_min_date, pt360_max_date, gn360_min_date, gn360_max_date]
        client_product_min_max_dates = "<br>".join(date for date in date_list if date is not None)

        environment = Environment(loader=FileSystemLoader(
            "C:\\Users\\AbhijeetPalo\\PycharmProjects\\pythonProject\\venv\\DeliveryCount-Email-Generator\\email_templates"))
        template = environment.get_template("body_default.txt")
        body = template.render(
            client_name=client_name,
            release=release,
            delivery_products_list=delivery_products_list,
            cs_lead_name=cs_lead_name,
            cs_lead_email=cs_lead_email,
            delivery_method=delivery_method,
            location=s3_location,
            password=file_password
        )
    except TemplateNotFound as e:
        print(f"Template not found: {e}")
    except Exception as e:
        print(f"An error occurred while loading the template: {e}")

    # Generate the signature
    template = environment.get_template("signature_default.txt")
    signature = template.render(
        client_name=client_name,
        release=release,
        cs_lead_name=cs_lead_name,
        cs_lead_email=cs_lead_email,
        client_product_min_max_dates=client_product_min_max_dates
    )


    # Separate standard and adhoc deliveries
    regular_df = client_df[client_df['subscription subcategory'].str.upper() == 'STANDARD']
    adhoc_df = client_df[client_df['subscription subcategory'].str.upper() == 'ADHOC']
    print(regular_df)
    print(adhoc_df)
    email_text = body  # Start with the body of the email
    email_text += client_product_min_max_dates
    # Add HTML for regular and adhoc deliveries if data is available
    if not regular_df.empty:
        reg_html = convert_table_to_html(regular_df)
        email_text += f'<h3>Standard Deliveries:</h3>{reg_html}'

    if not adhoc_df.empty:
        adhoc_html = convert_table_to_html(adhoc_df)
        email_text += f'<h3>Adhoc Deliveries:</h3>{adhoc_html}'

    # Add the signature
    email_text += signature

    return email_text



# Function to send email with attachments (Modified for multiple attachments)
def send_email(smtp_server, port, sender_email, receiver_emails, subject, user_name, password, df, client_name,
               delivery_products_list, release, cs_lead_name, cs_lead_email, delivery_method, min_max_date_df,
               s3_location, attachment_files):
    try:
        # Generate the email content
        body = generate_client_email_text(df, client_name, release, delivery_products_list, cs_lead_name, cs_lead_email,
                                          delivery_method, min_max_date_df, s3_location)

        # Create the email message
        message = MIMEMultipart()
        message['From'] = sender_email
        message['Subject'] = subject

        # Attach the generated HTML body
        message.attach(MIMEText(body, 'html'))  # 'html' for HTML content
        message['To'] = ", ".join(rcv_maillist)
        message['Cc'] = ", ".join(cc_emails)  # Adding CC recipients

        # Attach the Excel files (Multiple attachments)
        for file_path in attachment_files:
            print(file_path)
            if file_path is not None and os.path.exists(file_path):  # Ensure file exists and is not None
                filename = os.path.basename(file_path)
                print(f"Preparing to attach: {filename}")

                try:
                    # Open the file and read its contents
                    with open(file_path, "rb") as attachment:
                        file_content = attachment.read()  # Read the file content
                        if not file_content:
                            print(f"Warning: The file {filename} is empty.")
                            continue  # Skip attaching empty files

                        part = MIMEBase('application', 'octet-stream')  # Create MIMEBase object for the attachment
                        part.set_payload(file_content)  # Set the file content as the payload
                        encoders.encode_base64(part)  # Encode the attachment content

                        # Add header for the attachment
                        part.add_header('Content-Disposition', f'attachment; filename={filename}')
                        message.attach(part)  # Attach the part (file) to the email message

                        print(f"Attachment {filename} added successfully.")
                except Exception as e:
                    print(f"Error opening or reading file {file_path}: {e}")
            else:
                print(f"Warning: The file {file_path} does not exist or is None.")

        # Secure SSL context
        context = ssl.create_default_context()

        # Connect to SMTP server and send email
        with smtplib.SMTP(smtp_server, port) as server:
            server.ehlo()  # Can be omitted
            server.starttls(context=context)  # Secure connection
            server.login(user_name, password)  # Login with email and app password
            server.sendmail(sender_email, rcv_maillist, message.as_string())
            print("Email sent successfully to:", ", ".join(rcv_maillist))

    except Exception as e:
        print("Failed to send email")
        print(e)


if __name__ == '__main__':
    # Load the DataFrame from CSV (unchanged)
    df = pd.read_csv(csv_file_path + delivery_count_file)
    del_cnt_df = df[df['customer'].str.upper() == client_name.upper()]
    print(len(del_cnt_df))
    min_max_date_df = pd.read_csv(csv_file_path + 'max_min_counts.csv')
    client_details_df = pd.read_csv(csv_file_path + 'client-delivery-details.csv')
    filtered_client_details_df = client_details_df[client_details_df['client'] == client_name.upper()]

    delivery_products_list = del_cnt_df['product'].unique().tolist()

    # Get CS Lead info and delivery method
    cs_lead_name = filtered_client_details_df['cs_lead_name'].iloc[0]
    cs_lead_email = filtered_client_details_df['cs_lead_email'].iloc[0]
    delivery_method = filtered_client_details_df['deliverymethod'].iloc[0]

    if filtered_client_details_df['s3location'].empty:
        s3location = 'NOT APPLICABLE'
    else:
        s3_location = filtered_client_details_df['s3location'].iloc[0]

    file_password = filtered_client_details_df['password'].iloc[0]
    if pd.isna(file_password):
        file_password = 'NOT APPLICABLE'

    attachment_files = con.attachment_files  # Load attachment files from config
    print("Attachment files:", attachment_files)
    # Send the email with attachments
    send_email(smtp_server, port, sender_email, receiver_emails, subject, user_name,password, del_cnt_df, client_name,
               delivery_products_list, release, cs_lead_name, cs_lead_email, delivery_method, min_max_date_df,
               s3_location, attachment_files)
