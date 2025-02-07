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
from get_receiver_email import get_emails_by_client
import argparse
import sys

# SMTP Server Configuration
port = con.port  # SMTP
smtp_server = con.smtp_server
sender_email = con.sender_email
user_name = con.user_name  # Your email for login
password = con.password  # App password (for 2FA-enabled accounts)
# client_name = con.client_name
client_name_list = con.client_name_list  # List of client names
release = con.release
csv_file_path = con.csv_file_path
delivery_count_file = con.delivery_count_file
client_delivery_details = con.client_delivery_details
min_max_counts = con.min_max_counts
product_paths_file=con.dd_attachemnt_path_file

# List of Multiple Recipients
# receiver_emails = con.receiver_emails
cc_emails = con.cc_emails
file_path = 'C:\\Users\\AbhijeetPalo\\PycharmProjects\\pythonProject\\venv\\DeliveryCount-Email-Generator\\email_templates\\receiver_list.txt'


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
def generate_client_email_text(client_df, client_name, release, delivery_products_list, delivery_type_list,
                               cs_lead_name, cs_lead_email,
                               delivery_method, min_max_date_df, s3_location):
    '''Generates the HTML file for a client email.'''
    # Same content as provided in the previous code,
    try:

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

    email_text = ""
    for product in delivery_type_list:
        date_list = []
        product_df = client_df[client_df['delivery_type'].str.upper() == product.upper()]

        if product in delivery_type_list:
            # Filter min_max_date_df
            product_min_max_df = min_max_date_df[
                (min_max_date_df['product'] == product) & (min_max_date_df['client'] == client_name)
                ]

            # Extract the min and max dates if data exists
            if not product_min_max_df.empty:
                min_date = product_min_max_df['min_date'].min()  # Min date in 'min_date' column
                max_date = product_min_max_df['max_date'].max()

                # Format the min and max date information for this product
                min_date_text = f"Minimum Date Contained in {product} data: {min_date}"
                max_date_text = f"Maximum Date Contained in {product} data: {max_date}"

                # Add  date information to the date_list
                date_list.append(min_date_text)
                date_list.append(max_date_text)

        # If the product DataFrame is not empty, add the product counts and the min/max date information
        if not product_df.empty:
            # Join the date information into a single string
            client_product_min_max_dates = "<br>".join(date for date in date_list if date is not None)

            # Add the date information to the email text (above the product table)
            if client_product_min_max_dates:
                email_text += f"<br>{client_product_min_max_dates}"

            # Convert the product counts DataFrame to an HTML table and add it to the email text
            product_html = convert_table_to_html(product_df)
            email_text += f'<h3>{product} Counts:</h3>{product_html}'

    # Generate the signature
    template = environment.get_template("signature_default.txt")
    signature = template.render(
        client_name=client_name,
        release=release,
        cs_lead_name=cs_lead_name,
        cs_lead_email=cs_lead_email,
        client_product_min_max_dates=''
    )

    # Add the signature at the end of the email
    print("email_test-")
    email_text += signature

    return email_text

def load_product_paths(file_path):
    product_paths = {}
    try:
        with open(file_path, "r") as f:
            for line in f:
                line = line.strip()
                if '=' in line:
                    key, value = line.split("=", 1)
                    product_paths[key.strip()] = value.strip()
    except Exception as e:
        print(f"Error loading product paths: {e}")
    print('product_path:', product_paths)
    return product_paths


# Function to send email with attachments (Modified for multiple attachments)
def send_email(smtp_server, port, sender_email, receiver_emails, subject, user_name, password, df, client_name,
               delivery_products_list, delivery_type_list, release, cs_lead_name, cs_lead_email, delivery_method,
               min_max_date_df,s3_location, attachment_files,product_paths_file):
    try:
        # Generate the email content
        # Create the directory path for saving the Excel file
        file_paths = load_product_paths(product_paths_file)
        save_directory = os.path.join(con.csv_file_path, client_name, str(release))
        # Ensure the directory exists
        os.makedirs(save_directory, exist_ok=True)
        body = generate_client_email_text(df, client_name, release, delivery_products_list, delivery_type_list,
                                          cs_lead_name, cs_lead_email,
                                          delivery_method, min_max_date_df, s3_location)
        subject = "Delivery Counts:" + client_name + ", " + str(release)
        # Create the email message
        message = MIMEMultipart()
        message['From'] = sender_email
        message['Subject'] = subject

        # Attach the generated HTML body
        message.attach(MIMEText(body, 'html'))  # 'html' for HTML content
        message['To'] = ", ".join(rcv_maillist)
        message['Cc'] = ", ".join(cc_emails)  # Adding CC recipients

        # Save the DataFrame as an Excel file with a dynamic name
        excel_filename = f"data_delivery_{client_name}_{release}.xlsx"
        excel_filepath = os.path.join(save_directory, excel_filename)  # Adjust path if needed
        df.to_excel(excel_filepath, index=False)
        print(f"Saved Excel file: {excel_filepath}")

        # Attach the Excel file
        if os.path.exists(excel_filepath):  # Ensure file exists before attaching
            with open(excel_filepath, "rb") as attachment:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment.read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', f'attachment; filename={os.path.basename(excel_filepath)}')
                message.attach(part)
                print(f"Attachment {excel_filename} added successfully.")
        else:
            print(f"Warning: The file {excel_filename} does not exist.")

        # Attach the specific files based on delivery_products_list
        print('delivery_type_list:', delivery_type_list)
        for product in delivery_type_list:
            # Split the product based on '-'
            product_parts = product.split('-')
            print(product_parts)
            for part in product_parts:
                attachment_key = f"{part}_DD"  # Create the key by appending '_DD' to the product part
                attachment_file = file_paths.get(attachment_key)  # Fetch the file path using the key
                print('attachment_file:', attachment_file)
                if attachment_file and os.path.exists(attachment_file):
                    filename = os.path.basename(attachment_file)
                    print(f"Preparing to attach: {filename}")
                    with open(attachment_file, "rb") as attachment:
                        part = MIMEBase('application', 'octet-stream')
                        part.set_payload(attachment.read())
                        encoders.encode_base64(part)
                        part.add_header('Content-Disposition', f'attachment; filename={filename}')
                        message.attach(part)
                        print(f"Attachment {filename} added successfully.")
                else:
                    print(f"Warning: The file {attachment_file} does not exist or is None.")

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
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Send delivery emails to clients")
    parser.add_argument('client', type=str, help="Comma-separated list of client names or 'ALL' to send to all clients")
    args = parser.parse_args()

    # Get the client names (comma-separated string or 'ALL')
    client_name_input = args.client

    # If 'ALL' is passed, iterate through all clients in the config file
    if client_name_input.upper() == 'ALL':
        client_name_list = con.client_name_list  # Get list of clients from the config file
        print("Sending emails to all clients...")
        for client_name in client_name_list:
            # Your existing logic for sending emails to each client
            rcv_maillist = get_emails_by_client(client_name.upper())
            print(rcv_maillist)
            df = pd.read_csv(con.csv_file_path + con.delivery_count_file)
            del_cnt_df = df[df['customer'].str.upper() == client_name.upper()]
            print(f"Processing client: {client_name} with {len(del_cnt_df)} deliveries")
            # Add a new 'iteration' column with the specified iteration value
            # del_cnt_df['iteration'] == 1
            del_cnt_df = del_cnt_df[del_cnt_df['iteration'] == 1]
            print(f"Processing client for iteration: {client_name} with {len(del_cnt_df)} deliveries")

            # Check if the DataFrame is empty
            if del_cnt_df.empty:
                print("No data found")
                sys.exit()  # Exit the code if no data is found
            else:
                # Continue with the rest of the code
                print("Data found, continuing with the process")
            # Load other client-specific data
            min_max_date_df = pd.read_csv(con.csv_file_path + 'max_min_counts.csv')
            client_details_df = pd.read_csv(con.csv_file_path + 'client-delivery-details.csv')
            filtered_client_details_df = client_details_df[client_details_df['client'] == client_name.upper()]

            delivery_products_list = del_cnt_df['Product'].unique().tolist()
            delivery_type_list = del_cnt_df['delivery_type'].unique().tolist()

            # Get CS Lead info and delivery method for the current client
            cs_lead_name = filtered_client_details_df['cs_lead_name'].iloc[0]
            cs_lead_email = filtered_client_details_df['cs_lead_email'].iloc[0]
            delivery_method = filtered_client_details_df['deliverymethod'].iloc[0]

            s3_location = filtered_client_details_df['s3location'].iloc[0] if not filtered_client_details_df[
                's3location'].empty else 'NOT APPLICABLE'
            file_password = filtered_client_details_df['password'].iloc[0] if not pd.isna(
                filtered_client_details_df['password'].iloc[0]) else 'NOT APPLICABLE'

            # Pass attachment_files from config to the send_email function
            attachment_files = con.attachment_files  # Load attachment files from config
            print(f"Attachment files for {client_name}: {attachment_files}")
            print(client_name)
            # Get the list of email addresses for the current client
            # rcv_maillist = get_emails_by_client(client_name)
            # print (rcv_maillist)

            # Iterate through the client's email list and send emails
            for client_email in rcv_maillist:
                send_email(con.smtp_server, con.port, con.sender_email, client_email, f"Delivery Counts: {client_name}",
                           con.user_name, con.password, del_cnt_df, client_name, delivery_products_list,
                           delivery_type_list, con.release,
                           cs_lead_name, cs_lead_email, delivery_method, min_max_date_df, s3_location, attachment_files,product_paths_file)

    else:
        # If a comma-separated list of client names is passed, split and iterate through them
        client_names = [name.strip().upper() for name in client_name_input.split(',')]  # Convert to uppercase and split
        print(f"Sending emails to specific clients: {client_names}")

        for client_name_input in client_names:
            rcv_maillist = get_emails_by_client(client_name)
            print(rcv_maillist)
            print(f"Processing client: {client_name_input}...")

            # Your logic to process and send the email to the specific client (similar to above)
            df = pd.read_csv(con.csv_file_path + con.delivery_count_file)
            del_cnt_df = df[df['customer'].str.upper() == client_name_input]
            print(f"Processing client: {client_name_input} with {len(del_cnt_df)} deliveries")

            # Load other client-specific data
            min_max_date_df = pd.read_csv(con.csv_file_path + 'max_min_counts.csv')
            client_details_df = pd.read_csv(con.csv_file_path + 'client-delivery-details.csv')
            filtered_client_details_df = client_details_df[client_details_df['client'] == client_name_input]

            delivery_products_list = del_cnt_df['product'].unique().tolist()
            delivery_type_list = del_cnt_df['delivery_type'].unique().tolist()

            # Get CS Lead info and delivery method for the current client
            cs_lead_name = filtered_client_details_df['cs_lead_name'].iloc[0]
            cs_lead_email = filtered_client_details_df['cs_lead_email'].iloc[0]
            delivery_method = filtered_client_details_df['deliverymethod'].iloc[0]

            s3_location = filtered_client_details_df['s3location'].iloc[0] if not filtered_client_details_df[
                's3location'].empty else 'NOT APPLICABLE'
            file_password = filtered_client_details_df['password'].iloc[0] if not pd.isna(
                filtered_client_details_df['password'].iloc[0]) else 'NOT APPLICABLE'

            # Pass attachment_files from config to the send_email function
            attachment_files = con.attachment_files  # Load attachment files from config
            print(f"Attachment files for {client_name_input}: {attachment_files}")

            # Get the list of email addresses for the current client
            rcv_maillist = get_emails_by_client(client_name_input)

            # Iterate through the client's email list and send emails
            for client_email in rcv_maillist:
                send_email(con.smtp_server, con.port, con.sender_email, client_email,
                           f"Delivery Counts: {client_name_input}",
                           con.user_name, con.password, del_cnt_df, client_name, delivery_products_list,
                           delivery_type_list, con.release,
                           cs_lead_name, cs_lead_email, delivery_method, min_max_date_df, s3_location, attachment_files,product_paths_file)
