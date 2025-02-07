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
import re
from datetime import datetime

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

        environment = Environment(loader=FileSystemLoader("C:\\Users\\AbhijeetPalo\\PycharmProjects\\pythonProject\\venv\\DeliveryCount-Email-Generator\\email_templates"))
        trademark_map = {
            'RWD360': 'RWD360® and RWD360®+OpenClaims+SDOH',
            'Patient360': 'Patient360™',
            'Genome360': 'Genome360™'
        }
        updated_delivery_products_list = [
            trademark_map.get(product, product) for product in delivery_products_list
        ]
        template = environment.get_template("body_default.txt")
        body = template.render(
            client_name=client_name,
            release=release,
            delivery_products_list=updated_delivery_products_list,
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

    email_text = body
    #for product in delivery_products_list:
    for product in delivery_type_list:
        date_list = []
        product_df = client_df[client_df['delivery_type'].str.upper() == product.upper()]
        trim_prodcut=product.replace('-Claims', '')
        print("generating max min dates")
        #product = re.sub(r'[^a-zA-Z0-9\s]', '', product)
        # Directly filter min_max_date_df based on the matching product and client_name
        filtered_df = min_max_date_df[
            (min_max_date_df['Product'].str.upper() == trim_prodcut.upper()) &
            (min_max_date_df['customer'].str.upper() == client_name.upper())
            ]

        # Extract the min and max dates if data exists

        if not filtered_df.empty:
            # Get the minimum and maximum date values from 'Minimum Date' and 'Maximum Date' columns
            min_date = filtered_df['Minimum Date'].min()
            max_date = filtered_df['Maximum Date'].max()

            # Check if the dates are already in datetime format, if not, convert them to datetime
            if isinstance(min_date, str):
                min_date = datetime.strptime(min_date, "%Y-%m-%d")
            if isinstance(max_date, str):
                max_date = datetime.strptime(max_date, "%Y-%m-%d")

            # Format the min and max dates
            min_date_formatted = min_date.strftime('%Y-%m-%d') if pd.notna(min_date) else "N/A"
            max_date_formatted = max_date.strftime('%Y-%m-%d') if pd.notna(max_date) else "N/A"

            # Format the date information for this product
            min_date_text = f"Minimum Date Contained in {trim_prodcut} data: {min_date_formatted}"
            max_date_text = f"Maximum Date Contained in {trim_prodcut} data: {max_date_formatted}"

            # Add date information to the date_list
            date_list.append(min_date_text)
            date_list.append(max_date_text)
        else:
            # Handle case where no data was found for the product and client combination
            date_list.append(f"No date information found for product: {trim_prodcut} and client: {client_name}")

        # After processing the product, you can handle or print the results
        if date_list:
            print(f"Date information for {product}:")
            for date_info in date_list:
                print(date_info)
        # If the product DataFrame is not empty, add the product counts and the min/max date information
        if not product_df.empty:
            # Filter out None or invalid dates from the date_list
            valid_dates = [date for date in date_list if date is not None]

            # Check if there are any valid dates
            if valid_dates:
                # Extract the minimum and maximum dates from valid dates
                min_date = min(valid_dates)
                max_date = max(valid_dates)

            # Convert the product counts DataFrame to an HTML table and add it to the email text
            updated_product = product.replace('-', ' ').replace('Claims', '')
            product_html = convert_table_to_html(product_df)
            email_text += f'<h3>{updated_product} Counts:</h3><span>Date coverage:</span><br>{max_date}<br>{min_date}<br><br>{product_html}'

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
               min_max_date_df,s3_location,product_paths_file):
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
        new_df = df[['Product', 'Format', 'Addon', 'Indication', 'Subscription Category', 'Subscription Subcategory',
                     'Patient Count']]
        new_df['Patient Count'] = pd.to_numeric(new_df['Patient Count'], errors='coerce')
        new_df.to_excel(excel_filepath, index=False)
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

        attached_files = set()

        for product in delivery_type_list:
            # Split the product based on '-'
            product_parts = product.split('-')
            print(product_parts)

            for part in product_parts:
                attachment_key = f"{part}_DD"  # Create the key by appending '_DD' to the product part
                attachment_file = file_paths.get(attachment_key)  # Fetch the file path using the key
                print('attachment_file:', attachment_file)

                # Ensure the file path is valid and exists
                if attachment_file:
                    filename = os.path.basename(attachment_file)  # Get the file name from the path

                    if os.path.exists(attachment_file):
                        # Check if the file has already been attached to avoid duplicates
                        if filename not in attached_files:
                            print(f"Preparing to attach: {filename}")

                            try:
                                # Open the file and attach it
                                with open(attachment_file, "rb") as attachment:
                                    part = MIMEBase('application', 'octet-stream')
                                    part.set_payload(attachment.read())
                                    encoders.encode_base64(part)
                                    part.add_header('Content-Disposition', f'attachment; filename={filename}')
                                    message.attach(part)
                                    print(f"Attachment {filename} added successfully.")

                                # Add the filename to the set to avoid future duplicates
                                attached_files.add(filename)
                            except Exception as e:
                                print(f"Error attaching file {filename}: {e}")
                        else:
                            print(f"Attachment {filename} has already been added, skipping.")
                    else:
                        print(f"Warning: The file {attachment_file} does not exist.")
                else:
                    print(f"Warning: The file path for {part} is None.")



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
            del_cnt_df = del_cnt_df[del_cnt_df['iteration'] == con.iteration]
            print(f"Processing client for iteration: {client_name} with {len(del_cnt_df)} deliveries")

            # Check if the DataFrame is empty
            if del_cnt_df.empty:
                print("No data found")
                sys.exit()  # Exit the code if no data is found
            else:
                # Continue with the rest of the code
                print("Data found, continuing with the process")
            # Load other client-specific data
            min_max_date_df = pd.read_csv(con.csv_file_path + con.min_max_counts)
            min_max_date_df=min_max_date_df[min_max_date_df['iteration'] == con.iteration]
            client_details_df = pd.read_csv(con.csv_file_path + con.client_delivery_details)
            print(client_details_df)
            #filtered_client_details_df = client_details_df[client_details_df['client'] == client_name.upper()]
            client_details_df['iteration'] = client_details_df['iteration'].astype(int)
            # filtered_client_details_df = client_details_df[
            #     (client_details_df['client'].str.upper() == client_name_input) & (client_details_df['iteration'] == con.iteration)
            #     ]
            filtered_client_details_df = client_details_df[ client_details_df['iteration'] == con.iteration ]
            print(filtered_client_details_df)
            delivery_products_list = del_cnt_df['Product'].unique().tolist()
            #delivery_products_list = [re.sub(r'[^\x00-\x7F]+', '', Product) for Product in delivery_products_list]
            delivery_type_list = del_cnt_df['delivery_type'].unique().tolist()
            cs_lead_email = 'None'
            delivery_method='None'
            s3_location ='None'
            file_password='None'
            # Get CS Lead info and delivery method for the current client
            cs_lead_name = filtered_client_details_df['cs_lead_name'].iloc[0]
            delivery_method = filtered_client_details_df['deliverymethod'].iloc[0]

            s3_location = filtered_client_details_df['s3location'].iloc[0] if not filtered_client_details_df[
                's3location'].empty else 'NOT APPLICABLE'
            file_password = filtered_client_details_df['password'].iloc[0] if not pd.isna(
                filtered_client_details_df['password'].iloc[0]) else 'NOT APPLICABLE'

            print(client_name)

            # Iterate through the client's email list and send emails
            #for client_email in rcv_maillist:
            send_email(con.smtp_server, con.port, con.sender_email, rcv_maillist, f"Delivery Counts: {client_name}",
                           con.user_name, con.password, del_cnt_df, client_name, delivery_products_list,
                           delivery_type_list, con.release,
                           cs_lead_name, cs_lead_email, delivery_method, min_max_date_df, s3_location,product_paths_file)

    else:
        # If a comma-separated list of client names is passed, split and iterate through them
        client_names = [name.strip().upper() for name in client_name_input.split(',')]  # Convert to uppercase and split
        print(f"Sending emails to specific clients: {client_names}")

        for client_name in client_names:
            rcv_maillist = get_emails_by_client(client_name.upper())
            print(rcv_maillist)
            print(f"Processing client: {client_name}...")

            # Your logic to process and send the email to the specific client (similar to above)
            df = pd.read_csv(con.csv_file_path + con.delivery_count_file)
            del_cnt_df = df[df['customer'].str.upper() == client_name]
            del_cnt_df = del_cnt_df[del_cnt_df['iteration'] == con.iteration]
            print(f"Processing client: {client_name} with {len(del_cnt_df)} deliveries")

            # Load other client-specific data
            min_max_date_df = pd.read_csv(con.csv_file_path + con.min_max_counts)
            min_max_date_df = min_max_date_df[min_max_date_df['iteration'] == con.iteration]
            client_details_df = pd.read_csv(con.csv_file_path + con.client_delivery_details)
            print(client_details_df)
            # filtered_client_details_df = client_details_df[client_details_df['client'] == client_name.upper()]
            client_details_df['iteration'] = client_details_df['iteration'].astype(int)
            # filtered_client_details_df = client_details_df[
            #     (client_details_df['client'].str.upper() == client_name_input) & (client_details_df['iteration'] == con.iteration)
            #     ]
            filtered_client_details_df = client_details_df[client_details_df['iteration'] == con.iteration]
            print(filtered_client_details_df)
            delivery_products_list = del_cnt_df['Product'].unique().tolist()
            # delivery_products_list = [re.sub(r'[^\x00-\x7F]+', '', Product) for Product in delivery_products_list]
            delivery_type_list = del_cnt_df['delivery_type'].unique().tolist()
            cs_lead_email = 'None'
            delivery_method='None'
            s3_location ='None'
            file_password='None'
            # Get CS Lead info and delivery method for the current client
            cs_lead_name = filtered_client_details_df['cs_lead_name'].iloc[0]
            delivery_method = filtered_client_details_df['deliverymethod'].iloc[0]

            s3_location = filtered_client_details_df['s3location'].iloc[0] if not filtered_client_details_df[
                's3location'].empty else 'NOT APPLICABLE'
            file_password = filtered_client_details_df['password'].iloc[0] if not pd.isna(
                filtered_client_details_df['password'].iloc[0]) else 'NOT APPLICABLE'

            print(client_name)

            # Iterate through the client's email list and send emails
            # for client_email in rcv_maillist:
            send_email(con.smtp_server, con.port, con.sender_email, rcv_maillist, f"Delivery Counts: {client_name}",
                       con.user_name, con.password, del_cnt_df, client_name, delivery_products_list,
                       delivery_type_list, con.release,
                       cs_lead_name, cs_lead_email, delivery_method, min_max_date_df, s3_location, product_paths_file)
