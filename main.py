from utils import Config,redshift_conn,create_schema_list,format_sql_script,combine_sql_query,get_emails_by_client,format_rwd360_claims_script,apply_row_filter,format_patient_count
import pandas as pd
import os, sys
import re
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import ssl
import pandas as pd
import configparser
from datetime import datetime
from jinja2 import Environment, FileSystemLoader, TemplateNotFound
from sqlalchemy import create_engine

db_control_dict = {'dbname': 'concerto_dev',
                   'host': 'concerto.cfaw7z5r1wcw.us-east-1.redshift.amazonaws.com',
                   'user': 'apalo@concertohealthai.com',
                   'port': '5439',
                   'password': 'Abhi@@##270220'}

config = Config('config.ini')
iteration=config.iteration
client_list=config.clients
input_delivery_df = pd.read_csv(config.input_file_path)
release=config.release
# email parameters
smtp_server=config.smtp_server
port=config.port
sender_email=config.sender_email
user_name =config.user_name
password=config.password

#dd_attachment_filepath=config.dd_file_attachments # File path to fetch datadictionary file
delivery_count_csv_path=config.delivery_count_attachment_path # Path to save the delivery count file attchemnets
dd_filepath_details=config.dd_filepath_details

# Redshift conn parameters
db_user =config.db_user
db_pass=config.db_password
db_host=config.db_host
db_port=config.db_port
db_name=config.db_name

# RWD360 + Cliams SQL template

rwd360_ehr_sql_template= config.rwd360_ehr_sql_template
rwd360_ehr_claims_sql_template= config.rwd360_ehr_claims_sql_template
rwd360_ehr_mo_sql_template = config.rwd360_ehr_mo_sql_template
rwd360_ehr_label_sql_template=config.rwd360_ehr_label_sql_template

#engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}')
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

    # Apply formatting using Styler to format 'patient_count' with commas and styling the entire DataFrame
    styled_df = client_df.style.format({'patient_count': '{:,.0f}'})  # Format 'patient_count' with commas

    # Apply general table styles
    styled_df = styled_df.set_properties(**{'font-size': '9pt', 'font-family': 'Calibri',
                                            'text-align': 'left', 'min-width': '120px'}) \
        .set_table_styles([
        {'selector': 'th',
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
    ]) \
        .hide(axis='index')  # Hide the index in the HTML output

    # Convert the styled DataFrame to HTML
    table_html = styled_df.to_html()  # Use render() to get the HTML representation

    return table_html

def load_product_paths(filename):
    product_paths = {}
    absolute_path = os.path.abspath('email_templates/dd_attachments.txt')
    print(f"Absolute Path: {absolute_path}")
    try:
        file_path = 'email_templates/{}'.format(filename)
        with open(file_path, 'r') as f :
            for line in f:
                line = line.strip()
                if '=' in line:
                    key, value = line.split("=", 1)
                    product_paths[key.strip()] = value.strip()
    except Exception as e:
        print(f"Error loading product paths: {e}")
    print('product_path:', product_paths)
    return product_paths

def get_client_delivery_details(filename,iteration,client_name):
    client_details_df = pd.read_csv(f'delivery_counts_scripts/{filename}')
    print(client_details_df)
    client_details_df['iteration'] = client_details_df['iteration'].astype(int)
    filtered_client_details_df = client_details_df[
         (client_details_df['client'].str.strip().str.upper() == client_name.strip().upper()) &
    (client_details_df['iteration'] == int(iteration))]
    cs_lead_email = 'None'
    delivery_method = 'None'
    s3_location = 'None'
    file_password = 'None'
    # Get CS Lead info and delivery method for the current client
    print(filtered_client_details_df)
    cs_lead_name = filtered_client_details_df['cs_lead_name'].iloc[0]
    delivery_method = filtered_client_details_df['deliverymethod'].iloc[0]

    s3_location = filtered_client_details_df['s3location'].iloc[0] if not filtered_client_details_df[
        's3location'].empty else 'NOT APPLICABLE'
    file_password = filtered_client_details_df['password'].iloc[0] if not pd.isna(
        filtered_client_details_df['password'].iloc[0]) else 'NOT APPLICABLE'
    print(f'FilePassword: {file_password}')
    return(delivery_method,s3_location,file_password,cs_lead_name)

def generate_client_email_text(client_df, client_name, release, delivery_products_list, delivery_type_list,
                               cs_lead_name,
                               delivery_method, min_max_date_df, s3_location,file_password):
    '''Generates the HTML file for a client email.'''

    unique_products = set(val for item in delivery_type_list for val in item.split('-'))

    #unique_products = set(item for item in delivery_products_list )
    print('unique:',unique_products)
    try:

        environment = Environment(loader=FileSystemLoader("email_templates"))
        trademark_map = {
            'RWD360': 'RWD360®',
            'Patient360': 'Patient360™',
            'Genome360': 'Genome360™',
            'Claims':'OpenClaims+SDOH',
            'RWD360-Analytical Layer':'RWD360-Analytical Layer',
            'PT360-Analytical Layer':'Patient360-Analytical Layer'
        }
        # Define explicit order:
        product_order = {
            'RWD360': 0,
            'Patient360': 1,
            'Genome360': 2,
            'Claims': 3,
            'RWD360-Analytical Layer': 4,
            'PT360-Analytical Layer': 5
        }
        # Sort unique_products by the index in product_order:
        unique_products_sorted = sorted(unique_products, key=lambda x: product_order.get(x, 999))

        updated_delivery_products_list = [trademark_map.get(p, p) for p in unique_products_sorted]
        # updated_delivery_products_list = [
        #     trademark_map.get(product, product) for product in unique_products]

        # updated_delivery_products_list = []
        # for product in unique_products:
        #     updated_delivery_products_list.append(trademark_map.get(product, product))


        print('updated_delivery_products_list:',updated_delivery_products_list)
        template = environment.get_template("body_default.txt")
        body = template.render(
            client_name=client_name,
            release=release,
            delivery_products_list=updated_delivery_products_list,
            cs_lead_name=cs_lead_name,
            delivery_method=delivery_method,
            location=s3_location,
            file_password=file_password
        )
    except TemplateNotFound as e:
        print(f"Template not found: {e}")
    except Exception as e:
        print(f"An error occurred while loading the template: {e}")
    email_text = body
    # Remove delivery type items with Claims
    #delivery_type_list = [item for item in delivery_type_list if 'Claims' not in item]
    # delivery_type_list = [item for item in delivery_type_list
    #                       if '-Claims' not in item] + [item + '-Claims' for item in delivery_type_list if 'Claims' not in item]

    # Appended claims to not split the table count  in to NonCliams and CLaims
    #claims_delivery_type_list = list(set([item + '-Claims' for item in delivery_type_list if 'Claims' not in item]))
    #claims_delivery_type_list = list(set([item.replace('-Claims', '') for item in delivery_type_list]))

    #delivery_unique_products = client_df.loc['product'].unique()
    print("unique delivery_unique_products{}".format(delivery_type_list))
    #for product in claims_delivery_type_list:
    for product in delivery_type_list:
        date_list = []
        #print(client_df.loc['Product'])
        product_df = client_df[client_df['delivery_type'].str.upper() == product.upper()]
        trim_prodcut=product.replace('-Claims', '')
        #trim_prodcut=product.split('-')[0]
        print(trim_prodcut)
        print("generating max min dates")
        #product = re.sub(r'[^a-zA-Z0-9\s]', '', product)
        # Directly filter min_max_date_df based on the matching product and client_name
        filtered_df = min_max_date_df[
            (min_max_date_df['Product'].str.upper() == trim_prodcut.upper()) &
            (min_max_date_df['Client'].str.upper() == client_name.upper())
            ]

        # Extract the min and max dates if data exists

        if not filtered_df.empty:
            # Get the minimum and maximum date values from 'Minimum Date' and 'Maximum Date' columns
            min_date = filtered_df['min_date'].min()
            max_date = filtered_df['max_date'].max()

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

            # Join the coverage information into a single string
            #date_coverage_text = "<br>".join(date_coverage)
            # Convert the product counts DataFrame to an HTML table and add it to the email text
            updated_product = product.replace('-', ' ').replace('Claims', '')
            updated_product = product.replace('-', ' ').replace('Claims', '')
            print(product_df)
            product_html = convert_table_to_html(product_df)
            min_txt ='(not including 1901-01-01 date of birth values set as part of de-identification of patients over 80 years of age)'
            email_text += f'<h3>{updated_product} Counts:</h3><span>Date coverage:</span><br>{max_date}{min_txt}<br>{min_date}<br><br>{product_html}'

    # Generate the signature
    template = environment.get_template("signature_default.txt")
    signature = template.render(
        client_name=client_name,
        release=release,
        cs_lead_name=cs_lead_name,
        client_product_min_max_dates=''
    )

    # Add the signature at the end of the email
    print("email_test-")
    email_text += signature
    return email_text


def send_email(smtp_server, port, sender_email, rcv_maillist, user_name, password, df, client_name,
               delivery_products_list, delivery_type_list, release, cs_lead_name, delivery_method,
               min_max_date_df,s3_location,product_paths_file,file_password):
    try:
        # Generate the email content
        # Create the directory path for saving the Excel file
        release_label = f"{str(release)}_v{iteration}"
        file_paths = load_product_paths(product_paths_file)
        save_directory = os.path.join(delivery_count_csv_path, client_name, release_label)
        # Ensure the directory exists
        os.makedirs(save_directory, exist_ok=True)
        body = generate_client_email_text(df, client_name, release, delivery_products_list, delivery_type_list,
                                          cs_lead_name,
                                          delivery_method, min_max_date_df, s3_location,file_password)
        subject = "Delivery Counts:" + client_name + ", " + str(release)
        # Create the email message
        message = MIMEMultipart()
        message['From'] = sender_email
        message['Subject'] = subject

        # Attach the generated HTML body
        message.attach(MIMEText(body, 'html'))  # 'html' for HTML content
        message['To'] = ", ".join(rcv_maillist)
        #message['Cc'] = ", ".join(cc_emails)  # Adding CC recipients

        # Save the DataFrame as an Excel file with a dynamic name
        excel_filename = f"data_delivery_{client_name}_{release}.xlsx"
        excel_filepath = os.path.join(save_directory, excel_filename)  # Adjust path if needed
        new_df = df[['Product', 'Format', 'Addon', 'Indication', 'Subscription Category', 'Subscription Subcategory',
                     'Patient Count']]
        #new_df['Patient Count'] = pd.to_numeric(new_df['Patient Count'])
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
        print("error:",e)

# Sub-function to handle RWD360 data processing
def process_rwd360_counts(product, iteration, client, rs_conn):
    schema_list = create_schema_list(product['df'])
    # Retrieve schemas based on the presence of claims
    #print(product['df'])
    rwd360_delivery_schema = next(iter(product['df'][~product['df']['SCHEMA NAME'].str.contains('open', case=False, na=False)]['SCHEMA NAME']), 'NA')
    rwd360_claims_delivery_schema = next(iter(product['df'][product['df']['SCHEMA NAME'].str.contains('open', case=False, na=False)]['SCHEMA NAME']), 'NA')
    rwd360_label_delivery_schema = rwd360_delivery_schema

    if rwd360_claims_delivery_schema != 'NA':
        # Prepare SQL queries for RWD360 with claims
        rwd360_formatted_sql = format_rwd360_claims_script(rwd360_ehr_sql_template, rwd360_delivery_schema,rwd360_claims_delivery_schema,iteration, client,config.patient_metadata_schema)
        rwd360_claims_formatted_sql = format_rwd360_claims_script(rwd360_ehr_claims_sql_template,rwd360_delivery_schema,rwd360_claims_delivery_schema, iteration, client,config.patient_metadata_schema)
        rwd360_mo_formatted_sql = format_rwd360_claims_script(rwd360_ehr_mo_sql_template, rwd360_delivery_schema,rwd360_claims_delivery_schema,iteration, client,'NA')
        rwd360_label_formatted_sql = format_rwd360_claims_script(rwd360_ehr_label_sql_template, rwd360_delivery_schema,rwd360_claims_delivery_schema, iteration, client,'NA')

        #print('ehr sql script--',rwd360_formatted_sql)


        #print(rwd360_mo_formatted_sql)
        rwd360_mo_sql =pd.read_sql(rwd360_mo_formatted_sql, rs_conn)

        combine_rwd360_mo_sql = combine_sql_query(rwd360_mo_sql)

        rwd360_df = pd.read_sql(rwd360_formatted_sql, rs_conn)
        rwd360_df.to_csv('delivery_counts_scripts/rwd360_df.csv', index=False)
        rwd360_claims_df = pd.read_sql(rwd360_claims_formatted_sql, rs_conn)
        rwd360_mo_df = pd.read_sql(combine_rwd360_mo_sql, rs_conn)
        rwd360_label_df = pd.read_sql(rwd360_label_formatted_sql, rs_conn)
        # print(rwd360_df)
        # print(rwd360_mo_df)
        # print(pd.concat([rwd360_df, rwd360_claims_df, rwd360_mo_df, rwd360_label_df], ignore_index=True))
        # Concatenate all RWD360 related dataframes
        print(rwd360_claims_formatted_sql)
        rwd360_claims_df.to_csv('delivery_counts_scripts/rwd360_claims_df.csv', index=False)

        return pd.concat([rwd360_df, rwd360_claims_df, rwd360_mo_df, rwd360_label_df], ignore_index=True)

    else:
        # Prepare SQL queries for RWD360 without claims
        rwd360_formatted_sql = format_rwd360_claims_script(rwd360_ehr_sql_template, rwd360_delivery_schema, iteration, client,config.patient_metadata_schema)
        rwd360_label_formatted_sql = format_rwd360_claims_script(rwd360_ehr_label_sql_template, rwd360_label_delivery_schema, iteration, client,'NA')

        rwd360_mo_formatted_sql = format_rwd360_claims_script(rwd360_ehr_mo_sql_template, rwd360_delivery_schema, iteration, client,'NA')
        rwd360_mo_sql = pd.read_sql(rwd360_mo_formatted_sql, rs_conn)
        combine_rwd360_mo_sql = combine_sql_query(rwd360_mo_sql)

        rwd360_df = pd.read_sql(rwd360_formatted_sql, rs_conn)
        rwd360_mo_df = pd.read_sql(combine_rwd360_mo_sql, rs_conn)
        rwd360_label_df = pd.read_sql(rwd360_label_formatted_sql, rs_conn)

        #print(pd.concat([rwd360_df, rwd360_mo_df, rwd360_label_df], ignore_index=True))


        # Concatenate RWD360 related dataframes without claims
        return pd.concat([rwd360_df, rwd360_mo_df, rwd360_label_df], ignore_index=True)

def process_min_max_date(product,schema_list, iteration, client, rs_conn, min_max_date_sql_file_name,product_min_max):
    date_formatted_sql = format_sql_script(min_max_date_sql_file_name, schema_list, iteration, client)
    date_df = pd.read_sql(date_formatted_sql, rs_conn)
    min_max_date_query = combine_sql_query(date_df)
    print(min_max_date_query)
    min_max_date_df = pd.read_sql(min_max_date_query, rs_conn)
    print(product_min_max)
    min_max_date_df['Product'] = product_min_max
    min_max_date_df['Client'] = client
    print('min_max_date_df:',schema_list,':',min_max_date_df)
    return min_max_date_df
def generate_client_delivery_counts(customer_filtered_df,iteration,client,config):
    # filter data for each product PT360,PR360,TR360,RWD360,GN360
    ## Patient360
    print('customer_filtered_df:',customer_filtered_df)
    pt360_df = customer_filtered_df[customer_filtered_df['PRODUCT'].str.contains('PT360', na=False) & ~customer_filtered_df['PRODUCT'].str.contains('Analytical', na=False)& ~customer_filtered_df['PRODUCT'].str.contains('REGISTRY', na=False)]
    # RWD360
    rwd360_df = customer_filtered_df[customer_filtered_df['PRODUCT'].str.contains('RWD360', na=False) & ~customer_filtered_df['PRODUCT'].str.contains('Analytical', na=False)]
    print(rwd360_df)
    # Precision 360
    pr360_df = customer_filtered_df[customer_filtered_df['PRODUCT'].str.contains('PR360', na=False)]
    # Transactional 360
    tr360_df = customer_filtered_df[customer_filtered_df['PRODUCT'].str.contains('TR360', na=False)]
    # Genome 360
    gn360_df = customer_filtered_df[customer_filtered_df['PRODUCT'].str.contains('GN360', na=False)]
    # Custom Cohort
    pt360_fast_registry = customer_filtered_df[
        customer_filtered_df['PRODUCT'].str.contains('PT360', na=False) & customer_filtered_df['PRODUCT'].str.contains(
            'REGISTRY', na=False) & ~customer_filtered_df['PRODUCT'].str.contains('Analytical', na=False)]
    print('pt360_fast_registry',pt360_fast_registry)
    pt360_analytical_layer_df=customer_filtered_df[customer_filtered_df['PRODUCT'].str.contains('PT360', na=False) & customer_filtered_df['PRODUCT'].str.contains('Analytical', na=False)& ~customer_filtered_df['PRODUCT'].str.contains('REGISTRY', na=False)]
    rwd360_analytical_layer_df = customer_filtered_df[customer_filtered_df['PRODUCT'].str.contains('RWD360', na=False) & customer_filtered_df['PRODUCT'].str.contains('Analytical', na=False)]
    registry_analytical_layer_df=customer_filtered_df[customer_filtered_df['PRODUCT'].str.contains('Analytical', na=False)& customer_filtered_df['PRODUCT'].str.contains('REGISTRY', na=False)]
    custom_df = customer_filtered_df[customer_filtered_df['PRODUCT'].str.contains('CUSTOM', na=False)& ~customer_filtered_df['PRODUCT'].str.contains('PT360', na=False)]

    product_configs = [
        {
            'name': 'Patient360',
            'df': pt360_df,
            'sql_template': config.pt360_sql_template
         },
        {
            'name': 'PT360Analytical Layer',
            'df': pt360_analytical_layer_df,
            'sql_template': config.al_registry_sql_template
        },
         {
            'name': 'TR360',
            'df': tr360_df,
            'sql_template': config.tr360_sql_template
         },
        {
            'name': 'PT360FastRegistry',
            'df': pt360_fast_registry,
            'sql_template': config.al_registry_sql_template
        },
        {
            'name': 'RegistryAL',
            'df': registry_analytical_layer_df,
            'sql_template': config.al_registry_sql_template
        },

        {
            'name': 'RWD360',
            'df': rwd360_df,
            'sql_template': 'NA'
        },

        {
            'name': 'RWD360Analytical Layer',
            'df': rwd360_analytical_layer_df,
            'sql_template': config.al_registry_sql_template
        },
        {
            'name': 'Precision360',
            'df': pr360_df,
            'sql_template': config.pr360_sql_template
        }
        ,
        {
            'name': 'Genome360',
            'df': gn360_df,
            'sql_template': config.gn360_sql_template
        },

        {
            'name': 'custom_df',
            'df': custom_df,
            'sql_template': config.al_registry_sql_template
        }
    ]

    try:
        rs_conn = redshift_conn(db_control_dict)
        #cur = rs_conn.cursor()
        min_max_date_sql_file_name =config.min_max_date_sql_template

        #all_count_dfs = []
        #all_min_max_date_dfs = []
        all_count_dfs = pd.DataFrame()
        all_min_max_date_dfs = pd.DataFrame()

        for product in product_configs:
            print (product['name'])
            delivery_type =''
            # print(product['df'])
            if not product['df'].empty and product['name']!='RWD360':
                #print('product_df-',product['df'])
                schema_list = create_schema_list(product['df'])
                formatted_sql = format_sql_script(product['sql_template'], schema_list, iteration, client)
                df = pd.read_sql(formatted_sql, rs_conn)
                count_query = combine_sql_query(df)
                delivery_count_df = pd.read_sql(count_query, rs_conn)
                #all_count_dfs.append(count_df)
                #product_min_max = delivery_count_df['delivery_type'].str.replace('-Claims', '', regex=False)

            elif not product['df'].empty and product['name']=='RWD360':
                # print('Process RWD360 data')
                # print(product)
                delivery_count_df = process_rwd360_counts(product, iteration, client, rs_conn)
                delivery_count_df = apply_row_filter(delivery_count_df, client.lower())


            else: continue

            #all_count_dfs.append(delivery_count_df)

            distinct_delivery_types = delivery_count_df['delivery_type'] \
                .str.replace('-Claims', '', regex=False) \
                .dropna().unique().tolist()
            product_min_max = ', '.join(distinct_delivery_types)
            print('distinct_delivery_types',product_min_max)
            all_count_dfs = pd.concat([all_count_dfs, delivery_count_df], ignore_index=True)
            #print('all_count_dfs_columns',all_count_dfs.columns)


    ## Generate min/max date SQL and query
            schema_list = create_schema_list(product['df'])
            #extract delivery_type and pass as product in min_max_df

            #product_min_max = all_count_dfs['delivery_type'].str.replace('-Claims', '', regex=False)
            #print('schemalist:', schema_list, ':', product_min_max)
            min_max_date_df=process_min_max_date(product,schema_list, iteration, client, rs_conn, min_max_date_sql_file_name,product_min_max)

            print(min_max_date_df)
            all_min_max_date_dfs = pd.concat([all_min_max_date_dfs, min_max_date_df], ignore_index=True)
            #all_min_max_date_dfs.append(min_max_date_df)

        # Combine all data
        final_delivery_counts = all_count_dfs
        final_max_min_date = all_min_max_date_dfs
        # final_delivery_counts = pd.concat(all_count_dfs, ignore_index=True)
        # final_max_min_date = pd.concat(all_min_max_date_dfs, ignore_index=True)

        final_delivery_counts.to_csv('delivery_counts_scripts/final_delivery_counts.csv', index=False)
        #final_delivery_counts['patient_count'] = final_delivery_counts['patient_count'].astype(int)
        return(final_delivery_counts, final_max_min_date)

    finally:
        rs_conn.close()




def main():
    for client in client_list:
        customer_filtered_df=input_delivery_df[input_delivery_df['ITERATION'] == int(iteration)]
        customer_filtered_df= customer_filtered_df[customer_filtered_df['CLIENT']==client]

        delivery_count_df,min_max_date_df=generate_client_delivery_counts(customer_filtered_df, iteration, client, config)

        delivery_method,s3_location,file_password,cs_lead_name=get_client_delivery_details(config.client_delivery_details_file,iteration,client)

        rcv_maillist = get_emails_by_client(client.upper())
        print(rcv_maillist)
        print(delivery_count_df)
        delivery_products_list = delivery_count_df['product'].unique().tolist()
        # delivery_products_list = [re.sub(r'[^\x00-\x7F]+', '', Product) for Product in delivery_products_list]
        delivery_type_list = delivery_count_df['delivery_type'].unique().tolist()
        delivery_count_df.rename(columns={
            'iteration': 'Iteration',
            'product': 'Product',
            'customer': 'Customer',
            'format':'Format',
            'indication':'Indication',
            'addon':'Addon',
            'subscription_category': 'Subscription Category',
            'subscription_subcategory': 'Subscription Subcategory',
            'patient_count': 'Patient Count'
        }, inplace=True)
        # converting Patient count in 1000 format

        delivery_count_df['Patient Count'] = delivery_count_df['Patient Count'].apply(format_patient_count)

        delivery_count_df = delivery_count_df.sort_values(by=['Indication', 'Addon', 'Format', 'Product'])


        send_email(smtp_server, port, sender_email, rcv_maillist,
                   user_name, password, delivery_count_df, client, delivery_products_list,
                   delivery_type_list, release,
                   cs_lead_name, delivery_method, min_max_date_df, s3_location, dd_filepath_details, file_password)
if __name__ == '__main__':
    main()