import json
import psycopg2
import pandas as pd

import os
# import pg
import re
import os

import configparser
import sys

import json
import os
from multiprocessing import Pool
from datetime import datetime
from sqlalchemy import create_engine
import re
import logging
import logging.handlers


class Logger:
    def __init__(self, name):
        logger = logging.getLogger(name)
        if not logger.handlers:
            formatter = logging.Formatter('%(asctime)s %(levelname)s:%(name)s -> %(message)s')
            logger.setLevel(logging.DEBUG)
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        self.__logger = logger

    def get(self):
        return self.__logger


_logging = Logger("Utils").get()

class Config:

    def __init__(self, config_file: str):

        self.jira_api_token = None
        config = configparser.ConfigParser()
        self.config_file = config_file
        # if not is_local():
        #     config.read(find_file(os.getcwd(), self.config_file))
        # else:
        config.read('config.ini')

        # load configs
        # self.version = self.get_version(self.config)
        self.release = config['run_param']['release']
        self.iteration = config['run_param']['iteration']
        self.client = config['run_param']['client']
        self.clients = config['run_param']['client'].split(",")
        #self.db_control_dict = config['conn_param']['db_control_dict']
        self.input_file_path = config['run_param']['input_file_path']

        self.pt360_sql_template=config['sql_param']['pt360_sql_template']
        self.rwd360_ehr_sql_template = config['sql_param']['rwd360_ehr_sql_template']
        self.rwd360_ehr_claims_sql_template = config['sql_param']['rwd360_ehr_claims_sql_template']
        self.rwd360_ehr_label_sql_template=config['sql_param']['rwd360_ehr_label_sql_template']
        self.rwd360_ehr_mo_sql_template = config['sql_param']['rwd360_ehr_mo_sql_template']
        self.pr360_sql_template = config['sql_param']['pr360_sql_template']
        self.tr360_sql_template = config['sql_param']['tr360_sql_template']
        self.gn360_sql_template = config['sql_param']['gn360_sql_template']
        self.custom_sql_template = config['sql_param']['custom_sql_template']
        self.al_registry_sql_template = config['sql_param']['al_registry_sql_template']

        self.patient_metadata_schema=config['sql_param']['patient_metadata_schema']

        self.min_max_date_sql_template = config['sql_param']['min_max_date_sql_template']

        self.port=config['smtp_param']['port']
        self.smtp_server = config['smtp_param']['smtp_server']
        self.sender_email = config['smtp_param']['sender_email']
        self.user_name = config['smtp_param']['user_name']
        self.password = config['smtp_param']['password']

        self.delivery_count_attachment_path = config['client_delivery_details_param']['csv_file_path']
        self.client_delivery_details_file = config['client_delivery_details_param']['file_name']
        #self.dd_file_attachments = config['client_delivery_details_param']['dd_file_attachments']
        self.dd_filepath_details=config['client_delivery_details_param']['dd_filepath_details']

        self.db_port = config['conn_param']['port']
        self.db_host = config['conn_param']['host']
        self.db_name = config['conn_param']['dbname']
        self.db_user = config['conn_param']['user']
        self.db_password = config['conn_param']['password']


def redshift_conn(db_control_dict):
    """This function takes a dictionary of redshift credentials to output a connection: rs_conn"""
    return psycopg2.connect(**db_control_dict)
    print("connection established")

def create_schema_list(df):
    schema_list = df['SCHEMA NAME'].dropna().unique()  # Drop NaNs and get unique values
    schema_string = ','.join(f"'{s}'" for s in schema_list)
    return schema_string

def format_rwd360_claims_script(sql_file_name,rwd360_delivery_schema,rwd360_claims_delivery_schema,iteration,client,patient_metadata_schema):
    with open('delivery_counts_scripts/{}'.format(sql_file_name), 'r') as f:
        sql_body = f.read()
    sql_body_formatted = sql_body.replace('#iteration', iteration)
    sql_body_formatted = sql_body_formatted.replace('#customer', client)
    sql_body_formatted = sql_body_formatted.replace('#rwd360_delivery_schema',rwd360_delivery_schema )
    sql_body_formatted = sql_body_formatted.replace('#rwd360_claims_delivery_schema', rwd360_claims_delivery_schema)
    sql_body_formatted = sql_body_formatted.replace('#patient_metadata_schema', patient_metadata_schema)
    return(sql_body_formatted)

def format_sql_script(sql_file_name,schema_string,iteration,client):
    with open('delivery_counts_scripts/{}'.format(sql_file_name), 'r') as f:
        sql_body = f.read()
    sql_body_formatted = sql_body.replace('#iteration', iteration)
    sql_body_formatted = sql_body_formatted.replace('#customer', client)
    sql_body_formatted = sql_body_formatted.replace('#schema_list', schema_string)
    return(sql_body_formatted)

def combine_sql_query(df):
    sql_query = df['sql_code'].dropna().tolist()
    combined_sql = "\n".join(sql_query)
    combined_sql = re.sub(r'(?i)\bUNION(\s+ALL)?\b', '', combined_sql, count=1).strip()
    return combined_sql


def get_emails_from_file(file_path):
    client_emails = {}

    with open(file_path, 'r') as file:
        # Read all lines from the file
        lines = file.readlines()

        client_name = None
        for line in lines:
            # Remove leading/trailing whitespace
            line = line.strip()

            # Skip empty lines
            if not line:
                continue

            # If the line starts with a client name (it will contain a colon)
            if ':' in line:
                # Split the client name and emails by the colon
                client_name, emails_str = line.split(':', 1)
                client_name = client_name.strip().upper()
                # Clean up and split emails by commas
                emails = [email.strip() for email in emails_str.split(',')]
                client_emails[client_name] = emails

    return client_emails


# Function to get emails by client name
def get_emails_by_client(client_name):
    # Get all client emails from the file
    file_path =  r'email_templates\receiver_list.txt'
    emails_by_client = get_emails_from_file(file_path)
    print(emails_by_client)

    # Fetch emails for the specific client
    if client_name in emails_by_client:
        return emails_by_client[client_name]
    else:
        return None

# get_emails_by_client(client_name, file_path)

def apply_row_filter(df, customer_key):
    print(customer_key)
    config = configparser.ConfigParser()
    config.read("config.ini")

    try:
        filter_expr = config[f"filters.{customer_key.lower()}"]['filter']
    except KeyError:
        print(f"No filter expression found for customer '{customer_key}'. Returning unfiltered DataFrame.")
        return df

    def safe_eval(row):
        try:
            return bool(eval(filter_expr, {"re": re}, row.to_dict()))
        except Exception as e:
            # Optional: log error per row if needed
            # print(f"Row filter error: {e}")
            return False
    print(df[df.apply(safe_eval, axis=1)])
    return df[df.apply(safe_eval, axis=1)]

def format_patient_count(val):
    try:
        # Try converting to float and then int
        int_val = int(float(val))
        return "{:,}".format(int_val)
    except (ValueError, TypeError):
        # If conversion fails, return the original string
        return val


