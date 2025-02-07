import re
import sys
# Function to parse the text file and extract email ids based on client names

file_path = 'C:\\Users\\AbhijeetPalo\\PycharmProjects\\pythonProject\\venv\\DeliveryCount-Email-Generator\\email_templates\\receiver_list.txt'
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
    emails_by_client = get_emails_from_file(file_path)
    print(emails_by_client)

    # Fetch emails for the specific client
    if client_name in emails_by_client:
        return emails_by_client[client_name]
    else:
        return None

# get_emails_by_client(client_name, file_path)
