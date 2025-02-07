# main.py

from get_receiver_email import get_emails_by_client

# Ask the user for the client name
client_name = 'ABBVIE'

# Call the function to get the emails for the given client
emails = get_emails_by_client(client_name)

# Display the result
if emails:
    print(f"Emails for {client_name}: {', '.join(emails)}")
else:
    print(f"No emails found for client '{client_name}'")
