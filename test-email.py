import os

# Path to the file
file_path = r"C:\Users\AbhijeetPalo\OneDrive - ConcertAI\SAI\ClientDeliveryCount\EmailAttachment\202412_ConcertAI Genome360 Data Dictionary.xlsx"

# Check if the file exists
if os.path.exists(file_path):
    print(f"File found: {file_path}")
    # Proceed with your file processing/attachment logic here
else:
    print(f"Warning: The file does not exist: {file_path}")
