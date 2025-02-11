from O365 import Account
import pandas as pd
import io
import json
import os
from datetime import datetime, timedelta
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=f'nsx_email_fetch_{datetime.now().strftime("%Y%m%d")}.log'
)

class NSXEmailProcessor:
    def __init__(self):
        # Load credentials from environment variables
        self.client_id = os.getenv('O365_CLIENT_ID')
        self.client_secret = os.getenv('O365_CLIENT_SECRET')
        
        if not all([self.client_id, self.client_secret]):
            raise ValueError(
                "Missing Microsoft 365 credentials. Please set environment variables: "
                "O365_CLIENT_ID, O365_CLIENT_SECRET"
            )
        
        # Initialize the O365 Account
        self.account = Account((self.client_id, self.client_secret))
        
        # Ensure we're authenticated
        if not self.account.is_authenticated:
            self.authenticate()
    
    def authenticate(self):
        """Authenticate with Microsoft 365"""
        try:
            result = self.account.authenticate()
            if not result:
                raise Exception("Authentication failed")
            logging.info("Successfully authenticated with Microsoft 365")
        except Exception as e:
            logging.error(f"Authentication error: {str(e)}")
            raise
    
    def get_latest_nsx_email(self):
        """Get the latest email from NSX"""
        try:
            # Access mailbox
            mailbox = self.account.mailbox()
            
            # Get inbox
            inbox = mailbox.inbox_folder()
            
            # Query for emails from NSX in the last 24 hours
            query = inbox.new_query()
            query.on_attribute('from').equals('info@nsx.com.na')
            query.chain('and').on_attribute('received').greater_equal(datetime.now() - timedelta(hours=12))
            
            # Get messages
            messages = inbox.get_messages(query=query, limit=25)  # Limit to recent messages
            
            # Sort by received time and get latest
            latest_message = None
            latest_time = datetime.min
            
            for message in messages:
                if message.received and message.received > latest_time:
                    latest_message = message
                    latest_time = message.received
            
            if not latest_message:
                logging.warning("No NSX emails found in the last 12 hours")
                return None
            
            logging.info(f"Found latest NSX email from {latest_time}")
            return latest_message
            
        except Exception as e:
            logging.error(f"Error fetching NSX email: {str(e)}")
            raise
    
    def download_nsx_report(self, message):
        """Download the NSX Daily Report attachment"""
        try:
            for attachment in message.attachments:
                if attachment.name and "NSX Daily Report" in attachment.name:
                    # Get the attachment content
                    content = attachment.content
                    
                    # Create BytesIO object from the content
                    excel_data = io.BytesIO(content)
                    logging.info(f"Successfully downloaded attachment: {attachment.name}")
                    return excel_data
            
            logging.warning("No NSX Daily Report attachment found in the email")
            return None
            
        except Exception as e:
            logging.error(f"Error downloading attachment: {str(e)}")
            raise
    
    def process_bonds_data(self, excel_data):
        """Process the bonds data from the Excel file"""
        try:
            # Read the Excel file
            df = pd.read_excel(
                excel_data,
                sheet_name="Bonds-Trading ATS",
                engine='openpyxl'
            )
            
            # Basic data validation
            if df.empty:
                raise ValueError("No data found in the Bonds-Trading ATS sheet")
            
            logging.info(f"Successfully processed bonds data. Found {len(df)} rows")
            return df
            
        except Exception as e:
            logging.error(f"Error processing bonds data: {str(e)}")
            raise
    
    def save_bonds_data(self, df):
        """Save the bonds data to CSV"""
        try:
            output_file = f'nsx_bonds_{datetime.now().strftime("%Y%m%d")}.csv'
            df.to_csv(output_file, index=False)
            logging.info(f"Successfully saved bonds data to {output_file}")
            return output_file
        except Exception as e:
            logging.error(f"Error saving bonds data: {str(e)}")
            raise

def main():
    try:
        # Initialize processor
        processor = NSXEmailProcessor()
        
        # Get latest NSX email
        latest_email = processor.get_latest_nsx_email()
        if not latest_email:
            logging.error("No NSX email found")
            return
        
        # Download the report
        excel_data = processor.download_nsx_report(latest_email)
        if not excel_data:
            logging.error("No NSX Daily Report found in the email")
            return
        
        # Process the bonds data
        df = processor.process_bonds_data(excel_data)
        
        # Save to CSV
        output_file = processor.save_bonds_data(df)
        
        print(f"Successfully processed NSX bonds data and saved to {output_file}")
        print("\nFirst few rows of the data:")
        print(df.head())
        
    except Exception as e:
        logging.error(f"Error in main execution: {str(e)}")
        raise

if __name__ == "__main__":
    main()
