from O365 import Account
import pandas as pd
import io
from datetime import datetime, timedelta
import logging
from utils import retry_with_notification
from config import Config
from workflow_result import WorkflowResult
import pytz  # Library for handling timezones

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=Config.get_logs_path() / f'nsx_email_fetch_{datetime.now().strftime("%Y%m%d")}.log'
)

class NSXEmailProcessor:
    def __init__(self):
        # Initialize the O365 Account
        self.account = Account((Config.O365_CLIENT_ID, Config.O365_CLIENT_SECRET))
        
        # Ensure we're authenticated
        if not self.account.is_authenticated:
            self.authenticate()
    
    @retry_with_notification()
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
    
    @retry_with_notification()
    def get_latest_nsx_email(self):
        """Get the latest email from NSX"""
        try:
            # Access mailbox
            mailbox = self.account.mailbox()
            
            # Get the main Inbox folder
            inbox = mailbox.inbox_folder()
            
            # Get subfolders of Inbox
            inbox_subfolders = inbox.get_folders()
            
            # Find the NSX subfolder
            nsx_folder = None
            for folder in inbox_subfolders:
                if folder.name == "NSX":
                    nsx_folder = folder
                    break
            
            if not nsx_folder:
                raise ValueError("Could not find the NSX subfolder in your Inbox")
            
            logging.info("Successfully found NSX subfolder")
            
            # Get current time in UTC
            now = datetime.now(pytz.UTC)
            time_threshold = now - timedelta(hours=12)
            
            # Query for emails from NSX in the last 12 hours
            query = nsx_folder.new_query()
            query.on_attribute('from').equals('info@nsx.com.na')
            query.chain('and').on_attribute('receivedDateTime').greater_equal(time_threshold)
            
            # Get messages
            messages = nsx_folder.get_messages(query=query, limit=25)  # Limit to recent messages
            
            # Sort by received time and get latest
            latest_message = None
            latest_time = datetime.min.replace(tzinfo=pytz.UTC)
            
            for message in messages:
                if message.received:
                    # Ensure message.received is timezone-aware
                    received_time = message.received
                    if received_time.tzinfo is None:
                        received_time = pytz.UTC.localize(received_time)
                    
                    if received_time > latest_time:
                        latest_message = message
                        latest_time = received_time
            
            if not latest_message:
                raise ValueError("No NSX emails found in the last 12 hours")
            
            logging.info(f"Found latest NSX email from {latest_time}")
            return latest_message
            
        except Exception as e:
            logging.error(f"Error fetching NSX email: {str(e)}")
            raise
    
    @retry_with_notification()
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
            
            raise ValueError("No NSX Daily Report attachment found in the email")
            
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
            output_file = Config.get_output_path('nsx') / f'nsx_bonds_{datetime.now().strftime("%Y%m%d")}.csv'
            df.to_csv(output_file, index=False)
            logging.info(f"Successfully saved bonds data to {output_file}")
            return output_file
        except Exception as e:
            logging.error(f"Error saving bonds data: {str(e)}")
            raise

def run_nsx_workflow() -> WorkflowResult:
    """Run the complete NSX email workflow"""
    try:
        # Initialize processor
        processor = NSXEmailProcessor()
        
        # Get latest NSX email
        latest_email = processor.get_latest_nsx_email()
        
        # Download the report
        excel_data = processor.download_nsx_report(latest_email)
        
        # Process the bonds data
        df = processor.process_bonds_data(excel_data)
        
        # Save to CSV
        output_file = processor.save_bonds_data(df)
        
        logging.info(f"Successfully completed NSX workflow")
        return WorkflowResult(success=True, data=df)
        
    except Exception as e:
        error_msg = f"Error in NSX workflow: {str(e)}"
        logging.error(error_msg)
        return WorkflowResult(success=False, error=error_msg)

if __name__ == "__main__":
    result = run_nsx_workflow()
    if result.success:
        print("NSX data preview:")
        print(result.data.head())
