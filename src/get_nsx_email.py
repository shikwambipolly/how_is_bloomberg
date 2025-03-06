from O365 import Account
import pandas as pd
import io
from datetime import datetime, timedelta
import logging
from utils import retry_with_notification
from config import Config
from workflow_result import WorkflowResult
import pytz  # Library for handling timezones
from pathlib import Path

# Set up logging
logger = logging.getLogger('nsx_workflow')
logger.setLevel(logging.INFO)

# Create a file handler
log_file = Config.get_logs_path() / f'nsx_email_fetch_{datetime.now().strftime("%Y%m%d")}.log'
file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.INFO)

# Create a formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(file_handler)

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
            logger.info("Successfully authenticated with Microsoft 365")
        except Exception as e:
            logger.error(f"Authentication error: {str(e)}")
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
            
            logger.info("Found NSX subfolder")
            
            # Get current time in UTC
            now = datetime.now(pytz.UTC)
            time_threshold = now - timedelta(hours=3)
            
            # Query for emails from NSX in the last 12 hours
            query = nsx_folder.new_query()
            query.on_attribute('from').equals('info@nsx.com.na')
            query.chain('and').on_attribute('receivedDateTime').greater_equal(time_threshold)
            
            # Get messages with full details including attachments
            messages = list(nsx_folder.get_messages(query=query, limit=25, download_attachments=True))
            
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
            
            # Ensure we have the full message with attachments
            if not hasattr(latest_message, 'attachments'):
                latest_message.attachments.download_attachments()
            
            logger.info(f"Found latest NSX email from {latest_time}")
            return latest_message
            
        except Exception as e:
            logger.error(f"Error fetching NSX email: {str(e)}")
            raise
    
    @retry_with_notification()
    def download_nsx_report(self, message):
        """Download the NSX Daily Report attachment"""
        try:
            logger.info(f"Processing email with subject: {message.subject}")
            
            # Ensure we have the full message details
            message.attachments.download_attachments()
            
            # Get all attachments
            attachments = list(message.attachments)
            
            # Look for NSX Daily Report
            for attachment in attachments:
                if hasattr(attachment, 'name') and attachment.name and "NSX Daily Report" in attachment.name:
                    logger.info(f"Found NSX Daily Report attachment: {attachment.name}")
                    
                    # Create temp directory if it doesn't exist
                    temp_dir = Config.get_output_path('temp')
                    temp_file = temp_dir / attachment.name
                    
                    try:
                        # Try to save using the attachment's save method
                        try:
                            attachment.save(temp_dir)
                        except Exception as save_error:
                            # Fallback: try to save manually
                            if hasattr(attachment, 'content'):
                                with open(temp_file, 'wb') as f:
                                    content = attachment.content
                                    if isinstance(content, str):
                                        content = content.encode('utf-8')
                                    f.write(content)
                            else:
                                raise ValueError("No content available in attachment")
                        
                        if not temp_file.exists() or temp_file.stat().st_size == 0:
                            raise ValueError("Failed to save attachment or file is empty")
                        
                        return temp_file
                        
                    except Exception as e:
                        if temp_file.exists():
                            temp_file.unlink()
                        raise
            
            raise ValueError("No NSX Daily Report attachment found in the email")
            
        except Exception as e:
            logger.error(f"Error downloading attachment: {str(e)}")
            raise
    
    def process_bonds_data(self, excel_path):
        """Process the bonds data from the Excel file"""
        try:
            logger.info(f"Processing NSX Daily Report from: {excel_path}")
            
            # First read to find the header row
            df = pd.read_excel(excel_path, sheet_name="Bonds-Trading ATS", header=None)
            
            # Find the header row by looking for 'Date' and 'Security'
            header_row = None
            for idx, row in df.iterrows():
                if 'Date' in row.values and 'Security' in row.values and 'Benchmark' in row.values:
                    header_row = idx
                    break
            
            if header_row is None:
                raise ValueError("Could not find header row with 'Date', 'Security', and 'Benchmark'")
            
            # Now read the Excel file again, using the found header row as the header
            df_processed = pd.read_excel(
                excel_path,
                sheet_name="Bonds-Trading ATS",
                skiprows=header_row,  # Skip rows up to the header
                header=0  # Use the first row (former header_row) as headers
            )
            
            # Clean up column names
            df_processed.columns = df_processed.columns.map(lambda x: str(x).strip())
            
            # Rename 'Unnamed: 7' to 'Prev Mark To (Yield)'
            unnamed_cols = [col for col in df_processed.columns if 'Unnamed: 7' in col]
            if unnamed_cols:
                df_processed.rename(columns={unnamed_cols[0]: 'Prev Mark To (Yield)'}, inplace=True)
            
            # Remove all remaining unnamed columns
            unnamed_cols = [col for col in df_processed.columns if 'Unnamed' in col]
            df_processed.drop(columns=unnamed_cols, inplace=True)
            
            # Drop any completely empty rows
            df_processed.dropna(how='all', inplace=True)
            
            # Reset the index after dropping rows
            df_processed.reset_index(drop=True, inplace=True)
            
            if df_processed.empty:
                raise ValueError("No data found after headers in Bonds-Trading ATS sheet")
            
            # Log the column names to verify alignment
            logger.info(f"Columns found in processed data: {df_processed.columns.tolist()}")
            
            # Clean up temporary file
            if Path(excel_path).parent.name == 'temp':
                Path(excel_path).unlink()
            
            return df_processed
            
        except Exception as e:
            logger.error(f"Error processing bonds data: {str(e)}")
            if Path(excel_path).parent.name == 'temp' and excel_path.exists():
                excel_path.unlink()
            raise
    
    def save_bonds_data(self, df):
        """Save the bonds data to CSV"""
        try:
            output_file = Config.get_output_path() / f'nsx_bonds_{datetime.now().strftime("%Y%m%d")}.csv'
            df.to_csv(output_file, index=False)
            logger.info(f"Successfully saved bonds data to {output_file}")
            return output_file
        except Exception as e:
            logger.error(f"Error saving bonds data: {str(e)}")
            raise

def run_nsx_workflow() -> WorkflowResult:
    """Run the complete NSX email workflow"""
    try:
        # Initialize processor
        processor = NSXEmailProcessor()
        
        # Get latest NSX email
        latest_email = processor.get_latest_nsx_email()
        
        # Download the report
        excel_path = processor.download_nsx_report(latest_email)
        
        # Process the bonds data
        df = processor.process_bonds_data(excel_path)
        
        # Save to CSV
        output_file = processor.save_bonds_data(df)
        
        logger.info(f"Successfully completed NSX workflow")
        return WorkflowResult(success=True, data=df)
        
    except Exception as e:
        error_msg = f"Error in NSX workflow: {str(e)}"
        logger.error(error_msg)
        return WorkflowResult(success=False, error=error_msg)

if __name__ == "__main__":
    result = run_nsx_workflow()
    if result.success:
        print("NSX data preview:")
        print(result.data.head())
