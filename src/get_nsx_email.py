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
            time_threshold = now - timedelta(hours=24)
            
            # Query for emails from NSX in the last 12 hours
            query = nsx_folder.new_query()
            query.on_attribute('from').equals('info@nsx.com.na')
            query.chain('and').on_attribute('receivedDateTime').greater_equal(time_threshold)
            
            # Get messages with full details including attachments
            messages = list(nsx_folder.get_messages(query=query, limit=25, download_attachments=True))
            logging.info(f"Found {len(messages)} messages from NSX")
            
            # Sort by received time and get latest
            latest_message = None
            latest_time = datetime.min.replace(tzinfo=pytz.UTC)
            
            for message in messages:
                logging.info(f"Checking message: Subject='{message.subject}', Has attachments={hasattr(message, 'attachments')}")
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
            
            logging.info(f"Found latest NSX email from {latest_time}")
            logging.info(f"Subject: {latest_message.subject}")
            logging.info(f"Has attachments: {hasattr(latest_message, 'attachments')}")
            if hasattr(latest_message, 'attachments'):
                logging.info(f"Number of attachments: {len(list(latest_message.attachments))}")
            
            return latest_message
            
        except Exception as e:
            logging.error(f"Error fetching NSX email: {str(e)}")
            raise
    
    @retry_with_notification()
    def download_nsx_report(self, message):
        """Download the NSX Daily Report attachment"""
        try:
            logging.info(f"Checking message with subject: {message.subject}")
            
            # Ensure we have the full message details
            message.attachments.download_attachments()
            
            # Get all attachments
            attachments = list(message.attachments)
            logging.info(f"Found {len(attachments)} attachments")
            
            # Debug: Print all attachment names and details
            for idx, attachment in enumerate(attachments):
                logging.info(f"Attachment {idx + 1}:")
                logging.info(f"  - Name: {attachment.name if hasattr(attachment, 'name') else 'No name'}")
                logging.info(f"  - Type: {type(attachment)}")
                logging.info(f"  - Has content: {hasattr(attachment, 'content')}")
            
            # Look for NSX Daily Report
            for attachment in attachments:
                if hasattr(attachment, 'name') and attachment.name and "NSX Daily Report" in attachment.name:
                    logging.info(f"Found NSX Daily Report attachment: {attachment.name}")
                    
                    # Create temp directory if it doesn't exist
                    temp_dir = Config.get_output_path('temp')
                    
                    # Use the original filename
                    temp_file = temp_dir / attachment.name
                    
                    try:
                        # Try to save using the attachment's save method
                        try:
                            attachment.save(temp_dir)
                        except Exception as save_error:
                            logging.error(f"Error using save method: {str(save_error)}")
                            # Fallback: try to save manually
                            if hasattr(attachment, 'content'):
                                with open(temp_file, 'wb') as f:
                                    content = attachment.content
                                    if isinstance(content, str):
                                        content = content.encode('utf-8')
                                    f.write(content)
                            else:
                                raise ValueError("No content available in attachment")
                        
                        logging.info(f"Successfully saved attachment to: {temp_file}")
                        
                        if not temp_file.exists():
                            raise FileNotFoundError(f"File was not created: {temp_file}")
                        
                        # Verify file size
                        file_size = temp_file.stat().st_size
                        logging.info(f"Saved file size: {file_size} bytes")
                        
                        if file_size == 0:
                            raise ValueError("Saved file is empty")
                        
                        return temp_file
                        
                    except Exception as e:
                        logging.error(f"Error saving attachment: {str(e)}")
                        if temp_file.exists():
                            temp_file.unlink()
                        raise
            
            # If we get here, we didn't find the right attachment
            logging.error("Available attachment names:")
            for attachment in attachments:
                logging.error(f"- {attachment.name if hasattr(attachment, 'name') else 'No name'}")
            raise ValueError("No NSX Daily Report attachment found in the email")
            
        except Exception as e:
            logging.error(f"Error downloading attachment: {str(e)}")
            raise
    
    def process_bonds_data(self, excel_path):
        """Process the bonds data from the Excel file"""
        try:
            logging.info(f"Attempting to read Excel file from: {excel_path}")
            
            # Verify file exists and has content
            if not excel_path.exists():
                raise FileNotFoundError(f"Excel file not found: {excel_path}")
            
            file_size = excel_path.stat().st_size
            logging.info(f"Excel file size: {file_size} bytes")
            
            if file_size == 0:
                raise ValueError("Excel file is empty")
            
            # Try to read the file and get available sheets
            try:
                # First try to list available sheets
                xls = pd.ExcelFile(excel_path, engine='openpyxl')
                logging.info(f"Available sheets in Excel file: {xls.sheet_names}")
                
                # Read the specific sheet
                df = pd.read_excel(xls, sheet_name="Bonds-Trading ATS")
                xls.close()
                
            except Exception as e:
                logging.error(f"Error reading with openpyxl: {str(e)}")
                try:
                    # Try reading with xlrd
                    xls = pd.ExcelFile(excel_path, engine='xlrd')
                    logging.info(f"Available sheets (xlrd): {xls.sheet_names}")
                    
                    df = pd.read_excel(xls, sheet_name="Bonds-Trading ATS")
                    xls.close()
                    
                except Exception as e2:
                    logging.error(f"Error reading with xlrd: {str(e2)}")
                    # Try one last time with a direct read
                    df = pd.read_excel(excel_path)
                    logging.info("Successfully read file with default engine")
            
            # Basic data validation
            if df.empty:
                raise ValueError("No data found in the Excel sheet")
            
            logging.info(f"Successfully processed bonds data. Found {len(df)} rows")
            logging.info(f"Columns found: {df.columns.tolist()}")
            
            # Clean up temporary file
            if Path(excel_path).parent.name == 'temp':
                Path(excel_path).unlink()
                logging.info(f"Cleaned up temporary file: {excel_path}")
            
            return df
            
        except Exception as e:
            logging.error(f"Error processing bonds data: {str(e)}")
            # Clean up temporary file even if processing failed
            if Path(excel_path).parent.name == 'temp':
                try:
                    Path(excel_path).unlink()
                    logging.info(f"Cleaned up temporary file after error: {excel_path}")
                except:
                    pass
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
        excel_path = processor.download_nsx_report(latest_email)
        
        # Process the bonds data
        df = processor.process_bonds_data(excel_path)
        
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
