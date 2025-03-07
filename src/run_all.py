import logging
from datetime import datetime
from get_yields_terminal import run_terminal_workflow
from get_nsx_email import run_nsx_workflow
from get_IJG_daily import run_ijg_workflow
from process_closing_yields import run_closing_yields_workflow
from post_processing import run_post_processing_workflow
from utils import send_workflow_email
from config import Config
from typing import Optional
import pandas as pd
from workflow_result import WorkflowResult

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=Config.get_logs_path() / f'master_workflow_{datetime.now().strftime("%Y%m%d")}.log'
)

class DataCollector:
    def __init__(self):
        self.bloomberg_data: Optional[pd.DataFrame] = None
        self.nsx_data: Optional[pd.DataFrame] = None
        self.ijg_gi_data: Optional[pd.DataFrame] = None
        self.ijg_gc_data: Optional[pd.DataFrame] = None
        self.closing_yields_data: Optional[pd.DataFrame] = None
        self.post_processed_data: Optional[pd.DataFrame] = None
        self.workflow_status = {
            'bloomberg': False,
            'nsx': False,
            'ijg': False,
            'closing_yields': False
        }
    
    def store_data(self, source: str, result: WorkflowResult):
        """Store workflow results and update status"""
        if result.success and result.data is not None:
            if source == 'bloomberg':
                self.bloomberg_data = result.data
            elif source == 'nsx':
                self.nsx_data = result.data
            elif source == 'ijg':
                # Handle dictionary of dataframes for IJG
                if isinstance(result.data, dict):
                    self.ijg_gi_data = result.data.get('GI')
                    self.ijg_gc_data = result.data.get('GC')
                    if self.ijg_gi_data is not None and self.ijg_gc_data is not None:
                        logging.info(f"Successfully stored IJG GI data with {len(self.ijg_gi_data)} rows")
                        logging.info(f"Successfully stored IJG GC data with {len(self.ijg_gc_data)} rows")
                    else:
                        logging.error("Missing GI or GC data in IJG result")
                        self.workflow_status[source] = False
                        return
            elif source == 'closing_yields':
                self.closing_yields_data = result.data
                logging.info(f"Successfully stored closing yields data with {len(self.closing_yields_data)} rows")
            elif source == 'post_processing':
                self.post_processed_data = result.data
                logging.info(f"Successfully stored post-processed data with {len(self.post_processed_data)} rows")
            
            self.workflow_status[source] = True
            if source != 'ijg':  # Already logged IJG data above
                logging.info(f"Successfully stored {source} data with {len(result.data)} rows")
        else:
            if source in self.workflow_status:  # Only update status for tracked workflows
                self.workflow_status[source] = False
            logging.error(f"Failed to store {source} data: {result.error}")
    
    def get_failed_workflows(self):
        """Get list of failed workflows"""
        return [name for name, status in self.workflow_status.items() if not status]
    
    def all_workflows_successful(self):
        """Check if all workflows were successful"""
        return all(self.workflow_status.values())
    
    def get_all_data(self):
        """Return all collected data"""
        return {
            'bloomberg': self.bloomberg_data,
            'nsx': self.nsx_data,
            'ijg_gi': self.ijg_gi_data,
            'ijg_gc': self.ijg_gc_data,
            'closing_yields': self.closing_yields_data,
            'post_processed': self.post_processed_data
        }

def ensure_output_directory():
    """Ensure output directory exists and is ready"""
    try:
        # Create output directories for each data source
        Config.get_output_path('bloomberg')
        Config.get_output_path('nsx')
        Config.get_output_path('ijg')
        Config.get_logs_path()
        
    except Exception as e:
        logging.error(f"Error creating directories: {str(e)}")
        raise

def run_all_workflows():
    """Run all data collection workflows and return collected data"""
    try:
        # Validate configuration
        Config.validate()
        
        # Ensure directories exist
        ensure_output_directory()
        
        # Initialize data collector
        collector = DataCollector()
        
        # Run Bloomberg Terminal workflow
        logging.info("Starting Bloomberg Terminal workflow...")
        bloomberg_result = run_terminal_workflow()
        collector.store_data('bloomberg', bloomberg_result)
        
        # Run NSX Email workflow
        logging.info("Starting NSX Email workflow...")
        nsx_result = run_nsx_workflow()
        collector.store_data('nsx', nsx_result)
        
        # Run IJG Daily workflow
        logging.info("Starting IJG Daily workflow...")
        ijg_result = run_ijg_workflow()
        collector.store_data('ijg', ijg_result)
        
        # Check if all data collection workflows were successful
        initial_workflows = ['bloomberg', 'nsx', 'ijg']
        initial_failed = [name for name in initial_workflows if not collector.workflow_status[name]]
        
        if not initial_failed:
            # Run closing yields workflow only if all other workflows succeeded
            logging.info("Starting Closing Yields workflow...")
            closing_yields_result = run_closing_yields_workflow(collector)
            collector.store_data('closing_yields', closing_yields_result)
            
            # Run post-processing workflow only if closing yields workflow was successful
            if closing_yields_result.success and collector.closing_yields_data is not None:
                logging.info("Starting Post-Processing workflow...")
                post_processing_result = run_post_processing_workflow(collector.closing_yields_data)
                collector.store_data('post_processing', post_processing_result)
                
                if post_processing_result.success:
                    logging.info("Post-Processing workflow completed successfully")
                else:
                    logging.error(f"Post-Processing workflow failed: {post_processing_result.error}")
            else:
                logging.error("Skipping Post-Processing workflow due to failed Closing Yields workflow")
        else:
            logging.error("Skipping Closing Yields workflow due to failed data collection workflows")
            collector.workflow_status['closing_yields'] = False
        
        # Get final workflow status
        failed_workflows = collector.get_failed_workflows()
        successful_workflows = [name for name, status in collector.workflow_status.items() if status]
        
        # Prepare email subject based on overall status
        if not failed_workflows:
            subject = "✓ Bond Data Collections: All Successful"
        elif not successful_workflows:
            subject = "✗ Bond Data Collections: All Failed"
        else:
            subject = f"⚠ Bond Data Collections: {len(successful_workflows)} Successful, {len(failed_workflows)} Failed"
        
        # Build email body
        body = "DAILY BOND DATA COLLECTION REPORT\n"
        body += "===================================\n\n"
        body += f"Date: {datetime.now().strftime('%Y-%m-%d')}\n"
        body += f"Time: {datetime.now().strftime('%H:%M:%S')}\n\n"
        body += "WORKFLOW STATUS\n"
        body += "==============\n\n"
        
        # Add successful workflows section if any
        if successful_workflows:
            body += "Successful Collections:\n"
            
            # Bloomberg summary
            if collector.workflow_status['bloomberg']:
                body += "  ✓ Bloomberg Terminal\n"
                body += f"     • Collected data for {len(collector.bloomberg_data) if collector.bloomberg_data is not None else 0} bonds\n\n"
            
            # NSX summary
            if collector.workflow_status['nsx']:
                body += "  ✓ NSX Daily Report\n"
                body += f"     • Processed {len(collector.nsx_data) if collector.nsx_data is not None else 0} rows\n\n"
            
            # IJG summary
            if collector.workflow_status['ijg']:
                body += "  ✓ IJG Daily Report\n"
                body += f"     • GI data: {len(collector.ijg_gi_data) if collector.ijg_gi_data is not None else 0} rows\n"
                body += f"     • GC data: {len(collector.ijg_gc_data) if collector.ijg_gc_data is not None else 0} rows\n\n"
            
            # Closing Yields summary
            if collector.workflow_status['closing_yields']:
                body += "  ✓ Closing Yields Processing\n"
                body += f"     • Processed {len(collector.closing_yields_data) if collector.closing_yields_data is not None else 0} bonds\n\n"
        
        # Add failed workflows section if any
        if failed_workflows:
            body += "Failed Collections:\n"
            
            # Create a dictionary to store workflow results and errors
            workflow_results = {
                'bloomberg': bloomberg_result,
                'nsx': nsx_result,
                'ijg': ijg_result,
                'closing_yields': closing_yields_result if 'closing_yields' not in initial_failed else None
            }
            
            for workflow in failed_workflows:
                body += f"  ✗ {workflow.title()}\n"
                # Add error details if available
                result = workflow_results.get(workflow)
                if result and result.error:
                    body += f"     • Error: {result.error}\n"
            body += "\n"
        
        # Add overall statistics
        body += "SUMMARY\n"
        body += "=======\n"
        body += f"Total workflows: {len(collector.workflow_status)}\n"
        body += f"Successful: {len(successful_workflows)}\n"
        body += f"Failed: {len(failed_workflows)}"
        
        # Send the status email
        send_workflow_email(subject, body)
        
        return collector
        
    except Exception as e:
        error_message = f"Error in master workflow: {str(e)}"
        logging.error(error_message)
        
        # Send email for critical error
        error_body = "DAILY BOND DATA COLLECTION REPORT\n"
        error_body += "===================================\n\n"
        error_body += f"Date: {datetime.now().strftime('%Y-%m-%d')}\n"
        error_body += f"Time: {datetime.now().strftime('%H:%M:%S')}\n\n"
        error_body += "CRITICAL ERROR\n"
        error_body += "==============\n\n"
        error_body += "A critical error occurred in the master workflow:\n"
        error_body += f"{str(e)}\n\n"
        error_body += "Stack trace (if available):\n"
        import traceback
        error_body += traceback.format_exc()
        
        send_workflow_email("✗ Bond Data Collections: Critical Error", error_body)
        return None

def process_collected_data(collector: DataCollector):
    """Example function to process the collected data"""
    if not collector.all_workflows_successful():
        logging.warning("Some workflows failed. Processing available data only.")
    
    data = collector.get_all_data()
    
    # Example: Print summary of available data
    for source, df in data.items():
        if df is not None:
            logging.info(f"{source} data summary:")
            logging.info(f"- Shape: {df.shape}")
            logging.info(f"- Columns: {df.columns.tolist()}")
        else:
            logging.warning(f"No data available for {source}")
    
    return data

if __name__ == "__main__":
    # Make sure its not a weekend day
    if datetime.now().weekday() >= 5:
        logging.warning("Skipping run on weekend day")
        exit()
    
    # Run all workflows and collect data
    collector = run_all_workflows()
    
    if collector:
        # Process the collected data
        all_data = process_collected_data(collector)
        
        # Example: Access individual DataFrames
        bloomberg_df = collector.bloomberg_data
        nsx_df = collector.nsx_data
        ijg_gi_df = collector.ijg_gi_data
        ijg_gc_df = collector.ijg_gc_data
        
        # Now you can work with the DataFrames as needed
        # For example:
        if bloomberg_df is not None:
            print("\nBloomberg Data Preview:")
            print(bloomberg_df.head())
        
        if nsx_df is not None:
            print("\nNSX Data Preview:")
            print(nsx_df.head())
        
        if ijg_gi_df is not None:
            print("\nIJG GI Data Preview:")
            print(ijg_gi_df.head())
        
        if ijg_gc_df is not None:
            print("\nIJG GC Data Preview:")
            print(ijg_gc_df.head()) 
        
        