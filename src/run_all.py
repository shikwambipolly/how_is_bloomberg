import logging
from datetime import datetime
from get_yields_terminal import run_terminal_workflow
from get_nsx_email import run_nsx_workflow
from get_IJG_daily import run_ijg_workflow
from utils import send_error_email
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
        self.ijg_yields_data: Optional[pd.DataFrame] = None
        self.ijg_spread_data: Optional[pd.DataFrame] = None
        self.workflow_status = {
            'bloomberg': False,
            'nsx': False,
            'ijg': False
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
                    self.ijg_yields_data = result.data.get('yields')
                    self.ijg_spread_data = result.data.get('spread')
                    if self.ijg_yields_data is not None and self.ijg_spread_data is not None:
                        logging.info(f"Successfully stored IJG yields data with {len(self.ijg_yields_data)} rows")
                        logging.info(f"Successfully stored IJG spread data with {len(self.ijg_spread_data)} rows")
                    else:
                        logging.error("Missing yields or spread data in IJG result")
                        self.workflow_status[source] = False
                        return
            
            self.workflow_status[source] = True
            if source != 'ijg':  # Already logged IJG data above
                logging.info(f"Successfully stored {source} data with {len(result.data)} rows")
        else:
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
            'ijg_yields': self.ijg_yields_data,
            'ijg_spread': self.ijg_spread_data
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
        
        # Check for any failures
        failed_workflows = collector.get_failed_workflows()
        
        if failed_workflows:
            error_message = f"The following workflows failed: {', '.join(failed_workflows)}"
            logging.error(error_message)
            send_error_email(error_message, "Master Workflow")
        else:
            logging.info("All workflows completed successfully")
        
        return collector
        
    except Exception as e:
        error_message = f"Error in master workflow: {str(e)}"
        logging.error(error_message)
        send_error_email(error_message, "Master Workflow")
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
    # Run all workflows and collect data
    collector = run_all_workflows()
    
    if collector:
        # Process the collected data
        all_data = process_collected_data(collector)
        
        # Example: Access individual DataFrames
        bloomberg_df = collector.bloomberg_data
        nsx_df = collector.nsx_data
        ijg_yields_df = collector.ijg_yields_data
        ijg_spread_df = collector.ijg_spread_data
        
        # Now you can work with the DataFrames as needed
        # For example:
        if bloomberg_df is not None:
            print("\nBloomberg Data Preview:")
            print(bloomberg_df.head())
        
        if nsx_df is not None:
            print("\nNSX Data Preview:")
            print(nsx_df.head())
        
        if ijg_yields_df is not None:
            print("\nIJG Yields Data Preview:")
            print(ijg_yields_df.head())
        
        if ijg_spread_df is not None:
            print("\nIJG Spread Data Preview:")
            print(ijg_spread_df.head()) 