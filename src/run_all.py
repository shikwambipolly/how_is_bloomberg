import logging
from datetime import datetime
from get_yields_terminal import run_terminal_workflow
from get_nsx_email import run_nsx_workflow
from get_IJG_daily import run_ijg_workflow
from utils import send_error_email
from config import Config

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=Config.get_logs_path() / f'master_workflow_{datetime.now().strftime("%Y%m%d")}.log'
)

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
    """Run all data collection workflows"""
    try:
        # Validate configuration
        Config.validate()
        
        # Ensure directories exist
        ensure_output_directory()
        
        # Track workflow status
        workflow_status = {
            'bloomberg': False,
            'nsx': False,
            'ijg': False
        }
        
        # Run Bloomberg Terminal workflow
        logging.info("Starting Bloomberg Terminal workflow...")
        workflow_status['bloomberg'] = run_terminal_workflow()
        
        # Run NSX Email workflow
        logging.info("Starting NSX Email workflow...")
        workflow_status['nsx'] = run_nsx_workflow()
        
        # Run IJG Daily workflow
        logging.info("Starting IJG Daily workflow...")
        workflow_status['ijg'] = run_ijg_workflow()
        
        # Check for any failures
        failed_workflows = [name for name, status in workflow_status.items() if not status]
        
        if failed_workflows:
            error_message = f"The following workflows failed: {', '.join(failed_workflows)}"
            logging.error(error_message)
            send_error_email(error_message, "Master Workflow")
            return False
        
        logging.info("All workflows completed successfully")
        return True
        
    except Exception as e:
        error_message = f"Error in master workflow: {str(e)}"
        logging.error(error_message)
        send_error_email(error_message, "Master Workflow")
        return False

if __name__ == "__main__":
    run_all_workflows() 