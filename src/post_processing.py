"""
This module performs post-processing operations after the closing yields workflow has successfully completed.
It is designed to run as an add-on to the main project and does not affect email reporting.
"""

import pandas as pd
import logging
from datetime import datetime
from pathlib import Path
import os
import sys
import openpyxl  # For Excel file manipulation

# Get the correct paths for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

from config import Config
from workflow_result import WorkflowResult

# Set up logging - separate from the main workflow logs
logger = logging.getLogger('post_processing')
logger.setLevel(logging.INFO)

# Create a file handler
log_file = Config.get_logs_path() / f'post_processing_{datetime.now().strftime("%Y%m%d")}.log'
file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.INFO)

# Create a formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(file_handler)

class PostProcessor:
    """
    Post-processor for additional operations after the closing yields workflow.
    This class handles any additional processing that needs to happen after 
    the closing yields workflow has completed successfully.
    """
    
    def __init__(self, closing_yields_data: pd.DataFrame):
        """
        Initialize the post-processor with the closing yields data.
        
        Args:
            closing_yields_data: DataFrame containing the processed closing yields
        """
        self.closing_yields_data = closing_yields_data
        logger.info("Initialized PostProcessor with closing yields data")
        self.excel_path = Path(Config.BASE_DIR) / "Bond Price Calculator.xlsx"
        
    def process_data(self) -> pd.DataFrame:
        """
        Update the Bond Price Calculator Excel file with the latest closing yields.
        
        This method:
        1. Reads the "Input" sheet from the Bond Price Calculator Excel file
        2. Gets the security names from the second row (index 1)
        3. Creates a new row with today's date and the closing yields from our data
        4. Adds this new row to the Excel sheet
        5. Saves the updated Excel file
        
        Returns:
            DataFrame containing the post-processed data (same as input data)
        """
        try:
            logger.info("Starting post-processing operations")
            
            # Verify Excel file exists
            if not self.excel_path.exists():
                logger.error(f"Excel file not found at {self.excel_path}")
                raise FileNotFoundError(f"Bond Price Calculator Excel file not found at {self.excel_path}")
            
            logger.info(f"Reading Excel file from {self.excel_path}")
            
            # Load the workbook and select the Input sheet
            try:
                workbook = openpyxl.load_workbook(self.excel_path)
                input_sheet = workbook["Input"]
                logger.info("Successfully opened 'Input' sheet")
            except Exception as e:
                logger.error(f"Error opening Excel sheet: {str(e)}")
                raise
            
            # Get security names from the second row (index 1)
            securities = {}
            for col in range(2, input_sheet.max_column + 1):  # Start from second column
                cell_value = input_sheet.cell(row=2, column=col).value
                if cell_value:
                    securities[str(cell_value).strip()] = col
            
            logger.info(f"Found {len(securities)} securities in the Excel sheet")
            
            # Create a map of securities to their closing yields from our DataFrame
            security_yields = {}
            for _, row in self.closing_yields_data.iterrows():
                security = row['Security']
                closing_yield = row['Closing Yield']
                if pd.notna(closing_yield):
                    security_yields[security] = closing_yield
            
            logger.info(f"Found {len(security_yields)} securities with closing yields in our data")
            
            # Get today's date
            today_date = datetime.now().strftime("%Y-%m-%d")
            
            # Determine the next row to write data
            next_row = input_sheet.max_row + 1
            logger.info(f"Adding new data at row {next_row}")
            
            # Write today's date in the first column
            input_sheet.cell(row=next_row, column=1).value = today_date
            
            # Match securities and write closing yields
            securities_written = 0
            for security, col in securities.items():
                if security in security_yields:
                    input_sheet.cell(row=next_row, column=col).value = security_yields[security]
                    securities_written += 1
                else:
                    logger.warning(f"Security {security} not found in closing yields data")
            
            logger.info(f"Added closing yields for {securities_written} securities")
            
            # Check for any securities in our data that were not found in the Excel sheet
            missing_securities = [sec for sec in security_yields.keys() if sec not in securities]
            if missing_securities:
                logger.warning(f"These securities were in our data but not in Excel: {missing_securities}")
            
            # Save the updated workbook
            try:
                workbook.save(self.excel_path)
                logger.info(f"Successfully saved updated Excel file to {self.excel_path}")
            except Exception as e:
                logger.error(f"Error saving Excel file: {str(e)}")
                raise
            
            logger.info("Post-processing operations completed successfully")
            
            # Add timestamp and other columns
            self.closing_yields_data['Processing_Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # You can add additional calculations as needed
            # For example, calculate yield deviation from previous day
            self.closing_yields_data['Yield_Deviation'] = 0.0  # Placeholder, you can implement actual calculations
            
            # Return the original DataFrame unchanged
            return self.closing_yields_data.copy()
            
        except Exception as e:
            logger.error(f"Error during post-processing: {str(e)}")
            raise
    
    def save_results(self, df: pd.DataFrame) -> Path:
        """
        Save the post-processed data to a CSV file.
        
        Args:
            df: DataFrame containing the post-processed data
            
        Returns:
            Path to the saved CSV file
        """
        try:
            # Save to today's output directory with a different name
            output_file = Config.get_output_path() / f'post_processed_data_{datetime.now().strftime("%Y%m%d")}.csv'
            
            # Save to CSV
            df.to_csv(output_file, index=False)
            logger.info(f"Successfully saved post-processed data to {output_file}")
            
            return output_file
            
        except Exception as e:
            logger.error(f"Error saving post-processed data: {str(e)}")
            raise

def run_post_processing_workflow(closing_yields_data: pd.DataFrame) -> WorkflowResult:
    """
    Run the post-processing workflow using the closing yields data.
    This function is called from run_all.py when the closing yields workflow
    has completed successfully.
    
    Args:
        closing_yields_data: DataFrame containing the processed closing yields
        
    Returns:
        WorkflowResult containing success status and processed data
    """
    try:
        logger.info("Starting post-processing workflow")
        
        # Initialize processor with closing yields data
        processor = PostProcessor(closing_yields_data)
        
        # Process the data
        results_df = processor.process_data()
        
        # Save results
        output_file = processor.save_results(results_df)
        
        logger.info("Successfully completed post-processing workflow")
        return WorkflowResult(success=True, data=results_df)
        
    except Exception as e:
        error_msg = f"Error in post-processing workflow: {str(e)}"
        logger.error(error_msg)
        return WorkflowResult(success=False, error=error_msg)

# This section only runs if you execute this file directly (not when imported)
if __name__ == "__main__":
    try:
        # Get the latest closing yields file for testing
        today_str = datetime.now().strftime("%Y%m%d")
        closing_yields_file = Config.get_output_path() / f'closing_yields_{today_str}.csv'
        
        if not closing_yields_file.exists():
            print(f"Error: Could not find closing yields file at {closing_yields_file}")
            exit(1)
        
        # Load the closing yields data
        closing_yields_data = pd.read_csv(closing_yields_file)
        
        # Run the post-processing workflow
        result = run_post_processing_workflow(closing_yields_data)
        
        if result.success:
            print("Post-processing completed successfully")
            print("Post-processed data preview:")
            print(result.data.head())
        else:
            print(f"Post-processing failed: {result.error}")
            
    except Exception as e:
        print(f"Error: {str(e)}") 