"""
This module processes the IJG Daily Excel report and extracts two types of data:
1. GI Data: Specific rows from the Yields sheet that contain GI codes
2. GC Data: Rows 2-19 from the Spread Calc sheet

The data is saved into separate CSV files with today's date in the filename.
"""

import pandas as pd  # Library for data manipulation and analysis
import re  # Library for regular expressions (pattern matching in text)
import logging  # Library for creating log files
from datetime import datetime  # Library for working with dates and times
from utils import retry_with_notification  # Custom retry mechanism
from config import Config  # Project configuration settings
from workflow_result import WorkflowResult  # Custom class for workflow results

# Set up logging to track the program's execution
# This creates a new log file each day with the date in the filename
logger = logging.getLogger('ijg_workflow')
logger.setLevel(logging.INFO)

# Create a file handler
log_file = Config.get_logs_path() / f'ijg_daily_{datetime.now().strftime("%Y%m%d")}.log'
file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.INFO)

# Create a formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(file_handler)

class IJGDailyProcessor:
    """
    Main class for processing the IJG Daily Excel report.
    This class handles reading the Excel file, extracting specific data,
    and saving the results to CSV files.
    """
    
    def __init__(self):
        """
        Initialize the processor by checking if the Excel file exists.
        Raises an error if the file is not found.
        """
        self.excel_path = Config.IJG_DAILY_PATH
        if not self.excel_path.exists():
            raise FileNotFoundError(f"Excel file not found at path: {self.excel_path}")
    
    def _is_gi_code(self, value):
        """
        Check if a value matches the GI code pattern (GI followed by exactly 2 numbers).
        Example: 'GI01' would match, but 'GI1' or 'GI123' would not.
        
        Args:
            value: The value to check
            
        Returns:
            bool: True if the value matches the GI pattern, False otherwise
        """
        if pd.isna(value):  # Check if the value is empty/NaN
            return False
        pattern = r'^GI\d{2}$'  # Pattern: Start with GI, followed by exactly 2 digits
        return bool(re.match(pattern, str(value).strip()))
    
    @retry_with_notification()  # Retry this operation if it fails
    def extract_yields_data(self):
        """
        Extract data from the Yields sheet of the Excel file.
        Only keeps rows where the first column (A) contains a GI code.
        
        Returns:
            pandas.DataFrame: The extracted data containing only rows with GI codes
        """
        try:
            # Read the Yields sheet from the Excel file
            df_yields = pd.read_excel(
                self.excel_path,
                sheet_name="Yields",
                engine='openpyxl'  # Use openpyxl engine for .xlsx files
            )
            
            # Filter rows where the first column contains GI codes
            gi_rows = df_yields[df_yields.iloc[:, 0].apply(self._is_gi_code)]
            
            if gi_rows.empty:
                raise ValueError("No GI codes found in Yields sheet")
            
            logger.info(f"Found {len(gi_rows)} rows with GI codes in Yields sheet")
            return gi_rows
            
        except Exception as e:
            logger.error(f"Error extracting Yields data: {str(e)}")
            raise
    
    @retry_with_notification()  # Retry this operation if it fails
    def extract_spread_data(self):
        """
        Extract rows 2-19 from the Spread Calc sheet of the Excel file.
        These rows contain specific spread calculation data.
        
        Returns:
            pandas.DataFrame: The extracted rows from the Spread Calc sheet
        """
        try:
            # Read the Spread Calc sheet from the Excel file
            df_spread = pd.read_excel(
                self.excel_path,
                sheet_name="Spread calc",
                engine='openpyxl'
            )
            
            # Extract rows 2-19 (index 1-18 since Python uses 0-based indexing)
            spread_rows = df_spread.iloc[1:19].copy()
            
            if spread_rows.empty:
                raise ValueError("No data found in rows 2-19 of Spread Calc sheet")
            
            logger.info(f"Successfully extracted {len(spread_rows)} rows from Spread Calc sheet")
            return spread_rows
            
        except Exception as e:
            logger.error(f"Error extracting Spread Calc data: {str(e)}")
            raise
    
    def save_data(self, df: pd.DataFrame, data_type: str) -> str:
        """
        Save the extracted data to a CSV file.
        The filename includes the data type (GI or GC) and today's date.
        
        Args:
            df: The data to save
            data_type: Type of data ('GI' or 'GC')
            
        Returns:
            str: Path to the saved CSV file
        """
        try:
            # Create filename with format: ijg_<type>_YYYYMMDD.csv
            filename = f'ijg_{data_type}_{datetime.now().strftime("%Y%m%d")}.csv'
            output_file = Config.get_output_path() / filename
            
            # Save to CSV file
            df.to_csv(output_file, index=False)
            logger.info(f"Successfully saved {data_type} data to {output_file}")
            return output_file
            
        except Exception as e:
            logger.error(f"Error saving {data_type} data: {str(e)}")
            raise

def run_ijg_workflow() -> WorkflowResult:
    """
    Main function to run the complete IJG workflow:
    1. Initialize the processor
    2. Extract both types of data (GI and GC)
    3. Save each dataset to its own CSV file
    4. Return the results
    
    Returns:
        WorkflowResult: Object containing success status and the extracted data
    """
    try:
        # Create processor instance
        processor = IJGDailyProcessor()
        
        # Extract both types of data
        yields_data = processor.extract_yields_data()  # Get GI data
        spread_data = processor.extract_spread_data()  # Get GC data
        
        # Save each dataset to its own CSV file
        gi_file = processor.save_data(yields_data, 'GI')
        gc_file = processor.save_data(spread_data, 'GC')
        
        # Return both datasets in a dictionary
        result_data = {
            'yields': yields_data,
            'spread': spread_data
        }
        
        logger.info(f"Successfully completed IJG workflow")
        return WorkflowResult(success=True, data=result_data)
        
    except Exception as e:
        error_msg = f"Error in IJG workflow: {str(e)}"
        logger.error(error_msg)
        return WorkflowResult(success=False, error=error_msg)

# This section only runs if you execute this file directly (not when imported)
if __name__ == "__main__":
    result = run_ijg_workflow()
    if result.success:
        print("\nIJG GI data preview:")
        print(result.data['yields'].head())
        print("\nIJG GC data preview:")
        print(result.data['spread'].head())
