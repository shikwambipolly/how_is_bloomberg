import pandas as pd
import re
import logging
from datetime import datetime
from utils import retry_with_notification
from config import Config
from workflow_result import WorkflowResult

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=Config.get_logs_path() / f'ijg_daily_{datetime.now().strftime("%Y%m%d")}.log'
)

class IJGDailyProcessor:
    def __init__(self):
        self.excel_path = Config.IJG_DAILY_PATH
        if not self.excel_path.exists():
            raise FileNotFoundError(f"Excel file not found at path: {self.excel_path}")
    
    def _is_gi_code(self, value):
        """Check if value matches GI followed by 2 numbers pattern"""
        if pd.isna(value):
            return False
        pattern = r'^GI\d{2}$'
        return bool(re.match(pattern, str(value).strip()))
    
    @retry_with_notification()
    def extract_yields_data(self):
        """Extract data from Yields sheet where column A contains GI codes"""
        try:
            # Read the Yields sheet
            df_yields = pd.read_excel(
                self.excel_path,
                sheet_name="Yields",
                engine='openpyxl'
            )
            
            # Filter rows where column A contains GI codes
            gi_rows = df_yields[df_yields.iloc[:, 0].apply(self._is_gi_code)]
            
            if gi_rows.empty:
                raise ValueError("No GI codes found in Yields sheet")
            
            logging.info(f"Found {len(gi_rows)} rows with GI codes in Yields sheet")
            return gi_rows
            
        except Exception as e:
            logging.error(f"Error extracting Yields data: {str(e)}")
            raise
    
    @retry_with_notification()
    def extract_spread_data(self):
        """Extract rows 2-19 from Spread Calc sheet"""
        try:
            # Read the Spread Calc sheet
            df_spread = pd.read_excel(
                self.excel_path,
                sheet_name="Spread Calc",
                engine='openpyxl'
            )
            
            # Extract rows 2-19 (1-18 in 0-based index)
            spread_rows = df_spread.iloc[1:19].copy()
            
            if spread_rows.empty:
                raise ValueError("No data found in rows 2-19 of Spread Calc sheet")
            
            logging.info(f"Successfully extracted {len(spread_rows)} rows from Spread Calc sheet")
            return spread_rows
            
        except Exception as e:
            logging.error(f"Error extracting Spread Calc data: {str(e)}")
            raise
    
    def combine_data(self, yields_data, spread_data):
        """Combine data from both sheets into one DataFrame"""
        try:
            # Add source column to each DataFrame
            yields_data = yields_data.assign(Source='Yields')
            spread_data = spread_data.assign(Source='Spread Calc')
            
            # Combine the DataFrames
            combined_df = pd.concat([yields_data, spread_data], ignore_index=True)
            
            logging.info(f"Successfully combined data: {len(combined_df)} total rows")
            return combined_df
            
        except Exception as e:
            logging.error(f"Error combining data: {str(e)}")
            raise
    
    def save_data(self, df):
        """Save the extracted data to CSV"""
        try:
            output_file = Config.get_output_path('ijg') / f'ijg_bonds_{datetime.now().strftime("%Y%m%d")}.csv'
            df.to_csv(output_file, index=False)
            logging.info(f"Successfully saved data to {output_file}")
            return output_file
        except Exception as e:
            logging.error(f"Error saving data: {str(e)}")
            raise

def run_ijg_workflow() -> WorkflowResult:
    """Run the complete IJG workflow"""
    try:
        # Initialize processor
        processor = IJGDailyProcessor()
        
        # Extract data from both sheets
        yields_data = processor.extract_yields_data()
        spread_data = processor.extract_spread_data()
        
        # Combine the data
        combined_df = processor.combine_data(yields_data, spread_data)
        
        # Save to CSV
        output_file = processor.save_data(combined_df)
        
        logging.info(f"Successfully completed IJG workflow")
        return WorkflowResult(success=True, data=combined_df)
        
    except Exception as e:
        error_msg = f"Error in IJG workflow: {str(e)}"
        logging.error(error_msg)
        return WorkflowResult(success=False, error=error_msg)

if __name__ == "__main__":
    result = run_ijg_workflow()
    if result.success:
        print("IJG data preview:")
        print(result.data.head())
