"""
This module processes the collected bond data from various sources (Bloomberg, NSX, IJG)
to calculate and compile closing yields for all bonds. It runs as the final workflow
after all data collection is successful.
"""

import pandas as pd
import logging
from datetime import datetime
from pathlib import Path
from config import Config
from workflow_result import WorkflowResult
from typing import Dict

# Set up logging
logger = logging.getLogger('closing_yields_workflow')
logger.setLevel(logging.INFO)

# Create a file handler
log_file = Config.get_logs_path() / f'closing_yields_{datetime.now().strftime("%Y%m%d")}.log'
file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.INFO)

# Create a formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(file_handler)

class ClosingYieldsProcessor:
    def __init__(self, bloomberg_data: pd.DataFrame, nsx_data: pd.DataFrame, 
                 ijg_yields_data: pd.DataFrame, ijg_spread_data: pd.DataFrame):
        """
        Initialize the processor with data from all sources.
        
        Args:
            bloomberg_data: DataFrame containing Bloomberg Terminal data
            nsx_data: DataFrame containing NSX Daily Report data
            ijg_yields_data: DataFrame containing IJG Yields data
            ijg_spread_data: DataFrame containing IJG Spread data
        """
        self.bloomberg_data = bloomberg_data
        self.nsx_data = nsx_data
        self.ijg_yields_data = ijg_yields_data
        self.ijg_spread_data = ijg_spread_data
        
        # Validate input data
        self._validate_input_data()
    
    def _validate_input_data(self):
        """Validate that all required data is present and in the expected format"""
        if any(df is None for df in [self.bloomberg_data, self.nsx_data, 
                                   self.ijg_yields_data, self.ijg_spread_data]):
            raise ValueError("All data sources must be provided")
        
        # Add more specific validation as needed for each DataFrame
        logger.info("Input data validation completed successfully")
    
    def process_data(self) -> pd.DataFrame:
        """
        Process the input data to calculate closing yields.
        Creates a new DataFrame with:
        - Security (from NSX data)
        - Benchmark (from NSX data)
        - Benchmark Yield (from Bloomberg data, matched by benchmark name)
        - Spread (from IJG spread data)
        - Closing Yield (calculated as benchmark yield + spread/100)
        
        Returns:
            DataFrame containing the processed closing yields
        """
        try:
            logger.info("Starting closing yields calculation")
            
            # Create new DataFrame with Security and Benchmark from NSX data
            closing_yields_df = pd.DataFrame({
                'Security': self.nsx_data['Security'],
                'Benchmark': self.nsx_data['Benchmark']
            })
            
            # Create a mapping of bond names to yields from Bloomberg data
            bloomberg_yields = {}
            for _, row in self.bloomberg_data.iterrows():
                if pd.notna(row['Bond']) and pd.notna(row['Yield']):
                    bloomberg_yields[row['Bond']] = row['Yield']
            
            logger.info(f"Created yield mapping for {len(bloomberg_yields)} bonds from Bloomberg")
            
            # Fill in Benchmark Yield column by matching benchmark names
            closing_yields_df['Benchmark Yield'] = closing_yields_df['Benchmark'].map(bloomberg_yields)
            
            # Copy Spread column from IJG spread data
            # First create a mapping of security to spread from IJG data
            ijg_spreads = {}
            for _, row in self.ijg_spread_data.iterrows():
                if 'Security' in row and 'Spread' in row:  # Ensure columns exist
                    if pd.notna(row['Security']) and pd.notna(row['Spread']):
                        ijg_spreads[row['Security']] = row['Spread']
            
            # Add Spread column
            closing_yields_df['Spread'] = closing_yields_df['Security'].map(ijg_spreads)
            
            # Calculate Closing Yield
            closing_yields_df['Closing Yield'] = closing_yields_df.apply(
                lambda row: row['Benchmark Yield'] + (row['Spread']/100) 
                if pd.notna(row['Benchmark Yield']) and pd.notna(row['Spread']) 
                else None, 
                axis=1
            )
            
            # Log summary statistics
            logger.info(f"Processed {len(closing_yields_df)} bonds")
            logger.info(f"Found benchmark yields for {closing_yields_df['Benchmark Yield'].notna().sum()} bonds")
            logger.info(f"Found spreads for {closing_yields_df['Spread'].notna().sum()} bonds")
            logger.info(f"Calculated closing yields for {closing_yields_df['Closing Yield'].notna().sum()} bonds")
            
            # Log any missing data
            missing_benchmark_yields = closing_yields_df[closing_yields_df['Benchmark Yield'].isna()]['Security'].tolist()
            missing_spreads = closing_yields_df[closing_yields_df['Spread'].isna()]['Security'].tolist()
            
            if missing_benchmark_yields:
                logger.warning(f"Missing benchmark yields for securities: {missing_benchmark_yields}")
            if missing_spreads:
                logger.warning(f"Missing spreads for securities: {missing_spreads}")
            
            return closing_yields_df
            
        except Exception as e:
            logger.error(f"Error processing closing yields: {str(e)}")
            raise
    
    def save_results(self, df: pd.DataFrame) -> Path:
        """
        Save the processed closing yields to a CSV file.
        
        Args:
            df: DataFrame containing the processed closing yields
            
        Returns:
            Path to the saved CSV file
        """
        try:
            # Create a new directory for closing yields if it doesn't exist
            output_file = Config.get_output_path('closing_yields') / f'closing_yields_{datetime.now().strftime("%Y%m%d")}.csv'
            
            # Save to CSV
            df.to_csv(output_file, index=False)
            logger.info(f"Successfully saved closing yields to {output_file}")
            
            return output_file
            
        except Exception as e:
            logger.error(f"Error saving closing yields: {str(e)}")
            raise

def run_closing_yields_workflow(data_collector) -> WorkflowResult:
    """
    Run the closing yields workflow using collected data.
    
    Args:
        data_collector: DataCollector instance containing all collected data
        
    Returns:
        WorkflowResult containing success status and processed data
    """
    try:
        # Initialize processor with collected data
        processor = ClosingYieldsProcessor(
            bloomberg_data=data_collector.bloomberg_data,
            nsx_data=data_collector.nsx_data,
            ijg_yields_data=data_collector.ijg_yields_data,
            ijg_spread_data=data_collector.ijg_spread_data
        )
        
        # Process the data
        results_df = processor.process_data()
        
        # Save results
        output_file = processor.save_results(results_df)
        
        logger.info("Successfully completed closing yields workflow")
        return WorkflowResult(success=True, data=results_df)
        
    except Exception as e:
        error_msg = f"Error in closing yields workflow: {str(e)}"
        logger.error(error_msg)
        return WorkflowResult(success=False, error=error_msg) 