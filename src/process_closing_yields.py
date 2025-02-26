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
                 ijg_gi_data: pd.DataFrame, ijg_gc_data: pd.DataFrame):
        """
        Initialize the processor with data from all sources.
        
        Args:
            bloomberg_data: DataFrame containing Bloomberg Terminal data
            nsx_data: DataFrame containing NSX Daily Report data
            ijg_gi_data: DataFrame containing IJG GI bonds data
            ijg_gc_data: DataFrame containing IJG GC bonds data
        """
        self.bloomberg_data = bloomberg_data
        self.nsx_data = nsx_data
        self.ijg_gi_data = ijg_gi_data
        self.ijg_gc_data = ijg_gc_data
        
        # Validate input data
        self._validate_input_data()
    
    def _validate_input_data(self):
        """Validate that all required data is present and in the expected format"""
        if any(df is None for df in [self.bloomberg_data, self.nsx_data, 
                                   self.ijg_gi_data, self.ijg_gc_data]):
            raise ValueError("All data sources must be provided")
        
        # Add more specific validation as needed for each DataFrame
        logger.info("Input data validation completed successfully")
    
    def process_data(self) -> pd.DataFrame:
        """
        Process the input data to calculate closing yields.
        
        New priority order for yields/spreads:
        1. For all bonds (GC and GI), first check IJG daily file with today's date:
           - For GC bonds: Use Spread if "Date of last event" column = today
           - For GI bonds: Use Yield if "Date" column = today
        2. If not available with today's date, check NSX data:
           - If Deals >= 1 AND Nominal >= 1,000,000, use NSX data
        3. If neither of the above, use IJG data regardless of date
        
        Returns:
            DataFrame containing the processed closing yields with spread and source information
        """
        try:
            logger.info("Starting closing yields calculation")
            
            # Get today's date in the format expected in the IJG files
            today_date = datetime.now().strftime("%Y-%m-%d")
            logger.info(f"Today's date for comparison: {today_date}")
            
            # Print NSX data columns to debug
            logger.info(f"NSX data columns: {list(self.nsx_data.columns)}")
            
            # Create new DataFrame with required columns from NSX data
            closing_yields_df = pd.DataFrame({
                'Security': self.nsx_data['Security'],
                'Benchmark': self.nsx_data['Benchmark'],
                'NSX_Deals': self.nsx_data.get('Deals', pd.Series([0] * len(self.nsx_data))),
                'NSX_Nominal': self.nsx_data.get('Nominal', pd.Series([0] * len(self.nsx_data))),
                'NSX_Yield': self.nsx_data.get('Mark To (Yield)', pd.Series([None] * len(self.nsx_data))),  # NSX yield if available
                'NSX_Spread': self.nsx_data.get('Spread', pd.Series([None] * len(self.nsx_data)))  # NSX spread if available
            })
            
            # Log NSX data sample
            logger.info(f"Sample of NSX data with Spread column:\n{self.nsx_data[['Security', 'Spread']].head() if 'Spread' in self.nsx_data.columns else 'Spread column not found in NSX data'}")
            
            # Create a mapping of bond names to yields from Bloomberg data
            bloomberg_yields = {}
            for _, row in self.bloomberg_data.iterrows():
                if pd.notna(row['Bond']) and pd.notna(row['Yield']):
                    bloomberg_yields[row['Bond']] = row['Yield']
            
            logger.info(f"Created yield mapping for {len(bloomberg_yields)} bonds from Bloomberg")
            
            # Fill in Benchmark Yield column by matching benchmark names
            closing_yields_df['Benchmark Yield'] = closing_yields_df['Benchmark'].map(bloomberg_yields)
            
            # Check if IJG GC data has "Date of last event" column
            has_gc_date_column = 'Date of last event' in self.ijg_gc_data.columns
            logger.info(f"IJG GC data has 'Date of last event' column: {has_gc_date_column}")
            
            # Check if IJG GI data has "Date" column (previously looking for "WAIT" column)
            has_gi_date_column = 'Date' in self.ijg_gi_data.columns
            logger.info(f"IJG GI data has 'Date' column: {has_gi_date_column}")
            
            # Create a mapping of government bonds to spreads from IJG GC data
            # Include date information if available
            ijg_spreads = {}
            ijg_gc_today_spreads = {}  # For spreads with today's date
            
            for _, row in self.ijg_gc_data.iterrows():
                if 'Government' in row.index and 'Spread' in row.index:
                    if pd.notna(row['Government']) and pd.notna(row['Spread']):
                        # Store in regular mapping
                        ijg_spreads[row['Government']] = row['Spread']
                        
                        # Check if today's date
                        if has_gc_date_column and pd.notna(row.get('Date of last event')):
                            try:
                                row_date = pd.to_datetime(row['Date of last event']).strftime("%Y-%m-%d")
                                if row_date == today_date:
                                    ijg_gc_today_spreads[row['Government']] = row['Spread']
                            except Exception as e:
                                logger.warning(f"Error parsing date for GC bond {row['Government']}: {str(e)}")
            
            logger.info(f"Created spread mapping for {len(ijg_spreads)} bonds from IJG GC data")
            logger.info(f"Found {len(ijg_gc_today_spreads)} GC bonds with today's date")
            
            # Create mapping for GI bonds from IJG GI data
            gi_yields = {}
            gi_today_yields = {}  # For yields with today's date
            
            for _, row in self.ijg_gi_data.iterrows():
                if pd.notna(row.iloc[0]) and pd.notna(row['PX_Last']):  # First column contains bond name
                    # Store in regular mapping
                    gi_yields[row.iloc[0]] = row['PX_Last']
                    
                    # Check if today's date - use "Date" column instead of "WAIT"
                    if has_gi_date_column and pd.notna(row.get('Date')):
                        try:
                            row_date = pd.to_datetime(row['Date']).strftime("%Y-%m-%d")
                            if row_date == today_date:
                                gi_today_yields[row.iloc[0]] = row['PX_Last']
                        except Exception as e:
                            logger.warning(f"Error parsing date for GI bond {row.iloc[0]}: {str(e)}")
            
            logger.info(f"Created yield mapping for {len(gi_yields)} GI bonds from IJG GI data")
            logger.info(f"Found {len(gi_today_yields)} GI bonds with today's date")
            
            # Add a column to track data source
            closing_yields_df['Source'] = "Unknown"
            
            # Create a dictionary to store sources for each security
            sources = {}
            
            # Calculate Closing Yield based on the new priority order
            def calculate_closing_yield(row):
                security = row['Security']
                has_active_trading = False
                
                # Check if NSX data is available for active trading check
                if pd.notna(row['NSX_Deals']) and pd.notna(row['NSX_Nominal']):
                    has_active_trading = (row['NSX_Deals'] >= 1) and (row['NSX_Nominal'] >= 1000000)
                
                # Check if it's a GI bond (no benchmark)
                if pd.isna(row['Benchmark']):
                    # Priority 1: Check if data available with today's date
                    if security in gi_today_yields:
                        logger.info(f"Using IJG yield with today's date for GI bond {security}: {gi_today_yields[security]}")
                        sources[security] = "IJG GI Data (Today's Date)"
                        return gi_today_yields[security]
                    # Priority 2: Check active trading
                    elif has_active_trading and pd.notna(row['NSX_Yield']):
                        logger.info(f"Using NSX yield for actively traded GI bond {security}: {row['NSX_Yield']}")
                        sources[security] = "NSX (Active Trading)"
                        return row['NSX_Yield']
                    # Priority 3: Use regular IJG yield if available
                    elif security in gi_yields:
                        logger.info(f"Using IJG yield for GI bond {security}: {gi_yields[security]}")
                        sources[security] = "IJG GI Data"
                        return gi_yields[security]
                    else:
                        logger.warning(f"No yield data found for GI bond {security}")
                        sources[security] = "No Data Found"
                        return None
                # For GC bonds (with benchmark)
                else:
                    # Get benchmark yield
                    benchmark_yield = row['Benchmark Yield']
                    if pd.isna(benchmark_yield):
                        logger.warning(f"Missing benchmark yield for {security}, cannot calculate closing yield")
                        sources[security] = "Missing Benchmark Yield"
                        return None
                    
                    # Priority 1: Check if data available with today's date
                    if security in ijg_gc_today_spreads and pd.notna(ijg_gc_today_spreads[security]):
                        ijg_today_spread = ijg_gc_today_spreads[security]
                        logger.info(f"Using IJG spread with today's date for GC bond {security}: {ijg_today_spread} bps")
                        sources[security] = "IJG GC Data (Today's Date)"
                        return benchmark_yield + (ijg_today_spread/100)
                    # Priority 2: Check active trading
                    elif has_active_trading:
                        # Check if NSX spread is available
                        if pd.notna(row['NSX_Spread']):
                            nsx_spread = row['NSX_Spread']
                            logger.info(f"Using spread from NSX for actively traded GC bond {security}: {nsx_spread} bps")
                            sources[security] = f"NSX Spread: {nsx_spread} bps"
                            return benchmark_yield + (nsx_spread/100)
                        # Check if NSX yield is available to calculate spread
                        elif pd.notna(row['NSX_Yield']):
                            # Calculate spread from yield
                            nsx_yield = row['NSX_Yield']
                            calculated_spread = (nsx_yield - benchmark_yield) * 100
                            logger.info(f"Calculated spread from NSX yield for actively traded GC bond {security}: {calculated_spread} bps")
                            sources[security] = f"NSX Calculated Spread: {calculated_spread:.2f} bps"
                            return nsx_yield
                    # Priority 3: Use regular IJG spread if available
                    elif security in ijg_spreads and pd.notna(ijg_spreads[security]):
                        ijg_spread = ijg_spreads[security]
                        logger.info(f"Using IJG spread for GC bond {security}: {ijg_spread} bps")
                        sources[security] = "IJG GC Data"
                        return benchmark_yield + (ijg_spread/100)
                    else:
                        logger.warning(f"No spread found for GC bond {security}")
                        sources[security] = "No Spread Found"
                        return None
            
            # Calculate closing yields
            closing_yields_df['Closing Yield'] = closing_yields_df.apply(calculate_closing_yield, axis=1)
            
            # Update the Source column using the sources dictionary after calculation
            for idx, row in closing_yields_df.iterrows():
                security = row['Security']
                if security in sources:
                    closing_yields_df.at[idx, 'Source'] = sources[security]
                    logger.debug(f"Setting source for {security} to: {sources[security]}")
            
            # Log the first few rows to check Source values
            logger.info(f"Preview of closing yields with Source values:\n{closing_yields_df[['Security', 'Source']].head()}")
            
            # Calculate spread in basis points for GC bonds
            def calculate_spread_bps(row):
                if pd.notna(row['Benchmark']) and pd.notna(row['Benchmark Yield']) and pd.notna(row['Closing Yield']):
                    # For GC bonds with active trading, get the spread directly
                    if "NSX Spread:" in str(row['Source']):
                        # Extract the numeric value from the source string (e.g., "NSX Spread: 123.45 bps")
                        try:
                            return float(str(row['Source']).split(':')[1].strip().split(' ')[0])
                        except (IndexError, ValueError) as e:
                            logger.warning(f"Error extracting spread from source for {row['Security']}: {str(e)}")
                            # Fallback to calculating spread if extraction fails
                            return (row['Closing Yield'] - row['Benchmark Yield']) * 100
                    elif "NSX Calculated Spread:" in str(row['Source']):
                        try:
                            return float(str(row['Source']).split(':')[1].strip().split(' ')[0])
                        except (IndexError, ValueError) as e:
                            logger.warning(f"Error extracting calculated spread from source for {row['Security']}: {str(e)}")
                            # Fallback to calculating spread
                            return (row['Closing Yield'] - row['Benchmark Yield']) * 100
                    else:
                        # Calculate spread for other GC bonds
                        return (row['Closing Yield'] - row['Benchmark Yield']) * 100
                return None
            
            # Apply the spread calculation after Source has been updated
            closing_yields_df['Spread (bps)'] = closing_yields_df.apply(calculate_spread_bps, axis=1)
            
            # Log summary statistics
            total_bonds = len(closing_yields_df)
            gi_bonds = closing_yields_df['Benchmark'].isna().sum()
            gc_bonds = closing_yields_df['Benchmark'].notna().sum()
            active_trading_count = ((closing_yields_df['NSX_Deals'] >= 1) & 
                                   (closing_yields_df['NSX_Nominal'] >= 1000000)).sum()
            ijg_today_count = closing_yields_df['Source'].str.contains("Today's Date").sum()
            
            logger.info(f"Processed {total_bonds} bonds in total:")
            logger.info(f"  - {gi_bonds} GI bonds")
            logger.info(f"  - {gc_bonds} GC bonds")
            logger.info(f"  - {ijg_today_count} bonds with today's date in IJG data (Priority 1)")
            logger.info(f"  - {active_trading_count} bonds with active trading (Deals >= 1 AND Nominal >= 1,000,000) (Priority 2)")
            logger.info(f"Found closing yields for {closing_yields_df['Closing Yield'].notna().sum()} bonds")
            
            # Log any missing data
            missing_yields = closing_yields_df[closing_yields_df['Closing Yield'].isna()]['Security'].tolist()
            if missing_yields:
                logger.warning(f"Missing closing yields for securities: {missing_yields}")
            
            # Log distribution of source values
            source_counts = closing_yields_df['Source'].value_counts()
            logger.info(f"Distribution of Source values:\n{source_counts}")
            
            # Remove temporary columns used for calculation
            final_df = closing_yields_df.drop(columns=['NSX_Deals', 'NSX_Nominal', 'NSX_Yield', 'NSX_Spread'])
            
            return final_df
            
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
            # Save to today's output directory
            output_file = Config.get_output_path() / f'closing_yields_{datetime.now().strftime("%Y%m%d")}.csv'
            
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
    
    Priority order for determining closing yields:
    1. For all bonds (GC and GI), first check IJG daily file with today's date:
       - For GC bonds: Use Spread if "Date of last event" column = today
       - For GI bonds: Use Yield if "Date" column = today
    2. If not available with today's date, check NSX data:
       - If Deals >= 1 AND Nominal >= 1,000,000, use NSX data
    3. If neither of the above, use IJG data regardless of date
    
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
            ijg_gi_data=data_collector.ijg_gi_data,
            ijg_gc_data=data_collector.ijg_gc_data
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