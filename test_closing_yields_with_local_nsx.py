#!/usr/bin/env python
"""
Test script for closing yields calculation with a local NSX test file.

This script:
1. Fetches real Bloomberg terminal data
2. Fetches real IJG daily data
3. Uses a local NSX test file (manually created)
4. Processes the data through the ClosingYieldsProcessor
5. Displays detailed results

Usage:
    python test_closing_yields_with_local_nsx.py
"""

import os
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime

# Import project modules
from src.get_yields_terminal import run_terminal_workflow
from src.get_IJG_daily import run_ijg_workflow
from src.get_nsx_email import NSXEmailProcessor
from src.process_closing_yields import ClosingYieldsProcessor
from src.config import Config

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('test_closing_yields')

def ensure_output_directory():
    """Ensure output directory exists"""
    try:
        Config.get_output_path()
        Config.get_logs_path()
    except Exception as e:
        logger.error(f"Error creating directories: {str(e)}")
        raise

def test_closing_yields():
    """Test the closing yields calculation with custom NSX data"""
    try:
        # Validate configuration
        Config.validate()
        
        # Ensure directories exist
        ensure_output_directory()
        
        # Step 1: Get real Bloomberg data
        logger.info("Fetching Bloomberg Terminal data...")
        bloomberg_result = run_terminal_workflow()
        if not bloomberg_result.success:
            logger.error("Failed to fetch Bloomberg data")
            return
        logger.info(f"Successfully fetched Bloomberg data with {len(bloomberg_result.data)} rows")
        
        # Step 2: Get real IJG data
        logger.info("Fetching IJG Daily data...")
        ijg_result = run_ijg_workflow()
        if not ijg_result.success:
            logger.error("Failed to fetch IJG data")
            return
        logger.info(f"Successfully fetched IJG yields data with {len(ijg_result.data['yields'])} rows")
        logger.info(f"Successfully fetched IJG spread data with {len(ijg_result.data['spread'])} rows")
        
        # Step 3: Load local NSX test file
        # Update this path to your test file location
        test_nsx_file = Path("test_data/nsx_test_data.xlsx")  
        
        if not test_nsx_file.exists():
            logger.error(f"NSX test file not found: {test_nsx_file}")
            logger.info("Creating test_data directory...")
            os.makedirs(test_nsx_file.parent, exist_ok=True)
            logger.error(f"Please create a test NSX file at {test_nsx_file} with appropriate columns")
            logger.info("Required columns: Security, Benchmark, Deals, Nominal, Mark To (Yield)")
            return
        
        logger.info(f"Processing local NSX test file: {test_nsx_file}")
        nsx_processor = NSXEmailProcessor()
        try:
            nsx_data = nsx_processor.process_bonds_data(test_nsx_file)
            logger.info(f"Successfully processed NSX test data with {len(nsx_data)} rows")
        except Exception as e:
            logger.error(f"Error processing NSX test file: {str(e)}")
            return
        
        # Step 4: Run the ClosingYieldsProcessor
        logger.info("Running ClosingYieldsProcessor with test data...")
        processor = ClosingYieldsProcessor(
            bloomberg_data=bloomberg_result.data,
            nsx_data=nsx_data,
            ijg_yields_data=ijg_result.data['yields'],
            ijg_spread_data=ijg_result.data['spread']
        )
        
        # Process the data
        results = processor.process_data()
        
        # Step 5: Display results
        logger.info("Closing Yields Results:")
        logger.info(f"Total bonds processed: {len(results)}")
        logger.info(f"Bonds with closing yields: {results['Closing Yield'].notna().sum()}")
        
        # Save results to CSV for inspection
        output_file = Config.get_output_path() / f'test_closing_yields_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        results.to_csv(output_file, index=False)
        logger.info(f"Saved results to {output_file}")
        
        # Display first few rows
        print("\nResults Preview:")
        print(results.head(10))
        
        # Display bonds with active trading
        active_trading = nsx_data[
            (nsx_data['Deals'] >= 1) & 
            (nsx_data['Nominal'] >= 1000000)
        ]['Security'].tolist()
        
        if active_trading:
            print("\nBonds with active trading:")
            print(active_trading)
            
            # Check if active trading bonds have closing yields
            active_results = results[results['Security'].isin(active_trading)]
            print("\nClosing yields for actively traded bonds:")
            print(active_results[['Security', 'Benchmark', 'Benchmark Yield', 'Closing Yield']])
        else:
            print("\nNo bonds with active trading found in test data")
        
        return results
        
    except Exception as e:
        logger.error(f"Error in test: {str(e)}")
        raise

if __name__ == "__main__":
    print("Starting closing yields test with local NSX file...")
    test_results = test_closing_yields()
    print("Test completed.") 