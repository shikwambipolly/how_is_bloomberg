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
    python src/test_closing_yields_with_local_nsx.py
"""

import os
import logging
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

# Import project modules - adjusted for src folder location
from get_yields_terminal import run_terminal_workflow
from get_IJG_daily import run_ijg_workflow
from get_nsx_email import NSXEmailProcessor
from process_closing_yields import ClosingYieldsProcessor
from config import Config

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
        # Update this path to your test file location - adjusted for src folder
        test_nsx_file = Path("report.xlsx")  
        
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
        
        # Create IJG spreads dictionary for adding to results
        ijg_spreads = {}
        for _, row in ijg_result.data['spread'].iterrows():
            if 'Government' in row.index and 'Spread' in row.index:
                if pd.notna(row['Government']) and pd.notna(row['Spread']):
                    ijg_spreads[row['Government']] = row['Spread']
        
        # Add Spread column to results by calculating it from yields
        # For each row, we'll calculate and store the spread that was used
        def determine_spread(row):
            security = row['Security']
            benchmark_yield = row['Benchmark Yield']
            closing_yield = row['Closing Yield']
            
            # Skip rows without closing yields or benchmark yields
            if pd.isna(closing_yield) or pd.isna(benchmark_yield):
                return np.nan
            
            # Calculate spread in basis points
            return round((closing_yield - benchmark_yield) * 100, 2)
        
        # Copy results to avoid modifying the original
        enhanced_results = results.copy()
        # Calculate spread for GC bonds (those with benchmarks)
        enhanced_results['Spread (bps)'] = enhanced_results.apply(
            lambda row: determine_spread(row) if pd.notna(row['Benchmark']) else np.nan, 
            axis=1
        )
        
        # Add Source column to indicate where the yield/spread came from
        def determine_source(row):
            security = row['Security']
            has_benchmark = pd.notna(row['Benchmark'])
            
            # Check active trading criteria in original NSX data
            is_active = False
            nsx_spread = None
            if security in nsx_data['Security'].values:
                nsx_row = nsx_data[nsx_data['Security'] == security].iloc[0]
                if ('Deals' in nsx_row.index and 'Nominal' in nsx_row.index and 
                    nsx_row['Deals'] >= 1 and nsx_row['Nominal'] >= 1000000):
                    is_active = True
                    # Get the NSX spread if available
                    if 'Spread' in nsx_row.index:
                        nsx_spread = nsx_row['Spread']
            
            if has_benchmark:  # GC bonds
                if is_active and nsx_spread is not None:
                    return f"NSX Spread: {nsx_spread} bps"
                else:
                    return "IJG Spread Data"
            else:  # GI bonds
                if is_active:
                    return "NSX (Active Trading)"
                else:
                    return "IJG Yields Data"
        
        enhanced_results['Source'] = enhanced_results.apply(determine_source, axis=1)
        
        # Step 5: Display results
        logger.info("Closing Yields Results:")
        logger.info(f"Total bonds processed: {len(enhanced_results)}")
        logger.info(f"Bonds with closing yields: {enhanced_results['Closing Yield'].notna().sum()}")
        
        # Save results to CSV for inspection
        output_file = Config.get_output_path() / f'test_closing_yields_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        enhanced_results.to_csv(output_file, index=False)
        logger.info(f"Saved results to {output_file}")
        
        # Display first few rows
        print("\nResults Preview:")
        print(enhanced_results[['Security', 'Benchmark', 'Benchmark Yield', 'Spread (bps)', 'Closing Yield', 'Source']].head(10))
        
        # Display bonds with active trading
        active_trading = nsx_data[
            (nsx_data['Deals'] >= 1) & 
            (nsx_data['Nominal'] >= 1000000)
        ]['Security'].tolist()
        
        if active_trading:
            print("\nBonds with active trading:")
            print(active_trading)
            
            # Check if active trading bonds have closing yields
            active_results = enhanced_results[enhanced_results['Security'].isin(active_trading)]
            print("\nClosing yields for actively traded bonds:")
            print(active_results[['Security', 'Benchmark', 'Benchmark Yield', 'Spread (bps)', 'Closing Yield', 'Source']])
        else:
            print("\nNo bonds with active trading found in test data")
        
        return enhanced_results
        
    except Exception as e:
        logger.error(f"Error in test: {str(e)}")
        raise

if __name__ == "__main__":
    print("Starting closing yields test with local NSX file...")
    test_results = test_closing_yields()
    print("Test completed.") 