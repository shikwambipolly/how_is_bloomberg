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
from decimal import Decimal  # Import Decimal for precise decimal handling

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
        
    def find_last_data_row(self, sheet):
        """
        Find the last row with data in the Excel sheet.
        
        Args:
            sheet: Excel worksheet to analyze
            
        Returns:
            int: The last row number containing data
        """
        # Start from the top and find the last row with data in column A (date column)
        last_row = 2  # Minimum is row 2 (header is row 1)
        
        for row in range(3, sheet.max_row + 1):
            if sheet.cell(row=row, column=1).value is not None:
                last_row = row
        
        logger.info(f"Found last data row at position {last_row}")
        return last_row
    
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
            
            # Map securities to closing yields
            securities_to_yields = {}
            for _, row in self.closing_yields_data.iterrows():
                if pd.notna(row['Security']) and pd.notna(row['Closing Yield']):
                    # Convert to Decimal for precise representation
                    try:
                        securities_to_yields[row['Security']] = Decimal(str(row['Closing Yield']))
                    except:
                        # Fallback to original value if conversion fails
                        securities_to_yields[row['Security']] = row['Closing Yield']
            
            logger.info(f"Mapped {len(securities_to_yields)} securities to their closing yields")
            
            # Find the last row with data
            last_row = self.find_last_data_row(input_sheet)
            
            # Find the template row to copy formatting from
            # Using last_row as our formatting template
            template_row = last_row
            logger.info(f"Using row {template_row} as formatting template")
            
            # Next row is immediately after the last data row
            next_row = last_row + 1
            logger.info(f"Adding new data at row {next_row}")
            
            # Get today's date - Using today's date, but the formatting will be adjusted
            today_date = datetime.now()
            
            # Write today's date in the first column and copy formatting from template
            new_date_cell = input_sheet.cell(row=next_row, column=1)
            template_date_cell = input_sheet.cell(row=template_row, column=1)
            
            # Set the value but keep the existing number format
            new_date_cell.value = today_date
            
            # Copy the number format for date
            if template_date_cell.number_format:
                new_date_cell.number_format = template_date_cell.number_format
                logger.info(f"Copied date number format: {template_date_cell.number_format}")
            
            # Copy other formatting properties from template cell
            self._copy_cell_format(template_date_cell, new_date_cell)
            
            # Match securities and write closing yields
            securities_written = 0
            for security, col in securities.items():
                if security in securities_to_yields:
                    # Get the template cell to copy formatting from 
                    template_cell = input_sheet.cell(row=template_row, column=col)
                    
                    # Get the cell we want to write to
                    new_cell = input_sheet.cell(row=next_row, column=col)
                    
                    # Get the value as Decimal to preserve precision
                    yield_value = securities_to_yields[security]
                    
                    # Set the value in the Excel cell
                    # Preserve the exact value (don't convert to string and back)
                    if isinstance(yield_value, Decimal):
                        new_cell.value = float(yield_value)
                    else:
                        new_cell.value = yield_value
                    
                    # Copy number format and other properties from template cell
                    if template_cell.number_format:
                        new_cell.number_format = template_cell.number_format
                    
                    # Copy other formatting properties
                    self._copy_cell_format(template_cell, new_cell)
                    
                    securities_written += 1
                else:
                    logger.warning(f"Security {security} not found in closing yields data")
            
            logger.info(f"Added closing yields for {securities_written} securities")
            
            # Check for any securities in our data that were not found in the Excel sheet
            missing_securities = [sec for sec in securities_to_yields.keys() if sec not in securities]
            if missing_securities:
                logger.warning(f"These securities were in our data but not in Excel: {missing_securities}")
            
            # Apply formula extension to the GC sheet
            logger.info("Now extending formulas in the GC sheet")
            self.extend_gc_sheet_formulas(workbook)
            
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
    
    def _copy_cell_format(self, source_cell, target_cell):
        """
        Copy formatting from source cell to target cell.
        This includes font, border, alignment, fill, etc.
        
        Args:
            source_cell: The cell to copy formatting from
            target_cell: The cell to apply formatting to
        """
        try:
            # Copy font
            if source_cell.font:
                target_cell.font = source_cell.font
            
            # Copy border
            if source_cell.border:
                target_cell.border = source_cell.border
            
            # Copy alignment
            if source_cell.alignment:
                target_cell.alignment = source_cell.alignment
            
            # Copy fill
            if source_cell.fill:
                target_cell.fill = source_cell.fill
            
            # Copy protection
            if source_cell.protection:
                target_cell.protection = source_cell.protection
            
            logger.debug(f"Successfully copied cell formatting from {source_cell.coordinate} to {target_cell.coordinate}")
        except Exception as e:
            logger.warning(f"Error copying cell format: {str(e)}")
            # Don't raise the exception - it's not critical if formatting fails
    
    def extend_gc_sheet_formulas(self, workbook):
        """
        Find the GC sheet, identify the last two rows with data, and use Excel's
        autofill functionality to extend the formulas down one more row.
        
        Args:
            workbook: The openpyxl workbook object
        """
        try:
            # Check if GC sheet exists
            if "GC" not in workbook.sheetnames:
                logger.warning("GC sheet not found in workbook")
                return
                
            gc_sheet = workbook["GC"]
            logger.info("Successfully opened 'GC' sheet")
            
            # Find the last row with data (starts from A column)
            last_row = 2  # Default to row 2 if no data found
            for row in range(2, gc_sheet.max_row + 1):
                if gc_sheet.cell(row=row, column=1).value is not None:
                    last_row = row
            
            if last_row < 3:  # Need at least 2 rows to extend formulas
                logger.warning("Not enough data rows in GC sheet to extend formulas")
                return
                
            # The two source rows for the formula patterns
            source_row1 = last_row - 1
            source_row2 = last_row
            
            # Target row where we want to extend the formulas
            target_row = last_row + 1
            
            logger.info(f"Extending formulas from rows {source_row1} and {source_row2} to row {target_row}")
            
            # First, we'll find all columns with formulas in the source rows
            formula_columns = []
            for col in range(1, gc_sheet.max_column + 1):
                cell1 = gc_sheet.cell(row=source_row1, column=col)
                cell2 = gc_sheet.cell(row=source_row2, column=col)
                
                # Check if either of the source cells has a formula
                if isinstance(cell1.value, str) and cell1.value.startswith('='):
                    formula_columns.append(col)
                elif isinstance(cell2.value, str) and cell2.value.startswith('='):
                    formula_columns.append(col)
            
            # Now copy non-formula values directly (like dates)
            for col in range(1, gc_sheet.max_column + 1):
                if col not in formula_columns:
                    # Copy the value and format from the last row
                    source_cell = gc_sheet.cell(row=source_row2, column=col)
                    target_cell = gc_sheet.cell(row=target_row, column=col)
                    
                    # For dates, increment by one day
                    if isinstance(source_cell.value, datetime):
                        target_cell.value = source_cell.value + pd.Timedelta(days=1)
                    else:
                        target_cell.value = source_cell.value
                        
                    self._copy_cell_format(source_cell, target_cell)
            
            # Now extend formulas using Excel's pattern recognition
            for col in formula_columns:
                cell1 = gc_sheet.cell(row=source_row1, column=col)
                cell2 = gc_sheet.cell(row=source_row2, column=col)
                target_cell = gc_sheet.cell(row=target_row, column=col)
                
                # Check if we can detect a pattern in the formulas
                if isinstance(cell1.value, str) and isinstance(cell2.value, str):
                    # Both cells have formulas, try to detect the pattern and extend it
                    formula1 = cell1.value
                    formula2 = cell2.value
                    
                    # Simple pattern: look for row references that changed
                    new_formula = self._extend_formula_pattern(formula1, formula2, target_row)
                    if new_formula:
                        target_cell.value = new_formula
                        # Copy formatting from the last row
                        self._copy_cell_format(cell2, target_cell)
                    else:
                        # If pattern detection fails, just copy the last formula
                        target_cell.value = formula2
                        self._copy_cell_format(cell2, target_cell)
                elif isinstance(cell2.value, str) and cell2.value.startswith('='):
                    # Only the last row has a formula, just copy and adjust it
                    formula = cell2.value
                    # Try to adjust row references
                    row_diff = target_row - source_row2
                    new_formula = self._adjust_row_references(formula, row_diff)
                    target_cell.value = new_formula
                    self._copy_cell_format(cell2, target_cell)
            
            logger.info(f"Successfully extended formulas to row {target_row} in GC sheet")
            
        except Exception as e:
            logger.error(f"Error extending GC sheet formulas: {str(e)}")
            # Don't raise the exception - we don't want to fail the entire workflow
    
    def _extend_formula_pattern(self, formula1, formula2, target_row):
        """
        Try to detect a pattern between two formulas and extend it to create a new formula.
        This is a simplified implementation that handles common patterns in Excel formulas.
        
        Args:
            formula1: Formula from the first source row
            formula2: Formula from the second source row
            target_row: Target row number for the new formula
            
        Returns:
            str: The extended formula for the target row, or None if pattern can't be detected
        """
        try:
            # First, check if formulas are identical
            if formula1 == formula2:
                return formula2  # No pattern to extend
            
            # Look for simple row reference increments
            # In Excel formulas, rows are often referenced as A1, B1, etc.
            # When moving down, these change to A2, B2, etc.
            
            # This is a simplistic approach - it assumes row references increment by 1
            # and that the pattern continues
            row_diff = target_row - (target_row - 1)  # Always 1 in our case
            
            # Try to detect the difference between formula1 and formula2
            # and apply the same difference to get formula3
            
            # Simple case: formulas with cell references like A1, B2, etc.
            # We'll look for cell references that changed by exactly one row
            import re
            
            # Find all cell references in both formulas
            # Example pattern: A1, B12, AA123, etc.
            cell_ref_pattern = r'([A-Z]+)(\d+)'
            refs1 = re.findall(cell_ref_pattern, formula1)
            refs2 = re.findall(cell_ref_pattern, formula2)
            
            if len(refs1) == len(refs2):
                # Map old references to new ones
                replacements = {}
                
                for (col1, row1), (col2, row2) in zip(refs1, refs2):
                    if col1 == col2 and int(row2) - int(row1) == row_diff:
                        # This reference follows the pattern
                        new_row = int(row2) + row_diff
                        replacements[f"{col2}{row2}"] = f"{col2}{new_row}"
                
                # Apply replacements to formula2
                new_formula = formula2
                for old_ref, new_ref in replacements.items():
                    new_formula = new_formula.replace(old_ref, new_ref)
                
                return new_formula
            
            # If we couldn't detect a pattern, return None
            return None
            
        except Exception as e:
            logger.warning(f"Error extending formula pattern: {str(e)}")
            return None
    
    def _adjust_row_references(self, formula, row_diff):
        """
        Adjust row references in a formula by the specified difference.
        
        Args:
            formula: The formula to adjust
            row_diff: The number of rows to add to each reference
            
        Returns:
            str: The adjusted formula
        """
        try:
            import re
            
            # Find all cell references in the formula
            cell_ref_pattern = r'([A-Z]+)(\d+)'
            refs = re.findall(cell_ref_pattern, formula)
            
            # Create new formula with adjusted references
            new_formula = formula
            for col, row in refs:
                old_ref = f"{col}{row}"
                new_row = int(row) + row_diff
                new_ref = f"{col}{new_row}"
                new_formula = new_formula.replace(old_ref, new_ref)
            
            return new_formula
            
        except Exception as e:
            logger.warning(f"Error adjusting row references: {str(e)}")
            return formula  # Return original formula if adjustment fails
    
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