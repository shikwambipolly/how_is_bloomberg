import pandas as pd
from docx import Document
import logging
from datetime import datetime
from utils import retry_with_notification
from config import Config

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=Config.get_logs_path() / f'ijg_daily_{datetime.now().strftime("%Y%m%d")}.log'
)

class IJGDailyProcessor:
    def __init__(self):
        self.docx_path = Config.IJG_DAILY_PATH
        if not self.docx_path.exists():
            raise FileNotFoundError(f"Document not found at path: {self.docx_path}")
    
    @retry_with_notification()
    def extract_table_data(self):
        """Extract table data from page 5 of the Word document"""
        try:
            # Load the document
            doc = Document(self.docx_path)
            logging.info(f"Successfully loaded document: {self.docx_path}")
            
            # Track paragraphs to identify page 5
            # In Word, each paragraph and table contributes to the page count
            current_page = 1
            page_5_elements = []
            
            # Collect all elements (paragraphs and tables) to track page 5
            for element in doc.element.body:
                if element.tag.endswith('p'):  # Paragraph
                    if current_page == 5:
                        page_5_elements.append(('p', element))
                elif element.tag.endswith('tbl'):  # Table
                    if current_page == 5:
                        page_5_elements.append(('tbl', element))
                
                # Check for page breaks
                for child in element.iter():
                    if child.tag.endswith('br') and child.get('type') == 'page':
                        current_page += 1
                        if current_page > 5:
                            break
            
            # Find tables on page 5
            target_table = None
            for element_type, element in page_5_elements:
                if element_type == 'tbl':
                    table = doc.tables[list(doc.element.body).index(element)]
                    if table.rows and table.rows[0].cells and "Bond" in table.rows[0].cells[0].text:
                        target_table = table
                        break
            
            if not target_table:
                raise ValueError("Could not find table starting with 'Bond' on page 5")
            
            # Extract headers and data
            headers = []
            for cell in target_table.rows[0].cells:
                headers.append(cell.text.strip())
            
            # Extract rows
            data = []
            for row in target_table.rows[1:]:  # Skip header row
                row_data = []
                for cell in row.cells:
                    row_data.append(cell.text.strip())
                data.append(row_data)
            
            # Create DataFrame
            df = pd.DataFrame(data, columns=headers)
            
            logging.info(f"Successfully extracted table data from page 5 with {len(df)} rows")
            return df
            
        except Exception as e:
            logging.error(f"Error extracting table data: {str(e)}")
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

def run_ijg_workflow():
    """Run the complete IJG workflow"""
    try:
        # Initialize processor
        processor = IJGDailyProcessor()
        
        # Extract table data
        df = processor.extract_table_data()
        
        # Save to CSV
        output_file = processor.save_data(df)
        
        logging.info(f"Successfully completed IJG workflow")
        return True
        
    except Exception as e:
        logging.error(f"Error in IJG workflow: {str(e)}")
        return False

if __name__ == "__main__":
    run_ijg_workflow()
