"""
This module connects to the Bloomberg Terminal and fetches real-time bond yield data.
It reads a list of bonds from a configuration file and retrieves their current
Yield to Maturity (YTM) values for both Bid and Ask prices.

The data is saved to a CSV file with today's date in the filename.
"""

import json  # Library for reading JSON files
import blpapi  # Bloomberg Terminal API
import pandas as pd  # Library for data manipulation and analysis
from datetime import datetime  # Library for working with dates and times
import logging  # Library for creating log files
from utils import retry_with_notification  # Custom retry mechanism
from config import Config  # Project configuration settings
from workflow_result import WorkflowResult  # Custom class for workflow results

# Set up logging to track the program's execution
logging.basicConfig(
    level=logging.INFO,  # Log all information messages and above
    format='%(asctime)s - %(levelname)s - %(message)s',  # Include timestamp, level, and message
    filename=Config.get_logs_path() / f'bloomberg_terminal_yields_{datetime.now().strftime("%Y%m%d")}.log'
)

@retry_with_notification()  # Retry this operation if it fails
def init_bloomberg_terminal():
    """
    Initialize a connection to the Bloomberg Terminal.
    This function sets up the connection parameters and establishes
    a session with the Terminal's data service.
    
    Returns:
        blpapi.Session: An active session with the Bloomberg Terminal
        
    Raises:
        ConnectionError: If unable to connect to the Terminal or open the data service
    """
    try:
        # Set up connection parameters
        session_options = blpapi.SessionOptions()
        session_options.setServerHost(Config.BLOOMBERG_HOST)
        session_options.setServerPort(Config.BLOOMBERG_PORT)
        
        # Create and start a new session
        session = blpapi.Session(session_options)
        
        # Attempt to start the session
        if not session.start():
            logging.error("Failed to start session. Make sure Bloomberg Terminal is running.")
            raise ConnectionError("Could not start Bloomberg Terminal session")
        
        # Open the reference data service
        if not session.openService("//blp/refdata"):
            logging.error("Failed to open //blp/refdata service")
            raise ConnectionError("Could not open //blp/refdata service")
        
        return session
    
    except Exception as e:
        logging.error(f"Error initializing Bloomberg Terminal session: {str(e)}")
        raise

@retry_with_notification()  # Retry this operation if it fails
def get_bond_yields(session, bonds):
    """
    Fetch yield values for a list of bonds from the Bloomberg Terminal.
    For each bond, retrieves both Bid and Ask Yield to Maturity (YTM) values.
    
    Args:
        session: Active Bloomberg Terminal session
        bonds: List of bond configurations containing IDs and names
        
    Returns:
        list: List of dictionaries containing bond data:
            - Bond: Name of the bond
            - Bloomberg_ID: Bloomberg identifier
            - Yield_Bid: YTM Bid value
            - Yield_Ask: YTM Ask value
            - Timestamp: When the data was collected
    """
    try:
        # Get the reference data service
        refdata_service = session.getService("//blp/refdata")
        
        # Create a new data request
        request = refdata_service.createRequest("ReferenceDataRequest")
        
        # Add each bond's ID to the request
        for bond in bonds:
            request.append("securities", bond['ID'])
        
        # Specify which yield values we want
        request.append("fields", "YLD_YTM_BID")  # Yield to Maturity (Bid)
        request.append("fields", "YLD_YTM_ASK")  # Yield to Maturity (Ask)
        
        logging.info(f"Sending request for {len(bonds)} bonds")
        
        # Send the request to Bloomberg
        session.sendRequest(request)
        
        # Process the response
        results = []
        while True:
            event = session.nextEvent(500)  # Wait up to 500ms for response
            
            if event.eventType() == blpapi.Event.RESPONSE:
                for msg in event:
                    security_data = msg.getElement("securityData")
                    
                    # Process each bond's data
                    for i in range(security_data.numValues()):
                        security = security_data.getValue(i)
                        ticker = security.getElement("security").getValue()
                        field_data = security.getElement("fieldData")
                        
                        # Find the bond's name from our configuration
                        bond_name = next((bond['Bond'] for bond in bonds if bond['ID'] == ticker), None)
                        
                        try:
                            # Get yield values
                            yield_bid = field_data.getElement("YLD_YTM_BID").getValue()
                            yield_ask = field_data.getElement("YLD_YTM_ASK").getValue()
                        except Exception as e:
                            yield_bid = None
                            yield_ask = None
                            logging.warning(f"Could not get yield for {ticker}: {str(e)}")
                        
                        # Store the results
                        results.append({
                            'Bond': bond_name,
                            'Bloomberg_ID': ticker,
                            'Yield_Bid': yield_bid,
                            'Yield_Ask': yield_ask,
                            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        })
                
                break  # Exit after processing the response
                
        return results
    
    except Exception as e:
        logging.error(f"Error fetching bond yields: {str(e)}")
        raise

def run_terminal_workflow() -> WorkflowResult:
    """
    Main function to run the complete Bloomberg Terminal workflow:
    1. Load bond configurations from JSON file
    2. Connect to Bloomberg Terminal
    3. Fetch yield data for all bonds
    4. Save results to CSV file
    
    Returns:
        WorkflowResult: Object containing success status and the collected data
    """
    session = None
    try:
        # Load bond configurations from JSON file
        with open(Config.BONDS_JSON_PATH, 'r') as f:
            bonds = json.load(f)
        
        logging.info(f"Loaded {len(bonds)} bonds from bonds.json")
        
        # Connect to Bloomberg Terminal
        session = init_bloomberg_terminal()
        
        # Fetch yield data
        results = get_bond_yields(session, bonds)
        
        # Convert results to a DataFrame for easier handling
        df = pd.DataFrame(results)
        
        # Save to CSV file with today's date
        output_file = Config.get_output_path('bloomberg') / f'bond_yields_terminal_{datetime.now().strftime("%Y%m%d")}.csv'
        df.to_csv(output_file, index=False)
        
        logging.info(f"Successfully saved yields to {output_file}")
        return WorkflowResult(success=True, data=df)
        
    except Exception as e:
        error_msg = f"Error in Bloomberg Terminal workflow: {str(e)}"
        logging.error(error_msg)
        return WorkflowResult(success=False, error=error_msg)
    finally:
        # Always close the Bloomberg session if it was opened
        if session:
            session.stop()

# This section only runs if you execute this file directly (not when imported)
if __name__ == "__main__":
    result = run_terminal_workflow()
    if result.success:
        print("Bloomberg data preview:")
        print(result.data.head())
