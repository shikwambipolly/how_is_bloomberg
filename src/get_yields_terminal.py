"""
This module connects to the Bloomberg Terminal and fetches real-time bond yield data.
It reads a list of bonds from a configuration file and retrieves their current
Yield to Maturity (YTM) values for both Bid and Ask prices.

The data is saved to a CSV file with today's date in the filename.
"""

import json  # Library for reading JSON files
import blpapi  # Bloomberg Terminal API
import pandas as pd  # Library for data manipulation and analysis
from datetime import datetime, timedelta  # Library for working with dates and times
import logging  # Library for creating log files
from utils import retry_with_notification  # Custom retry mechanism
from config import Config  # Project configuration settings
from workflow_result import WorkflowResult  # Custom class for workflow results
from decimal import Decimal  # Import Decimal for precise decimal handling

# Set up logging
logger = logging.getLogger('bloomberg_workflow')
logger.setLevel(logging.INFO)

# Create a file handler
log_file = Config.get_logs_path() / f'bloomberg_terminal_yields_{datetime.now().strftime("%Y%m%d")}.log'
file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.INFO)

# Create a formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(file_handler)

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
            logger.error("Failed to start session. Make sure Bloomberg Terminal is running.")
            raise ConnectionError("Could not start Bloomberg Terminal session")
        
        # Open the reference data service
        if not session.openService("//blp/refdata"):
            logger.error("Failed to open //blp/refdata service")
            raise ConnectionError("Could not open //blp/refdata service")
        
        return session
    
    except Exception as e:
        logger.error(f"Error initializing Bloomberg Terminal session: {str(e)}")
        raise

@retry_with_notification()  # Retry this operation if it fails
def get_bond_yields(session, bonds):
    """
    Fetch yield values for a list of bonds from the Bloomberg Terminal.
    Gets the Last Conventional Yield value from the previous business day.
    Also fetches JIBAR data separately using PX_LAST.
    Processes bonds in batches of maximum 10 securities per request.
    
    Args:
        session: Active Bloomberg Terminal session
        bonds: List of bond configurations containing IDs and names
        
    Returns:
        list: List of dictionaries containing bond data:
            - Bond: Name of the bond
            - Bloomberg_ID: Bloomberg identifier
            - Yield: Last Conventional Yield value or PX_LAST for JIBAR
            - Date: Date of the data
            - Timestamp: When the data was collected
    """ 
    try:
        results = []
        refdata_service = session.getService("//blp/refdata")
        
        # Get previous business day
        today = datetime.now()
        date_str = today.strftime("%Y%m%d")
        
        # Process regular bonds in batches of 10
        for i in range(0, len(bonds), 10):
            batch = bonds[i:i+10]
            
            # Create historical data request for this batch
            request = refdata_service.createRequest("HistoricalDataRequest")
            
            # Add each bond's ID to the request
            for bond in batch:
                request.append("securities", bond['ID'])
            
            # Specify which fields we want
            request.append("fields", "YLD_CNV_LAST")  # Last Conventional Yield
            
            # Set the date range to just the previous business day
            request.set("startDate", date_str)
            request.set("endDate", date_str)
            
            logger.info(f"Sending historical request for batch of {len(batch)} bonds (bonds {i+1} to {i+len(batch)}) for date {date_str}")
            
            # Send the request to Bloomberg
            session.sendRequest(request)
            
            # Process the response for this batch
            while True:
                event = session.nextEvent(500)  # Wait up to 500ms for response
                
                if event.eventType() in [blpapi.Event.PARTIAL_RESPONSE, blpapi.Event.RESPONSE]:
                    for msg in event:
                        if not msg.hasElement("securityData"):
                            logger.warning("Message does not contain 'securityData' element, skipping.")
                            continue
                        secElem = msg.getElement("securityData")
                        # Check if securityData is an array
                        if secElem.isArray():
                            count = secElem.numValues()
                        else:
                            count = 1
                        
                        for idx in range(count):
                            if secElem.isArray():
                                security_data = secElem.getValueAsElement(idx)
                            else:
                                security_data = secElem
                            
                            ticker = security_data.getElementAsString("security")
                            
                            # Find the bond's name from our configuration
                            bond_name = next((bond['Bond'] for bond in batch if bond['ID'] == ticker), None)
                            
                            try:
                                # Get the fieldData array
                                field_data = security_data.getElement("fieldData")
                                
                                # Initialize yield and date as None
                                yield_value = None
                                price_date = None
                                
                                # If we have any data points
                                if field_data.numValues() > 0:
                                    # Get the first (and should be only) data point
                                    point = field_data.getValueAsElement(0)
                                    
                                    # Extract yield and date if available
                                    if point.hasElement("YLD_CNV_LAST"):
                                        # Get the raw value as string first to preserve precision
                                        yield_str = point.getElementAsString("YLD_CNV_LAST")
                                        try:
                                            # Use Decimal to preserve exact decimal representation
                                            yield_value = float(Decimal(yield_str))
                                        except:
                                            # Fallback to getElementAsFloat if parsing fails
                                            yield_value = point.getElementAsFloat("YLD_CNV_LAST")
                                        
                                    if point.hasElement("date"):
                                        price_date = point.getElementAsDatetime("date")
                                
                                if yield_value is None:
                                    logger.warning(f"No yield data found for {ticker} on {date_str}")
                                
                            except Exception as e:
                                logger.warning(f"Could not get yield for {ticker}: {str(e)}")
                                yield_value = None
                                price_date = None
                            
                            # Store the results
                            results.append({
                                'Bond': bond_name,
                                'Bloomberg_ID': ticker,
                                'Yield': yield_value,
                                'Date': price_date.strftime("%Y-%m-%d") if price_date else None,
                                'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            })
                    
                    # Only break on final RESPONSE, not on PARTIAL_RESPONSE
                    if event.eventType() == blpapi.Event.RESPONSE:
                        break
        
        # Finally, get JIBAR data separately
        jibar_request = refdata_service.createRequest("HistoricalDataRequest")
        jibar_request.append("securities", "JIBA3M Index")
        jibar_request.append("fields", "PX_LAST")
        jibar_request.set("startDate", date_str)
        jibar_request.set("endDate", date_str)
        
        logger.info(f"Sending historical request for JIBAR data for date {date_str}")
        
        # Send the JIBAR request
        session.sendRequest(jibar_request)
        
        # Process the JIBAR response
        while True:
            event = session.nextEvent(500)
            
            if event.eventType() in [blpapi.Event.PARTIAL_RESPONSE, blpapi.Event.RESPONSE]:
                for msg in event:
                    if not msg.hasElement("securityData"):
                        logger.warning("JIBAR message does not contain 'securityData' element, skipping.")
                        continue
                    secElem = msg.getElement("securityData")
                    if secElem.isArray():
                        count = secElem.numValues()
                    else:
                        count = 1
                    
                    for idx in range(count):
                        if secElem.isArray():
                            security_data = secElem.getValueAsElement(idx)
                        else:
                            security_data = secElem
                        
                        try:
                            # Get the fieldData array
                            field_data = security_data.getElement("fieldData")
                            
                            # Initialize JIBAR value and date as None
                            jibar_value = None
                            price_date = None
                            
                            # If we have any data points
                            if field_data.numValues() > 0:
                                # Loop through the data points
                                for i in range(field_data.numValues()):
                                    point = field_data.getValueAsElement(i)
                                    
                                    # Extract PX_LAST value if available
                                    if point.hasElement("PX_LAST"):
                                        # Get the raw value as string first to preserve precision
                                        jibar_str = point.getElementAsString("PX_LAST")
                                        try:
                                            # Use Decimal to preserve exact decimal representation
                                            jibar_value = float(Decimal(jibar_str))
                                        except:
                                            # Fallback to getElementAsFloat if parsing fails
                                            jibar_value = point.getElementAsFloat("PX_LAST")
                                        
                                    if point.hasElement("date"):
                                        price_date = point.getElementAsDatetime("date")
                            
                            if jibar_value is None:
                                logger.warning(f"No JIBAR data found for date {date_str}")
                                
                        except Exception as e:
                            logger.warning(f"Could not get JIBAR value: {str(e)}")
                            jibar_value = None
                            price_date = None
                        
                        # Add JIBAR to results
                        results.append({
                            'Bond': '3M JIBAR',
                            'Bloomberg_ID': 'JIBA3M Index',
                            'Yield': jibar_value,
                            'Date': price_date.strftime("%Y-%m-%d") if price_date else None,
                            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        })
                
                # Only break on final RESPONSE, not on PARTIAL_RESPONSE
                if event.eventType() == blpapi.Event.RESPONSE:
                    break
        
        # Log the total number of results collected
        logger.info(f"Total bonds collected: {len(results)} (including JIBAR)")
        
        return results
    
    except Exception as e:
        logger.error(f"Error fetching bond yields: {str(e)}")
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
        
        logger.info(f"Loaded {len(bonds)} bonds from bonds.json")
        
        # Connect to Bloomberg Terminal
        session = init_bloomberg_terminal()
        
        # Fetch yield data
        results = get_bond_yields(session, bonds)
        
        # Convert results to a DataFrame
        df = pd.DataFrame(results)
        
        # Save to CSV file with today's date in today's directory
        output_file = Config.get_output_path() / f'bond_yields_terminal_{datetime.now().strftime("%Y%m%d")}.csv'
        df.to_csv(output_file, index=False)
        
        logger.info(f"Successfully saved yields to {output_file}")
        return WorkflowResult(success=True, data=df)
        
    except Exception as e:
        error_msg = f"Error in Bloomberg Terminal workflow: {str(e)}"
        logger.error(error_msg)
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
