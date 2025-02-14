import json
import blpapi
import pandas as pd
from datetime import datetime
import logging
from utils import retry_with_notification
from config import Config
from workflow_result import WorkflowResult

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=Config.get_logs_path() / f'bloomberg_terminal_yields_{datetime.now().strftime("%Y%m%d")}.log'
)

@retry_with_notification()
def init_bloomberg_terminal():
    """Initialize Bloomberg Terminal API session"""
    try:
        # Initialize SessionOptions for Terminal
        session_options = blpapi.SessionOptions()
        session_options.setServerHost(Config.BLOOMBERG_HOST)
        session_options.setServerPort(Config.BLOOMBERG_PORT)
        
        # Create a Session
        session = blpapi.Session(session_options)
        
        # Start session
        if not session.start():
            logging.error("Failed to start session. Make sure Bloomberg Terminal is running.")
            raise ConnectionError("Could not start Bloomberg Terminal session")
        
        # Open service
        if not session.openService("//blp/refdata"):
            logging.error("Failed to open //blp/refdata service")
            raise ConnectionError("Could not open //blp/refdata service")
        
        return session
    
    except Exception as e:
        logging.error(f"Error initializing Bloomberg Terminal session: {str(e)}")
        raise

@retry_with_notification()
def get_bond_yields(session, bonds):
    """Fetch yield values for the given bonds"""
    try:
        # Get service
        refdata_service = session.getService("//blp/refdata")
        
        # Create request
        request = refdata_service.createRequest("ReferenceDataRequest")
        
        # Add securities
        for bond in bonds:
            request.append("securities", bond['ID'])
        
        # Add fields
        request.append("fields", "YLD_YTM_BID")  # Yield to Maturity (Bid)
        request.append("fields", "YLD_YTM_ASK")  # Yield to Maturity (Ask)
        
        logging.info(f"Sending request for {len(bonds)} bonds")
        
        # Send request
        session.sendRequest(request)
        
        # Process response
        results = []
        while True:
            event = session.nextEvent(500)
            
            if event.eventType() == blpapi.Event.RESPONSE:
                for msg in event:
                    security_data = msg.getElement("securityData")
                    
                    for i in range(security_data.numValues()):
                        security = security_data.getValue(i)
                        ticker = security.getElement("security").getValue()
                        field_data = security.getElement("fieldData")
                        
                        # Get bond name from original bonds list
                        bond_name = next((bond['Bond'] for bond in bonds if bond['ID'] == ticker), None)
                        
                        try:
                            yield_bid = field_data.getElement("YLD_YTM_BID").getValue()
                            yield_ask = field_data.getElement("YLD_YTM_ASK").getValue()
                        except Exception as e:
                            yield_bid = None
                            yield_ask = None
                            logging.warning(f"Could not get yield for {ticker}: {str(e)}")
                        
                        results.append({
                            'Bond': bond_name,
                            'Bloomberg_ID': ticker,
                            'Yield_Bid': yield_bid,
                            'Yield_Ask': yield_ask,
                            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        })
                
                break
                
        return results
    
    except Exception as e:
        logging.error(f"Error fetching bond yields: {str(e)}")
        raise

def run_terminal_workflow() -> WorkflowResult:
    """Run the complete Bloomberg Terminal workflow"""
    session = None
    try:
        # Load bonds from JSON file
        with open(Config.BONDS_JSON_PATH, 'r') as f:
            bonds = json.load(f)
        
        logging.info(f"Loaded {len(bonds)} bonds from bonds.json")
        
        # Initialize Bloomberg Terminal session
        session = init_bloomberg_terminal()
        
        # Get yields
        results = get_bond_yields(session, bonds)
        
        # Convert to DataFrame
        df = pd.DataFrame(results)
        
        # Save to CSV
        output_file = Config.get_output_path('bloomberg') / f'bond_yields_terminal_{datetime.now().strftime("%Y%m%d")}.csv'
        df.to_csv(output_file, index=False)
        
        logging.info(f"Successfully saved yields to {output_file}")
        return WorkflowResult(success=True, data=df)
        
    except Exception as e:
        error_msg = f"Error in Bloomberg Terminal workflow: {str(e)}"
        logging.error(error_msg)
        return WorkflowResult(success=False, error=error_msg)
    finally:
        if session:
            session.stop()

if __name__ == "__main__":
    result = run_terminal_workflow()
    if result.success:
        print("Bloomberg data preview:")
        print(result.data.head())
