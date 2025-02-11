import json
import blpapi
import pandas as pd
from datetime import datetime
import logging
import os

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=f'bloomberg_bpipe_yields_{datetime.now().strftime("%Y%m%d")}.log'
)

def init_bloomberg_bpipe():
    """Initialize Bloomberg B-PIPE session with authentication"""
    try:
        # Get authentication details from environment variables
        host = os.getenv('BLOOMBERG_HOST')
        port = int(os.getenv('BLOOMBERG_PORT', '8194'))
        app_name = os.getenv('BLOOMBERG_APP_NAME')
        app_auth_key = os.getenv('BLOOMBERG_APP_AUTH_KEY')
        
        if not all([host, port, app_name, app_auth_key]):
            raise ValueError(
                "Missing required Bloomberg credentials. Please set environment variables: "
                "BLOOMBERG_HOST, BLOOMBERG_PORT, BLOOMBERG_APP_NAME, BLOOMBERG_APP_AUTH_KEY"
            )
        
        # Initialize SessionOptions for B-PIPE
        session_options = blpapi.SessionOptions()
        session_options.setServerHost(host)
        session_options.setServerPort(port)
        
        # Set authentication options
        session_options.setAuthenticationOptions(f"AuthenticationMode=APPLICATION_ONLY;"
                                              f"ApplicationName={app_name};"
                                              f"ApplicationAuthToken={app_auth_key}")
        
        # Create a Session
        session = blpapi.Session(session_options)
        
        # Start session
        if not session.start():
            logging.error("Failed to start B-PIPE session.")
            raise ConnectionError("Could not start Bloomberg B-PIPE session")
        
        # Open service
        if not session.openService("//blp/refdata"):
            logging.error("Failed to open //blp/refdata service")
            raise ConnectionError("Could not open //blp/refdata service")
        
        return session
    
    except Exception as e:
        logging.error(f"Error initializing Bloomberg B-PIPE session: {str(e)}")
        raise

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

def main():
    try:
        # Load bonds from JSON file
        with open('bonds.json', 'r') as f:
            bonds = json.load(f)
        
        logging.info(f"Loaded {len(bonds)} bonds from bonds.json")
        
        # Initialize Bloomberg B-PIPE session
        session = init_bloomberg_bpipe()
        
        # Get yields
        results = get_bond_yields(session, bonds)
        
        # Convert to DataFrame and save to CSV
        df = pd.DataFrame(results)
        output_file = f'bond_yields_bpipe_{datetime.now().strftime("%Y%m%d")}.csv'
        df.to_csv(output_file, index=False)
        
        logging.info(f"Successfully saved yields to {output_file}")
        
        # Print results
        print(df)
        
    except Exception as e:
        logging.error(f"Error in main execution: {str(e)}")
        raise
    finally:
        if 'session' in locals():
            session.stop()

if __name__ == "__main__":
    main()
