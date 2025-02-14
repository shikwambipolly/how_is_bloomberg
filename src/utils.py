import time
from O365 import Account
from datetime import datetime
import logging
from functools import wraps
from config import Config

# Global O365 Account instance
_o365_account = None

def get_o365_account():
    """Get or create O365 Account instance"""
    global _o365_account
    if _o365_account is None:
        _o365_account = Account((Config.O365_CLIENT_ID, Config.O365_CLIENT_SECRET))
        if not _o365_account.is_authenticated:
            _o365_account.authenticate()
    return _o365_account

def send_error_email(error_message, source):
    """Send error notification email using Office 365"""
    try:
        # Get O365 account
        account = get_o365_account()
        
        # Get mailbox
        mailbox = account.mailbox()
        
        # Create message
        message = mailbox.new_message()
        message.subject = f"Error in {source} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        
        body = f"""
        An error occurred in {source}:
        
        {error_message}
        
        This message was sent after 3 failed attempts with 15-minute intervals.
        """
        
        message.body = body
        
        # Add recipients
        message.to.add([Config.ERROR_RECIPIENT_1, Config.ERROR_RECIPIENT_2])
        
        # Send message
        message.send()
        
        logging.info(f"Error notification email sent for {source}")
        
    except Exception as e:
        logging.error(f"Failed to send error email: {str(e)}")

def retry_with_notification(max_retries=3, delay_minutes=15):
    """Decorator for retrying functions with delay and email notification"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logging.error(f"Attempt {attempt + 1} failed: {str(e)}")
                    
                    if attempt < max_retries - 1:
                        logging.info(f"Waiting {delay_minutes} minutes before next attempt...")
                        time.sleep(delay_minutes * 60)
                    else:
                        # Send error notification after all retries failed
                        error_message = f"Function {func.__name__} failed after {max_retries} attempts.\nLast error: {str(e)}"
                        send_error_email(error_message, func.__name__)
                        raise
            
        return wrapper
    return decorator 