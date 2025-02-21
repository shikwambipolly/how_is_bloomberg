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

def send_workflow_email(subject: str, body: str):
    """
    Send workflow status email using Office 365.
    Ensures proper formatting of line breaks in the email body.
    
    Args:
        subject: Email subject line
        body: Email body text with line breaks (\n)
    """
    try:
        # Get O365 account
        account = get_o365_account()
        
        # Get mailbox
        mailbox = account.mailbox()
        
        # Create message
        message = mailbox.new_message()
        message.subject = subject
        
        # Format the body with HTML to preserve line breaks
        html_body = f"""
        <html>
        <body>
        <pre style="font-family: Consolas, 'Courier New', monospace; white-space: pre-wrap;">
{body}
        </pre>
        </body>
        </html>
        """
        
        # Set the message body with HTML formatting
        message.body = html_body
        
        # Add recipients
        message.to.add([Config.ERROR_RECIPIENT_1, Config.ERROR_RECIPIENT_2, Config.ERROR_RECIPIENT_3])
        
        # Send message
        message.send()
        
        logging.info(f"Status email sent with subject: {subject}")
        
    except Exception as e:
        logging.error(f"Failed to send status email: {str(e)}")
        # Re-raise the exception to ensure the calling code knows about the failure
        raise

def retry_with_notification(max_retries=3, delay_minutes=0.05):
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
                        error_body = f"Function execution failed after {max_retries} attempts.\n\n"
                        error_body += f"Function: {func.__name__}\n"
                        error_body += f"Error: {str(e)}"
                        
                        send_workflow_email(
                            f"✗ Function Failed: {func.__name__}",
                            error_body
                        )
                        raise
            
        return wrapper
    return decorator 