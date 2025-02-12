import time
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import logging
from functools import wraps
from config import Config

def send_error_email(error_message, source):
    """Send error notification email"""
    try:
        # Create message
        msg = MIMEMultipart()
        msg['From'] = Config.SENDER_EMAIL
        msg['To'] = Config.RECIPIENT_EMAIL
        msg['Subject'] = f"Error in {source} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        
        body = f"""
        An error occurred in {source}:
        
        {error_message}
        
        This message was sent after 3 failed attempts with 15-minute intervals.
        """
        
        msg.attach(MIMEText(body, 'plain'))
        
        # Send email
        with smtplib.SMTP(Config.SMTP_SERVER, Config.SMTP_PORT) as server:
            server.starttls()
            server.login(Config.SENDER_EMAIL, Config.SENDER_PASSWORD)
            server.send_message(msg)
            
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