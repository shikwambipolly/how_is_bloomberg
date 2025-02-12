import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

class Config:
    # Project paths
    BASE_DIR = Path(__file__).parent.parent
    OUTPUT_DIR = Path(os.getenv('OUTPUT_DIR', 'output'))
    LOGS_DIR = Path(os.getenv('LOGS_DIR', 'logs'))
    BONDS_JSON_PATH = Path(os.getenv('BONDS_JSON_PATH', 'src/bonds.json'))
    IJG_DAILY_PATH = Path(os.getenv('IJG_DAILY_PATH'))

    # Bloomberg configuration
    BLOOMBERG_HOST = os.getenv('BLOOMBERG_HOST', 'localhost')
    BLOOMBERG_PORT = int(os.getenv('BLOOMBERG_PORT', '8194'))

    # Microsoft 365 configuration
    O365_CLIENT_ID = os.getenv('O365_CLIENT_ID')
    O365_CLIENT_SECRET = os.getenv('O365_CLIENT_SECRET')

    # Email notification configuration
    SMTP_SERVER = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
    SMTP_PORT = int(os.getenv('SMTP_PORT', '587'))
    SENDER_EMAIL = os.getenv('SENDER_EMAIL')
    SENDER_PASSWORD = os.getenv('SENDER_PASSWORD')
    RECIPIENT_EMAIL = os.getenv('RECIPIENT_EMAIL')

    @classmethod
    def validate(cls):
        """Validate required configuration values"""
        required_vars = [
            ('O365_CLIENT_ID', cls.O365_CLIENT_ID),
            ('O365_CLIENT_SECRET', cls.O365_CLIENT_SECRET),
            ('SENDER_EMAIL', cls.SENDER_EMAIL),
            ('SENDER_PASSWORD', cls.SENDER_PASSWORD),
            ('RECIPIENT_EMAIL', cls.RECIPIENT_EMAIL),
            ('IJG_DAILY_PATH', cls.IJG_DAILY_PATH),
        ]

        missing = [var[0] for var in required_vars if not var[1]]
        
        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}\n"
                "Please check your .env file and ensure all required variables are set."
            )

    @classmethod
    def get_output_path(cls, data_source):
        """Get output directory path for specific data source"""
        output_path = cls.BASE_DIR / cls.OUTPUT_DIR / data_source
        output_path.mkdir(parents=True, exist_ok=True)
        return output_path

    @classmethod
    def get_logs_path(cls):
        """Get logs directory path"""
        logs_path = cls.BASE_DIR / cls.LOGS_DIR
        logs_path.mkdir(parents=True, exist_ok=True)
        return logs_path 