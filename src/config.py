import os
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime

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

    # Error notification configuration
    ERROR_RECIPIENT_1 = os.getenv('ERROR_RECIPIENT_1')
    ERROR_RECIPIENT_2 = os.getenv('ERROR_RECIPIENT_2')
    ERROR_RECIPIENT_3 = os.getenv('ERROR_RECIPIENT_3')

    @classmethod
    def validate(cls):
        """Validate required configuration values"""
        required_vars = [
            ('O365_CLIENT_ID', cls.O365_CLIENT_ID),
            ('O365_CLIENT_SECRET', cls.O365_CLIENT_SECRET),
            ('ERROR_RECIPIENT_1', cls.ERROR_RECIPIENT_1),
            ('ERROR_RECIPIENT_2', cls.ERROR_RECIPIENT_2),
            ('ERROR_RECIPIENT_3', cls.ERROR_RECIPIENT_3),
            ('IJG_DAILY_PATH', cls.IJG_DAILY_PATH),
        ]

        missing = [var[0] for var in required_vars if not var[1]]
        
        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}\n"
                "Please check your .env file and ensure all required variables are set."
            )

    @classmethod
    def get_date_folder(cls):
        """Get today's date folder name"""
        return datetime.now().strftime("%Y%m%d")

    @classmethod
    def get_output_path(cls, data_source=None):
        """
        Get output directory path for today's date.
        All files will be stored in a single folder for each day.
        
        Args:
            data_source: Optional source identifier for temp directory only
        
        Returns:
            Path object for the appropriate output directory
        """
        # Special handling for temp directory
        if data_source == 'temp':
            temp_path = cls.OUTPUT_DIR / '.temp'
            temp_path.mkdir(parents=True, exist_ok=True)
            return temp_path
        
        # Create and return date-specific output directory
        date_path = cls.OUTPUT_DIR / cls.get_date_folder()
        date_path.mkdir(parents=True, exist_ok=True)
        return date_path

    @classmethod
    def get_logs_path(cls):
        """
        Get logs directory path for today's date.
        Creates a new folder for each day's logs.
        
        Returns:
            Path object for today's logs directory
        """
        date_path = cls.LOGS_DIR / cls.get_date_folder()
        date_path.mkdir(parents=True, exist_ok=True)
        return date_path 