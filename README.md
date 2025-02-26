# Bond Data Collection System

A robust, automated system for collecting and processing bond data from multiple sources to generate comprehensive analysis and reports.

## Project Overview

This system automates the daily collection of financial market data from three distinct sources, processes it according to financial analysis rules, and generates standardized outputs. Each data source has its own workflow with built-in error handling, retry mechanisms, and notification systems.

### Data Sources

1. **Market Data Terminal**
   - Connects to financial data terminal
   - Fetches yield data for configured bonds
   - Uses provider's Python API

2. **National Data Source**
   - Monitors Office 365 inbox for emails from the national exchange
   - Downloads and processes daily report attachments
   - Extracts relevant trading data

3. **Company Data**
   - Processes daily company Excel reports
   - Extracts and saves relevant datasets for further processing

### Key Features

- **Automated Data Collection**
  - Single command to run all workflows
  - Individual workflow execution option
  - Automatic retry on failures

- **Robust Error Handling**
  - Three retry attempts with configurable intervals
  - Detailed error logging
  - Email notifications for persistent failures

- **Data Processing**
  - Standardized data formats
  - Source tracking and prioritization
  - Data validation at each step

- **Monitoring & Notifications**
  - Uses Office 365 for email monitoring and notifications
  - Configurable notification recipients
  - Detailed logging of all operations

## Prerequisites

### Software Requirements
- Python 3.7+
- Market data terminal installed locally
- Microsoft Office 365 account with appropriate permissions
- Git (for version control)

### Python Dependencies
```bash
pip install -r requirements.txt
```

### Required Credentials
1. **Market Data Terminal**
   - Local installation
   - Default connection settings

2. **Microsoft Office 365**
   - Azure App Registration
   - Required permissions:
     - Mail.Read
     - Mail.Send
     - Mail.ReadWrite

## Project Structure
```
bond-data-collection/
├── README.md
├── requirements.txt
├── .env                        # Configuration file (not in version control)
├── .env.example                # Example configuration template
├── src/
│   ├── run_all.py              # Master workflow orchestrator
│   ├── config.py               # Configuration management
│   ├── utils.py                # Common utilities
│   ├── get_yields_terminal.py  # Market data collection
│   ├── get_national_email.py   # National data source processing
│   ├── get_company_daily.py    # Company data processing
│   └── bonds.json              # Bond configuration
├── output/                     # Data output directory
└── logs/                       # Log files directory
```

## Configuration

### Environment Setup
1. Copy `.env.example` to `.env`
```bash
cp .env.example .env
```

2. Configure environment variables:
```bash
# Market Data Terminal Configuration
BLOOMBERG_HOST=localhost
BLOOMBERG_PORT=8194

# Microsoft 365 Configuration
O365_CLIENT_ID=your_client_id
O365_CLIENT_SECRET=your_client_secret

# Error Notification Configuration
ERROR_RECIPIENT_1=first.recipient@example.com
ERROR_RECIPIENT_2=second.recipient@example.com

# File Paths
COMPANY_DAILY_PATH=/path/to/daily/report.xlsx
BONDS_JSON_PATH=src/bonds.json

# Output Configuration
OUTPUT_DIR=output
LOGS_DIR=logs
```

### Bond Configuration
Configure target financial instruments in the bonds.json file.

## Usage

### Running All Workflows
```bash
python src/run_all.py
```

### Running Individual Workflows
```bash
python src/get_yields_terminal.py  # Market data only
python src/get_national_email.py   # National data source only
python src/get_company_daily.py    # Company data only
```

### Output
Each workflow generates:
1. CSV data files with standardized formats
2. Detailed log files
3. Error notifications (if needed)

## Production Setup

### Daily Scheduling

#### Windows (Task Scheduler)
1. Open Task Scheduler
2. Create Basic Task
3. Set trigger to Daily
4. Action: Start a program
5. Program/script: `python`
6. Arguments: `path/to/src/run_all.py`

#### Linux/Unix (Cron)
Add to crontab:
```bash
0 9 * * 1-5 cd /path/to/project && python src/run_all.py
```

### Monitoring
- Check log files in `logs/` directory
- Monitor error notifications
- Review output CSV files

### Error Handling
The system includes comprehensive error handling:
1. **Retry Logic**
   - Configurable retry attempts per operation
   - Configurable intervals between attempts
   - Automatic notification on final failure

2. **Data Validation**
   - Input file existence checks
   - Data format validation
   - Output data verification

3. **Error Notifications**
   - Sent via Office 365
   - Include error details
   - Sent to multiple recipients

## Security Considerations

1. **Credentials**
   - Store in `.env` file
   - Never commit to version control
   - Use secure storage in production

2. **Office 365**
   - Use App Registration
   - Implement least privilege access
   - Regular credential rotation

3. **Data Protection**
   - Secure file permissions
   - Clean up temporary files
   - Encrypt sensitive data

## Maintenance

### Daily Tasks
- Monitor log files
- Check error notifications
- Verify data output

### Weekly Tasks
- Review system performance
- Check disk space
- Backup configuration

### Monthly Tasks
- Rotate log files
- Update dependencies
- Review access credentials

## Troubleshooting

### Common Issues
1. **Data Terminal Connection**
   - Verify Terminal is running
   - Check connection settings
   - Review logs

2. **Email Processing**
   - Check O365 credentials
   - Verify email format
   - Review attachment names

3. **Company Data**
   - Verify file path
   - Check Excel format
   - Validate sheet names

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Disclaimer

This project is not affiliated with any financial data provider or exchange. Names of specific services are the property of their respective owners. 