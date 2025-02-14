# Bond Data Collection System

A robust, automated system for collecting and processing bond data from multiple sources:
- Bloomberg Terminal (real-time bond yields)
- NSX Daily Report (via Office 365 email)
- IJG Daily Report (Excel-based bond data)

## Project Overview

This system automates the daily collection of bond data from three distinct sources, processes it, and stores it in a standardized format. Each data source has its own workflow with built-in error handling, retry mechanisms, and notification systems.

### Data Sources

1. **Bloomberg Terminal**
   - Connects to local Bloomberg Terminal
   - Fetches yield data (Bid/Ask) for configured bonds
   - Uses Bloomberg's Python API (blpapi)

2. **NSX Daily Report**
   - Monitors Office 365 inbox for emails from info@nsx.com.na
   - Downloads and processes "NSX Daily Report" Excel attachments
   - Extracts bond trading data from specific sheets

3. **IJG Daily Report**
   - Processes daily IJG Excel report
   - Extracts specific bond data:
     - Rows with GI codes from "Yields" sheet
     - Rows 2-19 from "Spread Calc" sheet
   - Combines data with source tracking

### Key Features

- **Automated Data Collection**
  - Single command to run all workflows
  - Individual workflow execution option
  - Automatic retry on failures

- **Robust Error Handling**
  - Three retry attempts with 15-minute intervals
  - Detailed error logging
  - Email notifications for persistent failures

- **Data Processing**
  - Standardized data formats
  - Source tracking
  - Data validation at each step

- **Monitoring & Notifications**
  - Uses Office 365 for email monitoring and notifications
  - Configurable notification recipients
  - Detailed logging of all operations

## Prerequisites

### Software Requirements
- Python 3.7+
- Bloomberg Terminal installed locally
- Microsoft Office 365 account with appropriate permissions
- Git (for version control)

### Python Dependencies
```bash
pip install -r requirements.txt
```

### Required Credentials
1. **Bloomberg Terminal**
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
├── .env.example               # Example configuration template
├── src/
│   ├── run_all.py            # Master workflow orchestrator
│   ├── config.py             # Configuration management
│   ├── utils.py              # Common utilities
│   ├── get_yields_terminal.py # Bloomberg data collection
│   ├── get_nsx_email.py      # NSX email processing
│   ├── get_IJG_daily.py      # IJG data processing
│   └── bonds.json            # Bond configuration
├── output/                    # Data output directory
│   ├── bloomberg/            # Bloomberg yield data
│   ├── nsx/                  # NSX daily report data
│   └── ijg/                  # IJG daily data
└── logs/                     # Log files directory
```

## Configuration

### Environment Setup
1. Copy `.env.example` to `.env`
```bash
cp .env.example .env
```

2. Configure environment variables:
```bash
# Bloomberg Terminal Configuration
BLOOMBERG_HOST=localhost
BLOOMBERG_PORT=8194

# Microsoft 365 Configuration
O365_CLIENT_ID=your_client_id
O365_CLIENT_SECRET=your_client_secret

# Error Notification Configuration
ERROR_RECIPIENT_1=first.recipient@example.com
ERROR_RECIPIENT_2=second.recipient@example.com

# File Paths
IJG_DAILY_PATH=/path/to/ijg/daily/report.xlsx
BONDS_JSON_PATH=src/bonds.json

# Output Configuration
OUTPUT_DIR=output
LOGS_DIR=logs
```

### Bond Configuration
Configure target bonds in `bonds.json`:
```json
[
    {
        "ID": "CP507394@EXCH Corp",
        "Bond": "R186"
    }
]
```

## Usage

### Running All Workflows
```bash
python src/run_all.py
```

### Running Individual Workflows
```bash
python src/get_yields_terminal.py  # Bloomberg only
python src/get_nsx_email.py       # NSX only
python src/get_IJG_daily.py       # IJG only
```

### Output
Each workflow generates:
1. CSV data files in respective output directories
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
   - 3 attempts per operation
   - 15-minute intervals between attempts
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
1. **Bloomberg Connection**
   - Verify Terminal is running
   - Check connection settings
   - Review Bloomberg logs

2. **Email Processing**
   - Check O365 credentials
   - Verify email format
   - Review attachment names

3. **IJG Data**
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

This project is not affiliated with Bloomberg L.P., NSX, or IJG. 