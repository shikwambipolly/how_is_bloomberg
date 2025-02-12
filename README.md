# Bond Data Collection System

This project provides scripts to fetch bond data from multiple sources:
- Bloomberg Terminal (yields)
- NSX Daily Report (via email)
- IJG Daily Report (from Word document)

## Features

- Fetch bond yield data from Bloomberg Terminal
- Download and process NSX Daily Report from email
- Extract bond data from IJG Daily Report
- Automatic retry logic with 15-minute intervals
- Error notification emails
- Comprehensive logging
- Single command to run all workflows

## Prerequisites

### Software Requirements
- Python 3.7+
- Bloomberg Terminal installed (for Terminal version)
- Microsoft Office 365 account (for NSX email access)

### Python Dependencies
```bash
pip install -r requirements.txt
```

## Project Structure
```
bond-data-collection/
├── README.md
├── requirements.txt
├── src/
│   ├── run_all.py              # Master script to run all workflows
│   ├── get_yields_terminal.py  # Bloomberg Terminal data collection
│   ├── get_nsx_email.py        # NSX email processing
│   ├── get_IJG_daily.py        # IJG report processing
│   ├── utils.py                # Utility functions
│   └── bonds.json              # Bond configuration file
├── output/                     # Data output directory
│   ├── bloomberg/             
│   ├── nsx/
│   └── ijg/
└── logs/                       # Log files directory
```

## Configuration

### Environment Variables
Set the following environment variables:

```bash
# Bloomberg B-PIPE (if using)
export BLOOMBERG_HOST="your-bpipe-host"
export BLOOMBERG_PORT="your-bpipe-port"
export BLOOMBERG_APP_NAME="your-app-name"
export BLOOMBERG_APP_AUTH_KEY="your-auth-key"

# Microsoft 365 (for NSX email)
export O365_CLIENT_ID="your-client-id"
export O365_CLIENT_SECRET="your-client-secret"

# Email Notifications
export SMTP_SERVER="smtp.gmail.com"
export SMTP_PORT="587"
export SENDER_EMAIL="your-sender-email"
export SENDER_PASSWORD="your-app-specific-password"
export RECIPIENT_EMAIL="your-notification-recipient"
```

### bonds.json
Configure your bonds in the `bonds.json` file:
```json
[
    {
        "ID": "CP507394@EXCH Corp",
        "Bond": "R186"
    },
    {
        "ID": "EI258596@EXCH Corp",
        "Bond": "R213"
    }
]
```

## Usage

### Running All Workflows
To run all data collection workflows:
```bash
python src/run_all.py
```

This will:
1. Run Bloomberg Terminal data collection
2. Process NSX email and extract data
3. Process IJG daily report
4. Save all data to respective output directories
5. Generate logs for each process

### Individual Workflows
You can also run individual workflows:
```bash
python src/get_yields_terminal.py  # Bloomberg only
python src/get_nsx_email.py       # NSX only
python src/get_IJG_daily.py       # IJG only
```

## Retry Logic and Error Handling

Each workflow includes:
- 3 retry attempts with 15-minute intervals
- Email notifications on final failure
- Detailed logging of all attempts and errors

The system will:
1. Attempt each operation
2. On failure, wait 15 minutes and retry
3. After 3 failed attempts, send error notification email
4. Continue with next workflow

## Output

Each workflow generates:
1. CSV data files in respective output directories
2. Detailed log files in the logs directory
3. Error notification emails (if failures occur)

## Scheduling Daily Runs

### Windows (Task Scheduler)
1. Open Task Scheduler
2. Create Basic Task
3. Set trigger to Daily
4. Action: Start a program
5. Program/script: `python`
6. Arguments: `path/to/src/run_all.py`

### Linux/Unix (Cron)
Add to crontab:
```bash
0 9 * * 1-5 cd /path/to/project && python src/run_all.py
```

## Error Handling

The system includes comprehensive error handling for:
- Bloomberg connection failures
- Missing or invalid bond data
- Email authentication errors
- File processing errors
- Network connectivity issues

All errors are:
1. Logged to appropriate log files
2. Retried up to 3 times
3. Reported via email if persistent

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Disclaimer

This project is not affiliated with Bloomberg L.P., NSX, or IJG. 