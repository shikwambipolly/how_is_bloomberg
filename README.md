# Bloomberg Bond Yield Fetcher

This project provides scripts to fetch bond yield data from Bloomberg, supporting both Bloomberg Terminal and B-PIPE connectivity methods.

## Features

- Fetch bond yield data (Bid/Ask) for multiple bonds
- Support for both Bloomberg Terminal and B-PIPE connectivity
- Automatic daily data logging
- CSV output with timestamps
- Comprehensive error handling and logging
- JSON-based bond configuration

## Prerequisites

### Software Requirements
- Python 3.7+
- Bloomberg API (blpapi)
- Bloomberg Terminal installed (for Terminal version) OR B-PIPE credentials (for B-PIPE version)

### Python Dependencies
```bash
pip install -r requirements.txt
```

## Project Structure
```
bloomberg-bond-yields/
├── README.md
├── requirements.txt
├── src/
│   ├── get_data_terminal.py    # For Bloomberg Terminal users
│   ├── get_data_bpipe.py       # For B-PIPE users
│   └── bonds.json              # Bond configuration file
└── logs/                       # Log files directory
```

## Configuration

### bonds.json
Configure your bonds in the `bonds.json` file using the following format:
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

### Environment Variables (B-PIPE Only)
If using B-PIPE version, set the following environment variables:
```bash
export BLOOMBERG_HOST="your-bpipe-host"
export BLOOMBERG_PORT="your-bpipe-port"
export BLOOMBERG_APP_NAME="your-app-name"
export BLOOMBERG_APP_AUTH_KEY="your-auth-key"
```

## Usage

### Bloomberg Terminal Version
1. Ensure Bloomberg Terminal is running and you're logged in
2. Run:
```bash
python src/get_data_terminal.py
```

### B-PIPE Version
1. Set required environment variables
2. Run:
```bash
python src/get_data_bpipe.py
```

## Output

The scripts generate two types of files:

1. CSV Data Files:
   - Terminal: `bond_yields_terminal_YYYYMMDD.csv`
   - B-PIPE: `bond_yields_bpipe_YYYYMMDD.csv`

2. Log Files:
   - Terminal: `bloomberg_terminal_yields_YYYYMMDD.log`
   - B-PIPE: `bloomberg_bpipe_yields_YYYYMMDD.log`

### CSV Format
```csv
Bond,Bloomberg_ID,Yield_Bid,Yield_Ask,Timestamp
R186,CP507394@EXCH Corp,7.85,7.87,2024-03-21 10:30:15
R213,EI258596@EXCH Corp,8.92,8.94,2024-03-21 10:30:15
```

## Scheduling Daily Runs

### Windows (Task Scheduler)
1. Open Task Scheduler
2. Create Basic Task
3. Set trigger to Daily
4. Action: Start a program
5. Program/script: `python`
6. Arguments: `path/to/src/get_data_terminal.py`

### Linux/Unix (Cron)
Add to crontab:
```bash
0 9 * * 1-5 cd /path/to/project && python src/get_data_terminal.py
```

## Error Handling

The scripts include comprehensive error handling for common issues:
- Bloomberg connection failures
- Missing or invalid bond data
- Authentication errors (B-PIPE)
- Data retrieval failures

All errors are logged to the respective log files with timestamps and error details.

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Disclaimer

This project is not affiliated with Bloomberg L.P. Bloomberg Terminal and B-PIPE are trademarks of Bloomberg L.P. 