import datetime

def is_public_holiday(date: datetime.date) -> bool:
    """
    Check if a given date is a public holiday in the South African market.
    
    Args:
        date (datetime.date): The date to check.
        
    Returns:
        bool: True if the date is a public holiday, False otherwise.
    """
    
    public_holidays = [
        datetime.date(date.year, 1, 1), # New Year's Day
        datetime.date(date.year, 3, 21), # Human Rights Day
        datetime.date(date.year, 4, 18), # Good Friday
        datetime.date(date.year, 4, 21), # Family Day
        datetime.date(date.year, 4, 28), # Freedom Day
        datetime.date(date.year, 5, 1), # Workers Day
        datetime.date(date.year, 6, 16), # Youth Day
        datetime.date(date.year, 8, 9), # National Women's Day
        datetime.date(date.year, 9, 24), # Day of Reconciliation
        datetime.date(date.year, 12, 16), # Day of Goodwill
        datetime.date(date.year, 12, 25), # Christmas Day
        datetime.date(date.year, 12, 26), # Day of Goodwill
    ]

    return date in public_holidays
