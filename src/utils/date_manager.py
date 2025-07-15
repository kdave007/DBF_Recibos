from datetime import date, datetime, timedelta
from calendar import monthrange

class DateManager:

    def get_dates(self):
        """
        Returns start_date and end_date based on the current date:
        - If today is the 1st day of the month: start_date is the 1st day of the previous month
        - If today is any other day: start_date is the 1st day of the current month
        - end_date is always the current date
        
        Returns:
            tuple: (start_date, end_date) as date objects in format date(year, month, day)
        """
        # Get current date
        today = date.today()
        current_day = today.day
        current_month = today.month
        current_year = today.year
        
        # Set end_date to current date
        end_date = date(current_year, current_month, current_day)
        
        # Determine start_date based on the day of the month
        if current_day == 1:
            # If today is the 1st day of the month, start from 1st day of previous month
            if current_month == 1:
                # If January, go back to December of previous year
                start_date = date(current_year - 1, 12, 1)
            else:
                # Otherwise, go back to previous month of same year
                start_date = date(current_year, current_month - 1, 1)
        else:
            # If today is not the 1st day, start from 1st day of current month
            start_date = date(current_year, current_month, 1)
        
        return start_date, end_date