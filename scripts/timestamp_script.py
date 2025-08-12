import logging
import sys
import datetime
#import orchestrator example argo or airflow

def get_year_month_date_week():
    # Get the current date
    now = datetime.datetime.now()

    # Extract year, month, day, and week number
    year = now.year%100
    month = now.month
    date = now.day
    #week_number = now.isocalendar()[1]

    # Format the output string
    output = f"{year:02d}{month:02d}{date:02d}"

    return output


if __name__== '__main__': # pragma: no cover
    # Get the formatted output
    result = get_year_month_date_week()
    print('present timestamp is : ',result)
    orchestrator.addOutputVariable('CALENDAR_WEEK', result)
