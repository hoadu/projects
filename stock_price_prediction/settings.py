import datetime

QUERY_START_DATE = '1950-01-01'
QUERY_END_DATE = '2015-12-31'

#change QUERY_START_DATE and QUERY_END_DATE to the below after first-time operation to automate daily data acquisition 
#QUERY_START_DATE = str(datetime.date.today())
#QUERY_END_DATE = str(datetime.date.today())

PREDICTION_START_DATE = '2015-01-01'
PREDICTION_END_DATE = str(datetime.date.today())
PREDICT_TOMMORROW = str(datetime.date.today() + datetime.timedelta(days=1))

DATA_STORAGE_METHOD = 'mysql'

#customize database_connection with your database API and access info
#example: DATABASE_CONNECTION = 'mysql+mysqlconnector://thrifty:123456@localhost/thrifty'
DATABASE_CONNECTION = 'mysql+mysqlconnector://DB_USER:DB_USER_PASS@HOST_IP/DB_NAME'
HISTORICAL_DATA_TABLE = 'historical_data'
PREDICTED_OUTPUT_TABLE = 'predicted_output'

HISTORICAL_DATA_FILE = 'historical_data.csv'
PREDICTED_OUTPUT_FILE = 'predicted_output.csv'

#customize cron job command
#example: CRON_DATA_JOB_COMMAND = '/Users/lilizhang/anaconda/bin/python /Users/lilizhang/documents/data_science_project_code/stock_price_data_acquisition.py'
CRON_DATA_JOB_COMMAND = 'PYTHON_PATH DATA_ACQUISITION_PYTHON_FILE_PATH'
CRON_PREDICTION_JOB_COMMAND = 'PYTHON_PATH PREDICTION_PYTHON_FILE_PATH'
