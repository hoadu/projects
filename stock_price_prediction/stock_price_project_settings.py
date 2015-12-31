import datetime

QUERY_START_DATE = '1950-01-01'
QUERY_END_DATE = '2015-12-31'

#QUERY_START_DATE = str(datetime.date.today())
#QUERY_END_DATE = str(datetime.date.today())

PREDICTION_START_DATE = '2015-01-01'
PREDICTION_END_DATE = str(datetime.date.today())
PREDICT_TOMMORROW = str(datetime.date.today() + datetime.timedelta(days=1))

DATA_STORAGE_METHOD = 'mysql'

DATABASE_CONNECTION = 'mysql+mysqlconnector://thrifty:123456@localhost/thrifty'
HISTORICAL_DATA_TABLE = 'historical_data'
PREDICTED_OUTPUT_TABLE = 'predicted_output'

HISTORICAL_DATA_FILE = 'historical_data.csv'
PREDICTED_OUTPUT_FILE = 'predicted_output.csv'
