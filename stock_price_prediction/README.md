Please refer to the code under the folder new_version. 
Because the webpage of Yahoo Finance has changed, the web scraping code does not work any more. 
See the issue discussion here https://github.com/Lili-Updating/projects/issues/1 
-------------------------------------------------------------------------------------------------
Here is what each program does. Customize settings.py as explained below, before running programs.

data_acquisition.py

1. scrape historical data from Yahoo Finance

2. store them in a mysql database or csv file

prediction.py

1. read historical data from a mysql database or csv file

2. calculate indicators used for prediction

3. remove rows with missing values

4. split historical data into train data and test data

5. make predictions on test data in the backtesting way using linear regression and randome forest model

6. calculate mae

7. use the model with smaller mae to make prediction for tomorrow

8. write predicted output

settings.py

1. specify the date range of historical data you want to scrape from Yahoo Finace website:      

  QUERY_START_DATE

  QUERY_END_DATE

2. specify the date range of test data you want to use for prediction

  PREDICTION_START_DATE

  PREDICTION_END_DATE

3. specify the date you want to predict with the selected model

  PREDICT_TOMMORROW

4. specify data storage method, either "mysql" or "csv"

  DATA_STORAGE_METHOD

5. if using mysql database, specify database connection, the name of table for storing historical data, and the name of table for storing predicted output. sqlalchemy is used in this program, whose engine configuration can be found http://docs.sqlalchemy.org/en/latest/core/engines.html.

  DATABASE_CONNECTION = 'mysql+mysqlconnector://DB_USER:DB_USER_PASS@HOST_IP/DB_NAME'

  HISTORICAL_DATA_TABLE

  PREDICTED_OUTPUT_TABLE

6. if using csv, specify the name of file for storing historical data and the name of file for storing predicted output

  HISTORICAL_DATA_FILE

  PREDICTED_OUTPUT_FILE

7. specify command used to create cron jobs (find PATHON_PATH by executing "which python")

  CRON_DATA_JOB_COMMAND = 'PYTHON_PATH DATA_ACQUISITION_PYTHON_FILE_PATH'

  CRON_PREDICTION_JOB_COMMAND = 'PYTHON_PATH PREDICTION_PYTHON_FILE_PATH'

cron_jobs.py

1. create cron jobs to scrape daily updated data and make prediction for tomorrow.
