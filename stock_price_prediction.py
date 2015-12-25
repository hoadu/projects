"""
1. scrape historical data of the date range that the user specifies on the SP500 from Yahoo Finance back by keeping going to "next" link
2. write them into a single csv file
3. read csv data
4. calculate indicators used for modelings
5. remove rows with "NaN"
6. splitting data into train data and test data
7. make predictions for the prediction period that the user specifies in the backtesting way with linear regression as the initial modeling method
8. select mae as the error measurement and calculate it
9. write predicted data to csv file
"""

import requests
from bs4 import BeautifulSoup
import csv
import time
import pandas as pd
from datetime import datetime
from datetime import timedelta
from sklearn.linear_model import LinearRegression
import sys

def getQueryInput():
    print 'Please provide the start date and end date of historical data you want to retrieve.'
    start_date = raw_input('Please enter the query start date in format YYYY-MM-DD(i.e. 2015-12-21):')
    end_date = raw_input('Please enter the query end date in format YYYY-MM-DD(i.e. 2015-12-21):')
    output_file = raw_input('Please enter the csv file name you want to load data to (i.e. acd.csv):')
    a = int(start_date.split('-')[1])-1
    b = int(start_date.split('-')[2])
    c = int(start_date.split('-')[0])
    d = int(end_date.split('-')[1])-1
    e = int(end_date.split('-')[2])
    f = int(end_date.split('-')[0])
    global first_link
    first_link = '/q/hp?s=%5EGSPC&' + 'a=%s&b=%s&c=%s&d=%s&e=%s&f=%s&g=d' % (a,b,c,d,e,f)
    start_date = time.strptime(start_date, '%Y-%m-%d')
    end_date = time.strptime(end_date, '%Y-%m-%d')
    if start_date <= end_date:
        return output_file, first_link
    else:
        sys.exit('Start date should be earlier than end date. Review your inputs and re-execute the program.')

def getData(output_file, pageUrl):
    try:
        response = requests.get('https://finance.yahoo.com'+pageUrl)
    except HTTPError as e:
        print e
    content = response.content
    parser = BeautifulSoup(content, 'html.parser')

    with open(output_file, 'a') as f:
        writer = csv.writer(f)
        try:
            table = parser.select('.yfnc_datamodoutline1')[0]
        except:
            print 'Table cannot be found in the link https://finance.yahoo.com%s. This may be caused by network problem. Try later.' % pageUrl
        if pageUrl == first_link:
            table_heads = table.select('th')
            row = [table_head.text.strip().strip("*").encode('utf-8') for table_head in table_heads]
            writer.writerow(row)
        table_rows = table.select('tr')[1:]
        for table_row in table_rows[1:-1]:
            row = []
            for index, table_data in enumerate(table_row):
                if index == 0:
                    table_data = table_data.text.strip().encode('utf-8')
                    if ',' in table_data:
                        table_data = time.strptime(table_data, '%b %d, %Y')
                        table_data = time.strftime('%Y-%m-%d', table_data)
                    else:
                        table_data = time.strptime(table_data, '%Y-%m-%d')
                        table_data = time.strftime('%Y-%m-%d', table_data)
                    row.append(table_data)
                else:
                    table_data = table_data.text.strip().replace(',', '').encode('utf-8')
                    row.append(table_data)
            writer.writerow(row)
    new_page = parser.find('a', rel='next')
    if new_page != None:
        new_page_link = new_page.attrs['href']
        getData(output_file, new_page_link)
    else:
        return

def getPredectionReq():
    print 'Please provide the start date and end date of the prediction period you want.'
    prediction_start_date = raw_input('Pleae enter the prediction start date in format YYYY-MM-DD(i.e. 2015-12-21):')
    prediction_end_date = raw_input('Pleae enter the prediction end date in format YYYY-MM-DD(i.e. 2015-12-21):')
    predicted_output_file = raw_input('Please enter the csv file name you want to save predicted results to (i.e. acd.csv):')
    prediction_start_date = datetime(int(prediction_start_date.split('-')[0]), int(prediction_start_date.split('-')[1]), int(prediction_start_date.split('-')[2]))
    prediction_end_date = datetime(int(prediction_end_date.split('-')[0]), int(prediction_end_date.split('-')[1]), int(prediction_end_date.split('-')[2]))
    if prediction_start_date <= prediction_end_date:
        return predicted_output_file, prediction_start_date, prediction_end_date
    else:
        sys.exit('Start date should be earlier than end date. Review your inputs and re-execute the program.')


def predict(output_file, predicted_output_file, prediction_start_date, prediction_end_date):
    df = pd.read_csv(output_file)
    df['Date'] =  pd.to_datetime(df['Date'], format='%Y-%m-%d')
    df['year'] = pd.DatetimeIndex(df['Date']).year
    df = df.set_index('Date', drop=True)
    df = df.sort_index(axis=0, ascending=True)
    df['avg_close_price_day_5'] = pd.rolling_mean(df['Close'], window=5).shift(1)
    df['avg_close_price_day_30'] = pd.rolling_mean(df['Close'], window=30).shift(1)
    df['avg_close_price_day_365'] = pd.rolling_mean(df['Close'], window=365).shift(1)
    df['ratio_avg_close_price_5_365'] = df['avg_close_price_day_5'] / df['avg_close_price_day_365']
    df['std_close_price_day_5'] = pd.rolling_std(df['Close'], window=5).shift(1)
    df['std_close_price_day_365'] = pd.rolling_std(df['Close'], window=365).shift(1)
    df['ratio_std_close_price_5_365'] = df['std_close_price_day_5'] / df['std_close_price_day_365']
    df['avg_volume_day_5'] = pd.rolling_mean(df['Volume'], window=5).shift(1)
    df['avg_volume_day_365'] = pd.rolling_mean(df['Volume'], window=365).shift(1)
    df['ratio_volume_5_365'] = df['avg_volume_day_5'] / df['avg_volume_day_365']
    df['std_avg_volume_5'] = pd.rolling_mean(df['avg_volume_day_5'], window=5).shift(1)
    df['std_avg_volume_365'] = pd.rolling_mean(df['avg_volume_day_365'], window=365).shift(1)
    df['ratio_std_avg_volume_5_365'] = df['std_avg_volume_5'] / df['std_avg_volume_365']

    df = df[['Close'] + list(df.columns[6:])]

    df = df.dropna(axis=0)

    predicted_values = []
    df_prediction = pd.DataFrame()
    df_prediction['Actual'] = df.ix[prediction_start_date:prediction_end_date]['Close']

    regressor = LinearRegression()
    for index in df_prediction.index:
        train = df.ix[df.index[0]: index-timedelta(days=1)]
        test = df.ix[index:index]
        train_predictors = train[list(df.columns[1:])]
        train_to_predict = train['Close']
        regressor.fit(train_predictors, train_to_predict)
        test_predictors = test[list(df.columns[1:])]
        predicted_values.append(regressor.predict(test_predictors)[0])
    df_prediction['Predicted'] = predicted_values
    df_prediction.to_csv(predicted_output_file, index=True)
    mae = sum(abs(df_prediction['Actual'] - df_prediction['Predicted'])) / len(df_prediction['Predicted'])
    print 'The mean absolute error is', mae

if __name__ == '__main__':
    output_file, first_link = getQueryInput()
    getData(output_file, first_link)
    predicted_output_file, prediction_start_date, prediction_end_date = getPredectionReq()
    predict(output_file, predicted_output_file, prediction_start_date, prediction_end_date)
