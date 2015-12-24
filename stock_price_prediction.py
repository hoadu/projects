"""
What have been done:
1. scrape all historical data on the SP500 from Yahoo Finance back to 1950 by keeping going to "next" link
2. write them into a single csv file
3. read csv data
4. calculate indicators used for modelings
5. remove rows with "NaN"
6. splitting data into train data and test data
7. make predictions in the backtesting way with linear regression as the initial modeling method
8. select mae as the error measurement and calculate it
9. write predicted data to csv file
To do:
1. optimize the code and make it an automatic system that can be used everyday
2. try other modeling methods like random forest to improve the prediction accurary
"""

import requests
from bs4 import BeautifulSoup
import csv
import time
import pandas as pd
from datetime import datetime
from sklearn.linear_model import LinearRegression

def getData(pageUrl):
    try:
        response = requests.get("https://finance.yahoo.com"+pageUrl)
    except HTTPError as e:
        print e
    content = response.content
    parser = BeautifulSoup(content, "html.parser")

    with open("stock.csv", "a") as f:
        writer = csv.writer(f)
        try:
            table = parser.find("table", class_="yfnc_datamodoutline1")
        except:
            print 'Table cannot be found in this link. This may be caused by network problem. Try later.'
            print "https://finance.yahoo.com"+pageUrl
        try:
            table = table.find("table")
        except:
            print 'Nested table cannot be found in this link. This may be caused by network problem. Try later.'
            print "https://finance.yahoo.com"+pageUrl
        if pageUrl == "/q/hp?s=%5EGSPC+Historical+Prices":
            tr = table.find('tr')
            td = tr.find_all('th', class_="yfnc_tablehead1")
            row = [element.text.strip().strip("*").encode('utf-8') for element in td]
            writer.writerow(row)
        for tr in table.find_all("tr")[2:]:
            td = tr.find_all("td")
            if len(td) > 1:
                row = []
                for i, element in enumerate(td):
                    if i == 0:
                        element = element.text.strip().encode('utf-8')
                        if "," in element:
                            element = time.strptime(element, "%b %d, %Y")
                            element = time.strftime("%Y-%m-%d", element)
                        else:
                            element = time.strptime(element, "%Y-%m-%d")
                            element = time.strftime("%Y-%m-%d", element)
                        row.append(element)
                    else:
                        element = element.text.strip().replace(",", "").encode("utf-8")
                        row.append(element)
                writer.writerow(row)
    new = parser.find("a", rel="next")
    if new != None:
        newPage = new.attrs["href"]
        getData(newPage)
    else:
        return

getData("/q/hp?s=%5EGSPC+Historical+Prices")


df = pd.read_csv('stock.csv')
df['Date'] =  pd.to_datetime(df['Date'], format='%Y-%m-%d')
df = df.sort(['Date'], ascending=[True])

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
df['year'] = pd.DatetimeIndex(df['Date']).year

df = df[['Date', 'Close'] + list(df.columns[7:])]

df = df[df['Date'] > datetime(year=1951, month=1, day=2)]
df = df.dropna(axis=0)
df = df.reset_index(drop=True)

total_rows = df['Date'].count()
threshold = df[df["Date"] == datetime(year=2015, month=12, day=2)].index.tolist()[0]
predicted_values = []

regressor = LinearRegression()

for row in range(threshold, total_rows):
    train = df.iloc[0:row]
    test = df.iloc[row]
    train_predictors = train[list(df.columns[2:])]
    train_to_predict = train['Close']
    regressor.fit(train_predictors, train_to_predict)
    test_predictors = test[list(df.columns[2:])]
    predicted_values.append(regressor.predict(test_predictors)[0])

result = pd.DataFrame()
result['Date'] = df['Date'].iloc[threshold:total_rows]
result['Actual'] = df['Close'].iloc[threshold:total_rows]
result['Predicted'] = predicted_values
result.to_csv('a_predicted_result.csv', index=False)

mae = sum(abs(result['Actual'] - result['Predicted'])) / len(result['Predicted'])
print 'The mean absolute error is', mae
