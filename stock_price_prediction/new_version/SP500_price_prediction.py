from yahoo_finance import Share
import pandas as pd
from sklearn.linear_model import LinearRegression
from datetime import timedelta

gspc = Share('^GSPC')
historical_data = gspc.get_historical('2016-05-01', '2016-08-11')

prediction_start_date = '2016-08-01'
prediction_end_date = '2016-08-11'

df = pd.DataFrame(historical_data)

df['Date'] =  pd.to_datetime(df['Date'], format='%Y-%m-%d')
df['year'] = pd.DatetimeIndex(df['Date']).year
df = df.set_index('Date', drop=True)
df = df.sort_index(axis=0, ascending=True)

df['avg_close_price_day_2'] = pd.rolling_mean(df['Close'], window=2).shift(1)

df['avg_close_price_day_5'] = pd.rolling_mean(df['Close'], window=5).shift(1)

df['avg_close_price_day_10'] = pd.rolling_mean(df['Close'], window=10).shift(1)

df['ratio_avg_close_price_2_10'] = df['avg_close_price_day_2'] / df['avg_close_price_day_10']

df['std_close_price_day_2'] = pd.rolling_std(df['Close'], window=2).shift(1)

df['std_close_price_day_10'] = pd.rolling_std(df['Close'], window=10).shift(1)

df['ratio_std_close_price_2_10'] = df['std_close_price_day_2'] / df['std_close_price_day_10']

df['avg_volume_day_2'] = pd.rolling_mean(df['Volume'], window=2).shift(1)

df['avg_volume_day_10'] = pd.rolling_mean(df['Volume'], window=10).shift(1)

df['ratio_volume_2_10'] = df['avg_volume_day_2'] / df['avg_volume_day_10']

df['std_avg_volume_2'] = pd.rolling_std(df['avg_volume_day_2'], window=2).shift(1)

df['std_avg_volume_10'] = pd.rolling_std(df['avg_volume_day_10'], window=10).shift(1)

df['ratio_std_avg_volume_2_10'] = df['std_avg_volume_2'] / df['std_avg_volume_10']

df = df[['Close'] + list(df.columns[6:])]

df = df.dropna(axis=0) 

predicted_values_regression = []

df_prediction = pd.DataFrame()
df_prediction['Actual'] = df.ix[prediction_start_date:prediction_end_date]['Close']
df_prediction['Actual'] = df_prediction['Actual'].astype(float)

regressor = LinearRegression()

for index in df_prediction.index:
    train = df.ix[df.index[0]: index-timedelta(days=1)]
    test = df.ix[index:index]
    train_predictors = train[list(df.columns[1:])]
    train_to_predict = train['Close']
    regressor.fit(train_predictors, train_to_predict)
    test_predictors = test[list(df.columns[1:])]
    predicted_values_regression.append(regressor.predict(test_predictors)[0])

df_prediction['Predicted_regression'] = predicted_values_regression
mae_regression = sum(abs(df_prediction['Actual'] - df_prediction['Predicted_regression'])) / len(df_prediction['Predicted_regression'])

print 'TEST DATA: ACTUAL AND PREDICTED VALUES'
print df_prediction
print 'The MAE of linear regression model is %s ' % mae_regression


