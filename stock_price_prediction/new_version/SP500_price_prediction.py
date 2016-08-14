from yahoo_finance import Share
import pandas as pd
from sklearn.linear_model import LinearRegression
from datetime import timedelta
import settings 

query_start_date = settings.QUERY_START_DATE
query_end_date = settings.QUERY_END_DATE
prediction_start_date = settings.PREDICTION_START_DATE
prediction_end_date = settings.PREDICTION_END_DATE

gspc = Share('^GSPC')
historical_data = gspc.get_historical(query_start_date, query_end_date)

df = pd.DataFrame(historical_data)

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


