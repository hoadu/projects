import settings as setting
import pandas as pd
from dateutil.parser import parse
from datetime import timedelta
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
import sys
from sqlalchemy import create_engine

def get_prediction_req(data_storage_method):
    prediction_start_date = parse(setting.PREDICTION_START_DATE)
    prediction_end_date = parse(setting.PREDICTION_END_DATE)
    predict_tommorrow = parse(setting.PREDICT_TOMMORROW)
    if prediction_start_date <= prediction_end_date:
        if data_storage_method == 'mysql':
            database_connection = setting.DATABASE_CONNECTION
            historical_data_table = setting.HISTORICAL_DATA_TABLE
            predicted_output_table = setting.PREDICTED_OUTPUT_TABLE
            return database_connection, historical_data_table, predicted_output_table, prediction_start_date, prediction_end_date, predict_tommorrow
        else:
            historical_data_file = setting.HISTORICAL_DATA_FILE
            predicted_output_file = setting.PREDICTED_OUTPUT_FILE
            return historical_data_file, predicted_output_file, prediction_start_date, prediction_end_date, predict_tommorrow
    else:
        sys.exit('Start date should be earlier than or same as end date. Review your inputs and re-execute the program.')

def read_data_from_mysql(database_connection, historical_data_table):
    engine = create_engine(database_connection)
    connection = engine.connect()
    df = pd.read_sql_table(historical_data_table, connection)
    connection.close()
    return df

def read_data_from_csv(historical_data_file):
    df = pd.read_csv(historical_data_file)
    return df

def predict(df, prediction_start_date, prediction_end_date, predict_tommorrow):
    df.loc[len(df)] = [predict_tommorrow, 0, 0, 0, 0, 0, 0]
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
    predicted_values_random_forest = []

    df_prediction = pd.DataFrame()
    df_prediction['Actual'] = df.ix[prediction_start_date:prediction_end_date]['Close']

    regressor = LinearRegression()
    random_forest_regressor = RandomForestRegressor()

    for index in df_prediction.index:
        train = df.ix[df.index[0]: index-timedelta(days=1)]
        test = df.ix[index:index]
        train_predictors = train[list(df.columns[1:])]
        train_to_predict = train['Close']
        regressor.fit(train_predictors, train_to_predict)
        random_forest_regressor.fit(train_predictors, train_to_predict)
        test_predictors = test[list(df.columns[1:])]
        predicted_values_regression.append(regressor.predict(test_predictors)[0])
        predicted_values_random_forest.append(random_forest_regressor.predict(test_predictors)[0])

    df_prediction['Predicted_regression'] = predicted_values_regression
    df_prediction['Predicted_random_forest'] = predicted_values_random_forest
    mae_regression = sum(abs(df_prediction['Actual'] - df_prediction['Predicted_regression'])) / len(df_prediction['Predicted_regression'])
    mae_random_forest = sum(abs(df_prediction['Actual'] - df_prediction['Predicted_random_forest'])) / len(df_prediction['Predicted_random_forest'])

    tomorrow = df.ix[predict_tommorrow:predict_tommorrow+timedelta(days=1)]
    tomorrow_predictors = tomorrow[list(df.columns[1:])]

    if mae_regression <= mae_random_forest:
        prediction_for_tomorrow = regressor.predict(tomorrow_predictors)[0]
    else:
        prediction_for_tomorrow = random_forest_regressor.predict(tomorrow_predictors)[0]

    f = open('predicted_value_for_tommorrow', 'w')
    f.write('The mean absolute error of linear regression model and randome forest model is %s and %s , respectively. Based on the model with smaller mae, predicted value for tomorrow is %s .' % (mae_regression,  mae_random_forest, prediction_for_tomorrow))
    f.close()
    return df_prediction

def write_prediction_to_mysql(df_prediction, database_connection, predicted_output_table):
    df_prediction.to_sql(predicted_output_table, database_connection, if_exists='replace')

def write_prediction_to_csv(df_prediction, predicted_output_file):
    df_prediction.to_csv(predicted_output_file, index=True)

def main():
    data_storage_method = setting.DATA_STORAGE_METHOD
    if data_storage_method == 'mysql':
        database_connection, historical_data_table, predicted_output_table, prediction_start_date, prediction_end_date, predict_tommorrow = get_prediction_req(data_storage_method)
        df = read_data_from_mysql(database_connection, historical_data_table)
        df_prediction = predict(df, prediction_start_date, prediction_end_date, predict_tommorrow)
        write_prediction_to_mysql(df_prediction, database_connection, predicted_output_table)
    elif data_storage_method == 'csv':
        historical_data_file, predicted_output_file, prediction_start_date, prediction_end_date, predict_tommorrow = get_prediction_req(data_storage_method)
        df = read_data_from_csv(historical_data_file)
        df_prediction = predict(df, prediction_start_date, prediction_end_date, predict_tommorrow)
        write_prediction_to_csv(df_prediction, predicted_output_file)
    else:
        sys.exit('Set DATA_STORAGE_METHOD etiher csv or mysql. This program only support to store data in csv or mysql.')

if __name__ == '__main__':
    main()
