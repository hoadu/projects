import settings
import matplotlib.pylab as plt
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar as calendar
from pandas.tools.plotting import scatter_matrix
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD
from pyspark.mllib.evaluation import RegressionMetrics
from pyspark.mllib.stat import Statistics
from sklearn.linear_model import LinearRegression

def get_settings():
    db_name = settings.DATABASE_NAME
    table = settings.PRICE_TABLE
    db_user = settings.DATABASE_USER
    db_pass = settings.DATABASE_PASS
    zone_name_to_forecast = settings.ZONE_NAME_TO_FORECAST
    return db_name, table, db_user, db_pass, zone_name_to_forecast

def predict(db_name, table, db_user, db_pass, zone_name_to_forecast):
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    mysql_url = "jdbc:mysql://localhost:3306/"+db_name
    df = sqlContext.read.format('jdbc').options(url=mysql_url, driver = "com.mysql.jdbc.Driver", dbtable=table, user=db_user, password=db_pass).load()
    df_zone = df[df['Name'] == zone_name_to_forecast]
    pandas_df = df_zone.toPandas()
    pandas_df['Date'] = pd.to_datetime(pandas_df['Date'])
    pandas_df['year'] = pd.DatetimeIndex(pandas_df['Date']).year
    pandas_df['squared_losses'] = pandas_df['Marginal_cost_losses'] ** 2
    pandas_df['squared_congestion'] = pandas_df['Marginal_cost_congestion'] ** 2
    pandas_df['avg_price_day_1'] = pd.rolling_mean(pandas_df['LBMP'], window=24).shift(1)
    pandas_df['avg_price_day_7'] = pd.rolling_mean(pandas_df['LBMP'], window=168).shift(1)
    pandas_df['avg_price_day_28'] = pd.rolling_mean(pandas_df['LBMP'], window=672).shift(1)
    pandas_df['avg_price_day_364'] = pd.rolling_mean(pandas_df['LBMP'], window=8736).shift(1)
    pandas_df['std_price_day_7'] = pd.rolling_std(pandas_df['LBMP'], window=168).shift(1)
    pandas_df['std_price_day_28'] = pd.rolling_std(pandas_df['LBMP'], window=672).shift(1)
    pandas_df['std_price_day_364'] = pd.rolling_std(pandas_df['LBMP'], window=8736).shift(1)
    pandas_df['ratio_std_7_28'] = pandas_df['std_price_day_7'] / pandas_df['std_price_day_28']
    pandas_df['ratio_std_7_364'] = pandas_df['std_price_day_7'] / pandas_df['std_price_day_364']
    pandas_df['ratio_std_28_364'] = pandas_df['std_price_day_28'] / pandas_df['std_price_day_364']
    pandas_df['ratio_ave_7_364'] = pandas_df['avg_price_day_7'] / pandas_df['avg_price_day_364']
    pandas_df['ratio_ave_7_28'] = pandas_df['avg_price_day_7'] / pandas_df['avg_price_day_28']
    pandas_df['ratio_ave_28_364'] = pandas_df['avg_price_day_28'] / pandas_df['avg_price_day_364']
    pandas_df = pandas_df.dropna(axis=0)
    scatter_matrix(pandas_df)
    plt.scatter(pandas_df.index, pandas_df['LBMP'])
    for column in list(pandas_df)[4:]:
        pandas_df.plot(kind='scatter', x=column, y='LBMP')
    spark_df = sqlContext.createDataFrame(pandas_df)
    spark_df.printSchema()

    # calculate correlation
    data = spark_df.map(lambda p: LabeledPoint(p[3], p[4:]))
    numFeatures = data.take(1)[0].features.size
    labelRDD = data.map(lambda p: p.label)
    corrType = 'pearson'
    print '%s\t%s' % ('index', 'pearson')
    for i in range(numFeatures):
        featureRDD = data.map(lambda p: p.features[i])
        corr = Statistics.corr(labelRDD, featureRDD, corrType)
        print '%d\t%g' % (4+i, corr)

    df_prediction = pd.DataFrame()
    df_prediction['Actual'] = pandas_df.iloc[-24:]['LBMP']
    train = pandas_df[:-24]
    test = pandas_df[-24:]
    regressor = LinearRegression()
    train_predictors = train[list(train.columns[4:])]
    train_to_predict = train['LBMP']
    regressor.fit(train_predictors, train_to_predict)
    test_predictors = test[list(test.columns[4:])]
    df_prediction['Predicted_regression'] = regressor.predict(test_predictors)
    mape_regression = sum(abs(df_prediction['Actual'] - df_prediction['Predicted_regression'])/df_prediction['Actual']) / len(df_prediction['Predicted_regression'])
    print 'mape is %s' % mape_regression
    sc.stop()

def main():
    db_name, table, db_user, db_pass, zone_name_to_forecast = get_settings()
    predict(db_name, table, db_user, db_pass, zone_name_to_forecast)

if __name__ == '__main__':
    main()
