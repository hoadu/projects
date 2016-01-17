import settings
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row

def get_settings():
    db_name = settings.DATABASE_NAME
    table = settings.TABLE
    db_user = settings.DATABASE_USER
    db_pass = settings.DATABASE_PASS
    zone_name_to_forecast = settings.ZONE_NAME_TO_FORECAST
    return db_name, table, db_user, db_pass, zone_name_to_forecast

def prediction(db_name, table, db_user, db_pass, zone_name_to_forecast):
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    mysql_url = "jdbc:mysql://localhost:3306/"+db_name
    df = sqlContext.read.format('jdbc').options(url=mysql_url, driver = "com.mysql.jdbc.Driver", dbtable=table, user=db_user, password=db_pass).load()
    df_zone = df[df['Name'] = zone_name_to_forecast]

    df_zone.printSchema()

def main():
    db_name, table, db_user, db_pass, zone_name_to_forecast = get_settings()
    prediction(db_name, table, db_user, db_pass, zone_name_to_forecast)

if __name__ == '__main__':
    main()
