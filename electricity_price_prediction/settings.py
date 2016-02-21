# The path you want to download the files to. It must exist.
PRICE_DATA_PATH  = '/Users/lili/documents/data/electricity_price/'
LOAD_DATA_PATH = '/Users/lili/documents/data/electricity_load/'
# The database connection with database user information, password, and database name. The database must exist.
DATABASE_CONNECTION = 'mysql+mysqlconnector://lili:123@127.0.0.1/electricity'
# The database table name. It will be created automatically.
PRICE_TABLE = 'electricity_price'
LOAD_TABLE = 'electricity_load'
# The database name
DATABASE_NAME = 'electricity'
# The database user
DATABASE_USER = 'lili'
# The database password for the user specified
DATABASE_PASS = '123'
# The Zone that will be forecasted
ZONE_NAME_TO_FORECAST = 'WEST'
