import settings
import requests
from bs4 import BeautifulSoup
import zipfile
import os
from sqlalchemy import create_engine, Table, Column, DateTime, Float, BigInteger, MetaData, String, Integer
import csv
from dateutil.parser import parse
from sqlalchemy.sql import text

def get_settings():
    data_path = settings.LOAD_DATA_PATH
    database_connection = settings.DATABASE_CONNECTION
    table = settings.LOAD_TABLE
    return data_path, database_connection, table

def download_files(data_path):
    try:
        response = requests.get('http://mis.nyiso.com/public/P-58Blist.htm')
        print 'Go to main page'
    except HTTPError as e:
        print e
    content = response.content
    parser = BeautifulSoup(content, 'html.parser')
    for link in parser.find_all('a', href=True):
        href = link.get('href')
        if 'zip' in href:
            response2 = requests.get('http://mis.nyiso.com/public/'+href)
            print 'download %s ' % href
            with open(data_path+href[8:], 'wb') as handle:
                handle.write(response2.content)

def extract_files(data_path):
    for ffile in os.listdir(data_path):
        if ffile.endswith('.zip'):
            zip_ref = zipfile.ZipFile(data_path+ffile, 'r')
            zip_ref.extractall(data_path)
            zip_ref.close()

def aggregate_data_to_database(data_path, database_connection, table):
    engine = create_engine(database_connection)
    connection = engine.connect()
    metadata = MetaData()
    if not engine.dialect.has_table(connection, table):
        load = Table(table, metadata,
            Column('Date', DateTime, primary_key=True),
            Column('Time_zone', String(10)),
            Column('Name', String(32), primary_key=True),
            Column('Load', Float),)
        metadata.create_all(engine)
    else:
        load = Table(table, metadata, autoload = True, autoload_with=engine)
    for ffile in os.listdir(data_path):
        if ffile.endswith('.csv'):
            with open(data_path+ffile, 'rb') as csvfile:
                csv_data = csv.reader(csvfile)
                for row in csv_data:
                    if 'Time Stamp' not in row:
                        selection = connection.execute(' select * from %s where Date=\'%s\' and Name=\'%s\' ' %(table, parse(row[0]), row[2]))
                        if len(selection.fetchall()) == 0:
                            ins = load.insert()
                            if row[4] != '':
                                connection.execute(ins, Date=parse(row[0]), Time_zone=row[1], Name=row[2], Load=row[4])
                            else:
                                connection.execute(ins, Date=parse(row[0]), Time_zone=row[1], Name=row[2], Load=0.0)
    connection.close()


def main():
    data_path, database_connection, table = get_settings()
    #download_files(data_path)
    #extract_files(data_path)
    aggregate_data_to_database(data_path, database_connection, table)

if __name__ == '__main__':
    main()
