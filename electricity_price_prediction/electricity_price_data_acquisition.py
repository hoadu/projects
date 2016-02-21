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
    data_path = settings.PRICE_DATA_PATH
    database_connection = settings.DATABASE_CONNECTION
    table = settings.PRICE_TABLE
    return data_path, database_connection, table

def download_files(data_path):
    try:
        response = requests.get('http://mis.nyiso.com/public/P-2Alist.htm')
    except HTTPError as e:
        print e
    content = response.content
    parser = BeautifulSoup(content, 'html.parser')
    for link in parser.find_all('a', href=True):
        href = link.get('href')
        if 'csv' in href and 'zip' in href and 'prev' not in href:
            response2 = requests.get('http://mis.nyiso.com/public/'+href)
            with open(data_path+href[12:], 'wb') as handle:
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
        prices = Table(table, metadata,
            Column('Date', DateTime, primary_key=True),
            Column('Name', String(32), primary_key=True),
            Column('PTID', Integer),
            Column('LBMP', Float),
            Column('Marginal_cost_losses', Float),
            Column('Marginal_cost_congestion', Float),)
        metadata.create_all(engine)
    else:
        prices = Table(table, metadata, autoload = True, autoload_with=engine)
    for ffile in os.listdir(data_path):
        if ffile.endswith('.csv'):
            with open(data_path+ffile, 'rb') as csvfile:
                csv_data = csv.reader(csvfile)
                for row in csv_data:
                    if 'Time Stamp' not in row:
                        selection = connection.execute(' select * from %s where Date=\'%s\' and Name=\'%s\' ' %(table, parse(row[0]), row[1]))
                        if len(selection.fetchall()) == 0:
                            ins = prices.insert()
                            connection.execute(ins, Date=parse(row[0]), Name=row[1], PTID=row[2], LBMP=row[3], Marginal_cost_losses=row[4], Marginal_cost_congestion=row[5])
    connection.close()


def main():
    data_path, database_connection, table = get_settings()
    download_files(data_path)
    extract_files(data_path)
    aggregate_data_to_database(data_path, database_connection, table)

if __name__ == '__main__':
    main()
