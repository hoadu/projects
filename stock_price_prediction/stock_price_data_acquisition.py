import stock_price_project_settings as setting
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, Table, Column, DateTime, Float, BigInteger, MetaData
import csv
from dateutil.parser import parse
import sys
import os

def get_query_input(data_storage_method):
    start_date = parse(setting.QUERY_START_DATE)
    end_date = parse(setting.QUERY_END_DATE)
    if start_date <= end_date:
        a = start_date.month - 1
        b = start_date.day
        c = start_date.year
        d = end_date.month - 1
        e = end_date.day
        f = end_date.year
        first_link = '/q/hp?s=%5EGSPC&' + 'a=%s&b=%s&c=%s&d=%s&e=%s&f=%s&g=d' % (a,b,c,d,e,f)
        if data_storage_method == 'mysql':
            output_table = setting.HISTORICAL_DATA_TABLE
            database_connection = setting.DATABASE_CONNECTION
            return output_table, database_connection, first_link
        else:
            output_file = setting.HISTORICAL_DATA_FILE
            return output_file, first_link
    else:
        sys.exit('Start date should be earlier than or same as end date. Review your inputs and re-execute the program.')

def scrape_page_data(pageUrl):
    try:
        response = requests.get('https://finance.yahoo.com'+pageUrl)
    except HTTPError as e:
        print e
    content = response.content
    parser = BeautifulSoup(content, 'html.parser')
    try:
        table = parser.select('.yfnc_datamodoutline1')[0]
    except:
        print 'Table cannot be found in the link https://finance.yahoo.com%s. This may be caused by network problem. Try later.' % pageUrl
    table_heads = table.select('th')
    table_head = [table_head.text.strip().strip("*").encode('utf-8') for table_head in table_heads]
    table_rows = table.select('tr')[1:]
    page_table = []
    for table_row in table_rows[1:-1]:
        row = []
        for index, table_data in enumerate(table_row):
            if index == 0:
                table_data = parse(table_data.text.strip().encode('utf-8'))
                row.append(table_data)
            else:
                table_data = table_data.text.strip().replace(',', '').encode('utf-8')
                row.append(table_data)
        page_table.append(row)
    new_page = parser.find('a', rel='next')
    return table_head, page_table, new_page

def aggregate_data_to_mysql(total_table, pageUrl, output_table, database_connection):
    table_head, page_table, new_page = scrape_page_data(pageUrl)
    for row in page_table:
        total_table.append(row)
    if new_page != None:
        new_page_link = new_page.attrs['href']
        aggregate_data_to_mysql(total_table, new_page_link, output_table, database_connection)
    else:
        engine = create_engine(database_connection)
        connection = engine.connect()
        metadata = MetaData()
        if not engine.dialect.has_table(connection, output_table):
            prices = Table(output_table, metadata,
                Column('Date', DateTime, primary_key=True),
                Column('Open', Float),
                Column('High', Float),
                Column('Low', Float),
                Column('Close', Float),
                Column('Volume', BigInteger),
                Column('Adj_close', Float),)
            metadata.create_all(engine)
        else:
            prices = Table(output_table, metadata, autoload = True, autoload_with=engine)
        for row in total_table:
            ins = prices.insert()
            connection.execute(ins, Date=row[0], Open=row[1], High=row[2], Low=row[3], Close=row[4], Volume=row[5], Adj_close=row[6])
        connection.close()

def aggregate_data_to_csv(total_table, pageUrl, output_file):
    table_head, page_table, new_page = scrape_page_data(pageUrl)
    for row in page_table:
        total_table.append(row)
    if new_page != None:
        new_page_link = new_page.attrs['href']
        aggregate_data_to_csv(total_table, new_page_link, output_file)
    else:
        with open(output_file, 'a') as f:
            writer = csv.writer(f)
            if os.stat(output_file).st_size == 0:
                writer.writerow(table_head)
            for row in total_table:
                writer.writerow(row)

def main():
    total_table = []
    data_storage_method = setting.DATA_STORAGE_METHOD
    if data_storage_method == 'mysql':
        output_table, database_connection, first_link = get_query_input(data_storage_method)
        aggregate_data_to_mysql(total_table, first_link, output_table, database_connection)
    elif data_storage_method == 'csv':
        output_file, first_link = get_query_input(data_storage_method)
        aggregate_data_to_csv(total_table, first_link, output_file)
    else:
        sys.exit('Set DATA_STORAGE_METHOD etiher csv or mysql. This program only support to store data in csv or mysql.')

if __name__ == '__main__':
    main()
