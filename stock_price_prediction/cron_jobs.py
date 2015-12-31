from crontab import CronTab

cron = CronTab()
job_data = cron.new(command='/Users/lilizhang/anaconda/bin/python /Users/lilizhang/documents/data_science_project_code/stock_price_data_acquisition.py')
job_data.setall('0 23 * * *')
job_prediction = cron.new(command='/Users/lilizhang/anaconda/bin/python /Users/lilizhang/documents/data_science_project_code/stock_price_prediction.py')
job_prediction.setall('0 1 * * *')
cron.write()
