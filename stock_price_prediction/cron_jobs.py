from crontab import CronTab
import settings as setting

cron = CronTab()
job_data = cron.new(command = setting.CRON_DATA_JOB_COMMAND)
job_data.setall('0 23 * * *')
job_prediction = cron.new(command = setting.CRON_PREDICTION_JOB_COMMAND)
job_prediction.setall('0 1 * * *')
cron.write()
