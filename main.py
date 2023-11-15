import params as pa
import pandas as pd
import numpy as np
import os
import time
import datetime as datetime
import sys
from apscheduler.schedulers.background import BackgroundScheduler

from Crawl import cr as cr


pd.set_option('display.width', 5000)
pd.set_option('display.max_rows', 5000)
pd.set_option('display.max_columns', 5000)


def mycrawl(dti):
    Farm=1
    TargetDay=dti[pa.rotation]
    Data=cr.GetGenData(TargetDay.strftime("%Y-%m-%d"),Farm)
    pa.rotation = pa.rotation+1
    return Data


if __name__ == '__main__':
    arg = sys.argv[1:]
    if len(arg) > 0:
        print(arg[1])
        StartDay = arg[1]
        EndDay = arg[2]

    else:
        StartDay = '2023-03-01'
        EndDay = '2023-03-31'

    sched = BackgroundScheduler(timezone="Asia/Seoul")

    dti = pd.date_range(start=StartDay, end=EndDay, freq="1D")

    mycrawl(dti)
    #sched.add_job(gen, 'cron', minute='33', id=Container, args=[Container], max_instances=1)
    sched.add_job(mycrawl, 'interval', minutes=3, id='Solar', args=[dti], max_instances=1)
    sched.start()


    while True:
        Now = datetime.datetime.today()
        Now = Now.replace( microsecond=0)
        print(dti[pa.rotation], Now )
        time.sleep(10)