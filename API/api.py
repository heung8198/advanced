import datetime

import numpy as np
import pandas as pd
import psycopg2
import requests
import urllib
import json
from urllib.parse import urlencode, unquote, quote_plus
from urllib.request import urlopen
import datetime

Key = '21e5680bed8b4eb8aff140ba7921d529'
HourlyPresent = '/current'
BasicUrl = 'https://api.weatherbit.io/v2.0'


def CurrentCollector():

    #39.07365, 125.74115

    LAT = 39.07365
    LNG = 125.74115

    params = '?' + urlencode({quote_plus("lat"): str(LAT),
                              quote_plus("LNG"): str(LNG),
                              quote_plus("key"): Key,
                              quote_plus("include"): 'minutely'})

    FinalURL = BasicUrl + HourlyPresent + unquote(params)
    req = urllib.request.Request(FinalURL)

    response_body = urlopen(req).read()
    data = json.loads(response_body)

    DF = pd.DataFrame.from_dict(data['data'])

    del DF['ts']

    DF2 = DF[['lon', 'lat', 'temp', 'dewpt']]

    PreNow = datetime.datetime.today()
    PreNow=PreNow.replace(second=0, minute=0,microsecond=0)
    DF2=DF2.assign(Target = PreNow)

    print(DF2)

    return DF2


if __name__ == '__main__':

    CurrentCollector()
