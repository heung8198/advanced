import os
import sys
import time
import pandas as pd
import numpy as np
import datetime as datetime
import psycopg2
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
#from webdriver_manager.chrome import ChromeDriverManager

from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By

import params as pa

pd.set_option('display.width', 5000)
pd.set_option('display.max_rows', 5000)
pd.set_option('display.max_columns', 5000)


def driversetting(DownloadPath):

    # Selenium Setting
    options = webdriver.ChromeOptions()
    options.add_experimental_option("prefs",
                                    {"download.default_directory": DownloadPath,
                                     "download.prompt_for_download": False,
                                     "download.directory_upgrade": True,
                                     "safebrowsing_for_trusted_sources_enabled": False,
                                     "safebrowsing.enabled": False})

    #options.add_argument('headless')  # If you want to hide it, show it.
    #options.add_argument('--no-sandbox')
    #options.add_argument('--disable-dev-shm-usage')
    #options.add_argument("start-maximized")

    service = Service()
    driver = webdriver.Chrome(service=service, options=options)

    return driver


def fileremover():
    file2 = os.listdir(pa.DownloadPath)
    for f2 in file2:
        if f2.startswith('TimeData'):
            FileName = os.path.join(pa.DownloadPath, f2)
            os.remove(FileName)
    return []


def GetGenData(TargetDay,Farm): # TargetDay,Method

    driver = driversetting(pa.DownloadPath)

    driver.get(pa.HYOSUNG)
    print('run website')
    time.sleep(pa.waitseconds)

    driver.find_element(By.XPATH, '//*[@id="Txt_1"]').send_keys('jarasolar')
    driver.find_element(By.XPATH, '//*[@id="Txt_2"]').send_keys('abcd1234')
    driver.find_element(By.XPATH, '//*[@id="imageField"]').click()

    print('login')
    time.sleep(pa.waitseconds)


    print('Popup')
    try:
        driver.find_element(By.XPATH, '// *[ @ id = "popupContainer"] / div / div / div / div[2] / button[1]').click()

    except Exception as err:
        print(err)
        print("there is no popup")

    driver.find_element(By.XPATH, '//*[@id="form1"]/div[4]/div[1]/div/ul[2]/a[5]/li').click()
    print('Statistical Report')
    time.sleep(pa.waitseconds)

    # 16MW
    driver.find_element(By.XPATH, '//*[@id="SrTop_cbo_plant"]/option[' + str(Farm) + ']').click()
    print('Select Farm')
    time.sleep(pa.waitseconds)

    # Date Clear
    driver.find_element(By.XPATH, '//*[@id="txt_Calendar"]').clear()
    time.sleep(pa.waitseconds)

    # Date Input
    driver.find_element(By.XPATH, '//*[@id="txt_Calendar"]').send_keys(TargetDay)
    print('Put new date')
    time.sleep(pa.waitseconds)

    # Close the calendar
    driver.find_element(By.XPATH,'//*[@id="txt_Calendar"]').send_keys(Keys.ENTER)
    print('Close the calender')
    time.sleep(pa.waitseconds)

    # Search
    driver.find_element(By.XPATH, '//*[@id="submitbtn"]').click()
    print('Search')
    time.sleep(pa.waitseconds)

    # Cleaning
    fileremover()

    # Download
    driver.find_element(By.XPATH, '//*[@id="exldownbtn"]').click()
    # driver.find_element(By.XPATH,'//*[@id="exldownbtn"]').click()
    print('Download')
    time.sleep(pa.waitseconds)

    PreNow = datetime.datetime.today()
    Today = PreNow.strftime("%Y-%m-%d")

    count=0
    while 1:
        file = os.listdir(pa.DownloadPath)
        if len(file) == 0:
            count = count + 1
            time.sleep(pa.waitseconds)
            if count == 10:
                break

        elif len(file) > 0:
            TargetFile = 'TimeData_'+Today+'.xls'

            if TargetFile in file:
                breaker = 0
                for f2 in file:
                    if f2 == TargetFile:
                        FileName=os.path.join(pa.DownloadPath,f2)
                        breaker =1
                        break
                if breaker == 1:
                    break

    Data = genuploader(FileName, Farm)

    fileremover()
    driver.close()
    return Data


'''
def genuploader(FilePath, Farm):

    # Bring DB
    conn = psycopg2.connect(host=pa.host, dbname=pa.dbname, user=pa.user, password=pa.password, port=pa.port)
    cur = conn.cursor()

    # Safe Margin two hours
    Now = datetime.datetime.today() - datetime.timedelta(hours=3)

    # Read
    # lxml

    xls_pv_html = pd.read_html(FilePath)[0]
    # Manipulation
    Result = pd.DataFrame(columns=['target', 'actual'])
    Result = Result.assign(target=xls_pv_html['시간', '시간'])
    Result = Result.assign(actual=xls_pv_html['생산량(kWh)', 'PV 발전량'])
    Result = Result.assign(actual=Result['actual'].round(0))
    Result.index = range(0, len(Result))

    RealGen = np.where(Result['target'] != 'Sum')
    Result = Result.loc[RealGen[0], :]

    Result = Result.assign(target=pd.to_datetime(Result["target"],format='%Y-%m-%d %H:%M', utc=False).dt.tz_localize(None))
    Result = Result.assign(site_id=Farm)


    for i in range(0, len(Result)):
        # print(Result.loc[i:i,:]
        Target = Result.loc[i, 'target']
        SiteID = Result.loc[i, 'site_id']
        Actual = Result.loc[i, 'actual']

        # To make it sure, we only accept past data.
        if Target > Now:
            continue

        select_all_sql = f"select EXISTS(select * from solar " \
                         f"where target = TIMESTAMP '%s' AND site_id = %s)" % (Target, SiteID)

        cur.execute(select_all_sql)
        Exists = cur.fetchone()[0]

        if not Exists:
            print("Upload: ",Target, Actual, SiteID)
            query = """ INSERT INTO solar (target,actual,site_id) values (TIMESTAMP '%s',%s,%s) """ % (
            Target, Actual, SiteID)
            cur.execute(query)
        else:
            print("Duplicated ",Target, Actual, SiteID)

    conn.commit()
    cur.close()
    conn.close()

    return Result
'''
#postgresql에 data올리는 코드
#postgresql이 전혀 깔리지 않아서.. 일단 Data라는 변수에 받은 발전량 데이터를 저장한다.

# Postgresql이 아니라 그냥 Data에 excel로부터 받아온 값을 저장하는 함수
def genuploader(FilePath, Farm):

    # Safe Margin two hours
    Now = datetime.datetime.today() - datetime.timedelta(hours=3)

    # Read
    # lxml

    xls_pv_html = pd.read_html(FilePath)[0]
    # Manipulation
    Result = pd.DataFrame(columns=['target', 'actual'])
    Result = Result.assign(target=xls_pv_html['시간', '시간'])
    Result = Result.assign(actual=xls_pv_html['생산량(kWh)', 'PV 발전량'])
    Result = Result.assign(actual=Result['actual'].round(0))
    Result.index = range(0, len(Result))

    RealGen = np.where(Result['target'] != 'Sum')
    Result = Result.loc[RealGen[0], :] #Sum열을 빼고 DataFrame 만드는 과정

    Result = Result.assign(target=pd.to_datetime(Result["target"],format='%Y-%m-%d %H:%M', utc=False).dt.tz_localize(None))
    Result = Result.assign(site_id=Farm)

    return Result

if __name__ == '__main__':


    Farm = 1
    TargetDay = '2023-03-03'
    Data = GetGenData(TargetDay, Farm)
    print(Data)