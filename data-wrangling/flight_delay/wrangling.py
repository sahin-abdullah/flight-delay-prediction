# Import necessary libraries
import os
import time
import subprocess
import numpy as np
import pandas as pd
from utils import conv_timedelta, conv_type, create_report, df_man, df_par, df_datetime
import datetime as dt
from scipy import special
import multiprocessing as mp
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec


def acquire(date_start, date_end):
    """Downloads Airline On-Time Performance Data between given range 
    in a compressed zip format.

    Returns my_path: ~/data/flight_data and my_path2: ~/flight_delay
    my_path is a directory where all compressed zip files downloaded
    my_path2 is a directory where shell scripts (csv_process.sh) is

    Parameters
    ----------
    date_start : string
        Start Date
    date_end : string
        End Date
    """
    my_path = os.path.abspath(os.path.dirname(__file__))
    my_path = os.path.dirname(my_path)
    my_path = os.path.join(my_path, '' + 'data\\flight_data')

    options = webdriver.ChromeOptions()
    # options.add_argument("--start-fullscreen")
    options.add_argument("--headless")  
    options.add_argument('window-size=1920x1080')
    prefs = {"download.default_directory": my_path,
             "download.prompt_for_download": False,
             "download.directory_upgrade": True}

    options.add_experimental_option('prefs', prefs)
    # Use chrome as a web browser
    browser = webdriver.Chrome(
        executable_path="C:\\Program Files (x86)\\Google\\Chrome\\Application\\chromedriver", chrome_options=options)
    # Main page URL
    url = 'https://www.bts.gov/'
    # Get main page
    browser.get(url)
    print('Browsing the website...')
    # Browser wait
    wait = WebDriverWait(browser, 30)
    # Navigate to database directory
    wait.until(ec.element_to_be_clickable(
        (By.XPATH, "//ul[@id='main-menu']//li[@class='menu-8756']")))
    browser.find_element_by_xpath(
        "//ul[@id='main-menu']//li[@class='menu-8756']").click()

    x_path = "//div[contains(@class,'panel-pane pane-block pane-menu-menu-browse-statistics-statistic .region-footer " \
             "ul.menu pane-menu')]//a[contains(@class,'menu__link')][contains(text(),'A-Z Index')] "
    wait.until(ec.element_to_be_clickable((By.XPATH, x_path)))
    browser.find_element_by_xpath(x_path).click()

    x_path = "//a[contains(text(),'Airline On-Time Database')]"
    wait.until(ec.element_to_be_clickable((By.XPATH, x_path)))
    browser.find_element_by_xpath(x_path).click()

    # Navigate to data download page
    wait.until(ec.element_to_be_clickable((By.XPATH, "//tr[7]//td[2]//a[3]")))
    browser.find_elements_by_link_text('Download')[1].click()
    # Check the box for all zipped data
    wait.until(ec.element_to_be_clickable(
        (By.XPATH, "//input[@id='DownloadZip']")))
    browser.find_element_by_id('DownloadZip').click()
    # Select from dropdown menu
    select_year = Select(browser.find_element_by_id('XYEAR'))
    select_month = Select(browser.find_element_by_id('FREQUENCY'))

    # Convert string to int
    ds_y = int(date_start.split('-')[0])
    ds_m = int(date_start.split('-')[1])
    de_y = int(date_end.split('-')[0])
    de_m = int(date_end.split('-')[1])

    # Select by visible text in a loop and download the data
    try:
        for year in range(ds_y, de_y + 1, 1):
            for month in range(ds_m, 13, 1):
                select_year.select_by_visible_text(str(year))
                select_month.select_by_value(str(month))
                wait.until(ec.element_to_be_clickable(
                    (By.XPATH, "//button[@name='Download2']")))
                browser.find_element_by_xpath(
                    "//button[@name='Download2']").click()
                if (year == de_y) & (month == de_m):
                    raise StopIteration
            ds_m = 1
    except StopIteration:
        pass
    print('Downloading...')
    time.sleep(3)
    x1 = 0
    while x1 == 0:
        count = 0
        li = os.listdir(my_path)
        for x1 in li:
            if x1.endswith(".crdownload"):
                count = count + 1
        if count == 0:
            x1 = 1
        else:
            x1 = 0
    print('Downloading files is completed')
    # Close browser
    browser.close()

    my_path2 = os.path.dirname(os.path.dirname(my_path))
    my_path2 = os.path.join(my_path2, 'flight_delay')
    return my_path, my_path2


def combine_csv(path1, path2):
    """Unzips compressed files first and concatenates these files
    into one file. It removes any leftover files and folder.

    Returns nothing. Final file should be under ~/data/flight_data
    folder

    Parameters
    ----------
    path1 : string
        ~/data/flight_data
    path2 : string
        ~/flight_delay
    """
    temp = path2.split('\\')
    path2 = '/mnt/c/' + '/'.join(temp[1:])
    temp = path1.split('\\')
    path1 = '/mnt/c/' + '/'.join(temp[1:])
    cmd_line = path2 + '/csv_process.sh'
    # No need to have +x permission
    subprocess.run(['bash', cmd_line, path1])


def import_csv(p_fli, sd, ed):
    """Imports csv folders and concatenates them if there are
    multiple files between date range

    Returns df : pandas data frame 
                airline on-time performance
            airport : pandas data frame
                airport information
            icao : pandas data frame
                ICAO codes of airports
            n : int
                number of rows in df
            d : int
                number of columns in df
    Parameters
    ----------
    p_fli : string
        path to folder ~/data/flight_data
    sd : string
        Start Date
    ed : string
        End date
    """
    csv_files = pd.period_range(sd, ed, freq='Y').strftime('%Y')
    temp_list = []
    for idx in csv_files:
        temp_list.append(pd.read_csv(
            p_fli + '\\' + idx + '.csv', low_memory=False))
        print(idx + '.csv is imported')

    df = pd.concat(temp_list)
    print('All csv files are concatenated')
    del temp_list
    df.columns = ['WeekDay', 'Date', 'IATA', 'TailNum', 'FlightNum',
                  'OrgAirID', 'OrgMarID', 'DestAirID', 'DestMarID',
                  'ScDepTime', 'DepTime', 'DepDelay', 'TxO', 'WhOff',
                  'WhOn', 'TxI', 'ScArrTime', 'ArrTime', 'ArrDelay',
                  'Cncl', 'CnclCd', 'Div', 'ScElaTime', 'AcElaTime',
                  'AirTime', 'Dist', 'CarrDel', 'WeaDel', 'NASDel',
                  'SecDel', 'LatAirDel']
    # Airport Data frame
    p_misc = os.path.join(os.path.dirname(p_fli), '' + 'misc')
    airport = pd.read_csv(p_misc + '\\airport.csv',
                          usecols=[0, 1, 2, 3, 4, 5, 6, 11, 16, 17])
    icao = pd.read_csv(p_misc + '\\icao.csv', usecols=[4, 5])
    icao.columns = ['Code', 'ICAO']
    airport.columns = ['AirID', 'Code', 'Name', 'City',
                       'Country', 'State', 'CityMarketID', 'Lat', 'Long', 'UTC']
    airport = airport.drop_duplicates(subset='AirID', keep='last')
    n, d = df.shape
    return df, airport, icao, n, d


def init_check(df, airport, sd, ed, d):
    """Initially checks all entries in data frames whether
    they are out of bound, null entries, or both, then flags them.

    Returns df : pandas data frame 
                airline on-time performance
            airport : pandas data frame
                airport information
            flag : dictionary
                column flags 1 for null, 2 for out of bound, and 3 for both
            date3 : pandas series
                values of date column under flag 3. Will be removed later on

    Parameters
    ----------
    df : pandas data frame
        Airline on-time performance data
    airport : pandas data frame
        Airport data
    sd : string
        Start Date
    ed : string
        End date
    d : int
        Number of columns in df
    """

    flag = dict(zip(df.columns, np.zeros(d)))

    # List of numerical columns
    num_list = ['WeekDay', 'FlightNum', 'OrgAirID', 'OrgMarID',
                'DestAirID', 'DestMarID', 'ScDepTime', 'DepTime',
                'DepDelay', 'TxO', 'WhOff', 'WhOn', 'TxI', 'ScArrTime',
                'ArrTime', 'ArrDelay', 'Cncl', 'Div', 'ScElaTime',
                'AcElaTime', 'AirTime', 'Dist']

    # Turn off warning, since our goal is to change original data frame
    pd.set_option('mode.chained_assignment', None)
    # Convert all entries to numeric, return NaN if it is not numeric
    for i in num_list:
        df.loc[:, i] = pd.to_numeric(df.loc[:, i], errors='coerce')

    # Weekday has to be in between 1 and 7
    if df.WeekDay.isnull().sum() > 0:
        flag['WeekDay'] = 1
        if df[(df.WeekDay > 7) | (df.WeekDay < 1)].shape[0] > 0:
            flag['WeekDay'] = 3
    elif df[(df.WeekDay > 7) & (df.WeekDay < 1)].shape[0] > 0:
        flag['WeekDay'] = 2

    per = len(pd.period_range(sd, ed, freq='M'))
    ms = pd.date_range(sd, periods=per, freq='MS')[0].strftime('%Y-%m-%d')
    me = pd.date_range(sd, periods=per, freq='M')[-1].strftime('%Y-%m-%d')
    dt_range = list(pd.date_range(ms, me).strftime('%Y-%m-%d').values)
    date3 = []  # Initialize
    # Check incorrect and null date entries
    if df.Date.isnull().sum() > 0:
        flag['Date'] = 1
        if df.Date.isin(dt_range).sum() > 0:
            date3 = df.loc[~df.Date.isin(dt_range), 'Date']
            flag['Date'] = 3
    elif (~df.Date.isin(dt_range)).sum() > 0:
        date3 = df.loc[~df.Date.isin(dt_range), 'Date']
        flag['Date'] = 2

    # There is no way to recover null IATA entries, so it is best to remove them
    if df.IATA.isnull().sum() > 0:
        flag['IATA'] = 1

    # There is no way to recover null TailNum entries, so it is best to remove them
    if df.TailNum.isnull().sum() > 0:
        flag['TailNum'] = 1

    # AirportID varies between 10001 and 16878 (99999 for unknown)
    # CityMarketId varies between 30001 and 36845 (99999 for unknown)

    if df.OrgAirID.isnull().sum() > 0:
        flag['OrgAirID'] = 1
        if ((df.OrgAirID < 10001) | (df.OrgAirID > 16878)).sum() > 0:
            flag['OrgAirID'] = 3
    elif ((df.OrgAirID < 10001) | (df.OrgAirID > 16878)).sum() > 0:
        flag['OrgAirID'] = 2

    if df.DestAirID.isnull().sum() > 0:
        flag['DestAirID'] = 1
        if ((df.DestAirID < 10001) | (df.DestAirID > 16878)).sum() > 0:
            flag['DestAirID'] = 3
    elif ((df.DestAirID < 10001) | (df.DestAirID > 16878)).sum() > 0:
        flag['DestAirID'] = 2

    if df.OrgMarID.isnull().sum() > 0:
        flag['OrgMarID'] = 1
        if ((df.OrgMarID < 30001) | (df.OrgMarID > 36845)).sum() > 0:
            flag['OrgMarID'] = 3
    elif ((df.OrgMarID < 30001) | (df.OrgMarID > 36845)).sum() > 0:
        flag['OrgMarID'] = 2

    if df.DestMarID.isnull().sum() > 0:
        flag['DestMarID'] = 1
        if ((df.DestMarID < 30001) | (df.DestMarID > 36845)).sum() > 0:
            flag['DestMarID'] = 3
    elif ((df.DestMarID < 30001) | (df.DestMarID > 36845)).sum() > 0:
        flag['DestMarID'] = 2

    # Before assigning datetime object to ScDepTime, DepTime, ScArrTime,
    # WhOff, WhOn, and ArrTime, it must be in between 0 and 2400

    if df.ScDepTime.isnull().sum() > 0:
        flag['ScDepTime'] = 1
        if ((df.ScDepTime < 0) | (df.ScDepTime > 2400)).sum() > 0:
            flag['ScDepTime'] = 3
    elif ((df.ScDepTime < 0) | (df.ScDepTime > 2400)).sum() > 0:
        flag['ScDepTime'] = 2

    if df.DepTime.isnull().sum() > 0:
        flag['DepTime'] = 1
        if ((df.DepTime < 0) | (df.DepTime > 2400)).sum() > 0:
            flag['DepTime'] = 3
    elif ((df.DepTime < 0) | (df.DepTime > 2400)).sum() > 0:
        flag['DepTime'] = 2

    if df.WhOff.isnull().sum() > 0:
        flag['WhOff'] = 1
        if ((df.WhOff < 0) | (df.WhOff > 2400)).sum() > 0:
            flag['WhOff'] = 3
    elif ((df.WhOff < 0) | (df.WhOff > 2400)).sum() > 0:
        flag['WhOff'] = 2

    if df.WhOn.isnull().sum() > 0:
        flag['WhOn'] = 1
        if ((df.WhOn < 0) | (df.WhOn > 2400)).sum() > 0:
            flag['WhOn'] = 3
    elif ((df.WhOn < 0) | (df.WhOn > 2400)).sum() > 0:
        flag['WhOn'] = 2

    if df.ScArrTime.isnull().sum() > 0:
        flag['ScArrTime'] = 1
        if ((df.ScArrTime < 0) | (df.ScArrTime > 2400)).sum() > 0:
            flag['ScArrTime'] = 3
    elif ((df.ScArrTime < 0) | (df.ScArrTime > 2400)).sum() > 0:
        flag['ScArrTime'] = 2

    if df.ArrTime.isnull().sum() > 0:
        flag['ArrTime'] = 1
        if ((df.ArrTime < 0) | (df.ArrTime > 2400)).sum() > 0:
            flag['ArrTime'] = 3
    elif ((df.ArrTime < 0) | (df.ArrTime > 2400)).sum() > 0:
        flag['ArrTime'] = 2

    # Cancellation columns has to be 0 or 1
    if df.Cncl.isnull().sum() > 0:
        flag['Cncl'] = 1
        if (~df.Cncl.isin([0, 1])).sum() > 0:
            flag['Cncl'] = 3
    elif (~df.Cncl.isin([0, 1])).sum() > 0:
        flag['Cncl'] = 2

    # Cancellation codes are A, B, C, and D. Assign 0 to null entries
    df.loc[:, 'CnclCd'].replace([np.nan, 'A', 'B', 'C', 'D'],
                                [0, 1, 2, 3, 4],
                                inplace=True)

    if df.CnclCd.isnull().sum() > 0:
        flag['CnclCd'] = 1
        if (~df.CnclCd.isin(range(5))).sum() > 0:
            flag['CnclCd'] = 3
    elif (~df.CnclCd.isin(range(5))).sum() > 0:
        flag['CnclCd'] = 2

    # Div column has to be 0 or 1
    if df.Div.isnull().sum() > 0:
        flag['Div'] = 1
        if (~df.Div.isin([0, 1])).sum() > 0:
            flag['Div'] = 3
    elif (~df.Div.isin([0, 1])).sum() > 0:
        flag['Div'] = 2

    if df.ScElaTime.isnull().sum() > 0:
        flag['ScElaTime'] = 1

    # Fill null rows under all delay columns
    df.iloc[:, 26:] = df.iloc[:, 26:].fillna(0)
    print("Initial check is completed")
    return df, airport, flag, date3


def reval_nan(df):
    """This function checks whether null entries are
    related to cancelled and diverted flights or not.
    If they are related, it leaves them as it is, if
    it is not, it tries to recover them.

    Returns df : pandas data frame 
                Airline on-time performance

    Parameters
    ----------
    df : pandas data frame
        Airline on-time performance data
    """
    # These columns are null when a flight is cancelled
    canc_cols = ['DepTime', 'DepDelay', 'TxO', 'WhOff', 'WhOn',
                 'TxI', 'ArrTime', 'ArrDelay', 'AcElaTime', 'AirTime']
    # Assign nan values to the entries where it is supposed to be
    for col in canc_cols:
        df.loc[(df.Cncl == 1) & (df[col].notna()), canc_cols] = np.nan
    # These columns are null when a flight is cancelled
    div_cols = ['WhOn', 'TxI', 'ArrTime', 'ArrDelay', 'AcElaTime', 'AirTime']
    # Assign nan values to the entries where it is supposed to be
    for col in div_cols:
        df.loc[(df.Div == 1) & df[col].notna(), div_cols] = np.nan

    # cond = flights.ScDepTime > flights.ScArrTime
    # idx = ((flights.loc[cond, 'ScDepTime'] + flights.loc[cond, 'ScElaTime'] + flights.loc[cond, 'TimeZoneDiff']) - flights.loc[cond, 'ScArrTime']) >= pd.Timedelta('1 hours')
    # idx = np.where(idx, idx.index, 0)
    # idx = idx[idx != 0]
    # flights.drop(idx, inplace=True)

    # A condition for missing data
    def cond(x): return (pd.isna(df[x])) & ((df.Cncl != 1) | (df.Div != 1))
    df.loc[cond('DepTime'), 'DepTime'] = df.loc[cond('DepTime'),
                                                'ScDepTime'] + df.loc[cond('DepTime'), 'DepDelay']
    # df.loc[cond('DepTime'), 'DepTime'] = pd.NaT
    df.loc[cond('DepDelay'), 'DepDelay'] = df.loc[cond('DepDelay'),
                                                  'ScDepTime'] - df.loc[cond('DepDelay'), 'DepTime']
    # df.loc[cond('DepDelay'), 'DepDelay'] = pd.NaT
    df.loc[cond('TxO'), 'TxO'] = df.loc[cond('TxO'), 'WhOff'] - \
        df.loc[cond('TxO'), 'DepTime']
    # df.loc[cond('TxO'), 'TxO'] = pd.NaT
    df.loc[cond('WhOff'), 'WhOff'] = df.loc[cond('WhOff'),
                                            'DepTime'] + df.loc[cond('WhOff'), 'TxO']
    # df.loc[cond('WhOff'), 'WhOff'] = pd.NaT
    df.loc[cond('WhOn'), 'WhOn'] = df.loc[cond('WhOn'),
                                          'ArrTime'] - df.loc[cond('WhOn'), 'TxI']
    # df.loc[cond('WhOn'), 'WhOn'] = pd.NaT
    df.loc[cond('TxI'), 'TxI'] = df.loc[cond('TxI'),
                                        'ArrTime'] - df.loc[cond('TxI'), 'WhOn']
    # df.loc[cond('TxI'), 'TxI'] = pd.NaT
    df.loc[cond('ArrTime'), 'ArrTime'] = df.loc[cond('ArrTime'),
                                                'ScArrTime'] + df.loc[cond('ArrTime'), 'ArrDelay']
    # df.loc[cond('ArrTime'), 'ArrTime'] = pd.NaT
    df.loc[cond('ArrDelay'), 'ArrDelay'] = df.loc[cond('ArrDelay'),
                                                  'ArrTime'] - df.loc[cond('ArrDelay'), 'ScArrTime']
    # df.loc[cond('ArrDelay'), 'ArrDelay'] = pd.NaT
    df.DepTime = df.ScDepTime + df.DepDelay
    df.WhOff = df.DepTime + df.TxO
    mask = df.ScDepTime > df.ScArrTime
    df.loc[mask, 'ScArrTime'] = df.loc[mask, 'ScDepTime'] + df.loc[mask, 'ScElaTime'] + df.loc[mask, 'TimeZoneDiff']
    df.ArrTime = df.ScArrTime + df.ArrDelay
    df.WhOn = df.ArrTime - df.TxI

    return df


def recover(df, airport, flag, date3, n, d):
    """Removes and/or imputes data if specific conditions are met.


    Returns df : pandas data frame 
                Airline on-time performance

    Parameters
    ----------
    df : pandas data frame 
        Airline on-time performance
    airport : pandas data frame
        Airport information
    flag : dictionary
        Column flags 1 for null, 2 for out of bound, and 3 for both
    date3 : pandas series
        Values of date column under flag 3. Will be removed later on
    n : int
        Number of rows in df
    d : int
        Number of columns in df
    """
    st = time.time()
    print("Recovering data has started", end="\r")
    # Recovering WeekDay using Date column only for null entries
    if flag['WeekDay'] == 1:
        cond = df.WeekDay.isnull()
        t1 = df.loc[cond, 'Date']
        if t1.isnull().sum() > 0:
            # Return nan if NaT
            df.loc[cond, 'WeekDay'] = t1.dt.dayofweek + 1  # Zero indexed
            df = df.loc[df.WeekDay.notna(), :]

    # Recovering WeekDay using Date column for null and out of range entries
    elif flag['WeekDay'] == 3:
        cond = (df.WeekDay.isnull()) | (df.WeekDay < 1) | (df.WeekDay > 7)
        t1 = df.loc[cond, 'Date']
        if t1.isnull().sum() >= 0:
            # Return nan if NaT
            df.loc[cond, 'WeekDay'] = t1.dt.dayofweek + 1
            # Check there is still out of range entries
            cond = (df.WeekDay > 7) | (df.WeekDay < 1)
            # out of range + null entries
            df = df.loc[(df.WeekDay.notna()) | (df.WeekDay.loc[~cond]), :]

    # Recovering WeekDay using Date column only for out of range entries
    elif flag['WeekDay'] == 2:
        cond = (df.WeekDay < 1) | (df.WeekDay > 7)
        t1 = df.loc[cond, 'Date']
        if t1.isnull().sum() >= 0:
            # Return nan if NaT
            df.loc[cond, 'WeekDay'] = t1.dt.dayofweek + 1
            # Check there is still out of range entries
            cond = (df.WeekDay > 7) | (df.WeekDay < 1)
            # out of range + null entries
            df = df.loc[(df.WeekDay.notna()) | (df.WeekDay.loc[~cond]), :]

    # Removing null or out of range Date, IATA, TailNum, OrgAirID, 
    # DestAirID entries because not way to recover it
    if flag['Date']:
        df = df.loc[df.Date.notna(), :]
        df = df.loc[df.Date != date3, :]

    if flag['IATA']:
        df = df.loc[df.IATA.notna(), :]

    if flag['TailNum']:
        df = df.loc[df.TailNum.notna(), :]

    if flag['OrgAirID']:
        df = df.loc[df.OrgAirID.notna(), :]
        cond = (df.OrgAirID <= 10001) | (df.OrgAirID >= 16878)
        df = df.loc[~cond, :]

    if flag['DestAirID']:
        df = df.loc[df.DestAirID.notna(), :]
        cond = (df.DestAirID <= 10001) | (df.DestAirID >= 16878)
        df = df.loc[~cond, :]

    # Recovering City Market ID's using supplementary data frame, airport
    if flag['OrgMarID']:
        cond = (df.OrgMarID <= 30001) | (
            df.OrgMarID >= 36845) | (df.OrgMarID.isnull())
        ntemp = cond.sum()
        array = df.loc[cond, 'OrgAirID'].drop_duplicates().values
        names = airport[airport.AirID.isin(array)][['AirID', 'CityMarketID']]
        for idx, val in names.iterrows():
            # np.where is much faster!
            df.OrgMarID = np.where(
                cond | (df.OrgAirID == val[0]), val[1], df.OrgMarID.values)
        # Recheck condition (In case both OrgAirID and OrgMarID is not available)
        cond = (df.OrgMarID <= 30001) | (
            df.OrgMarID >= 36845) | (df.OrgMarID.isnull())
        df = df.loc[~cond, :]

    if flag['DestMarID']:
        cond = (df.DestMarID <= 30001) | (
            df.DestMarID >= 36845) | (df.DestMarID.isnull())
        ntemp = cond.sum()
        array = df.loc[cond, 'DestAirID'].drop_duplicates().values
        names = airport[airport.AirID.isin(array)][['AirID', 'CityMarketID']]
        for idx, val in names.iterrows():
            # np.where is much faster!
            df.DestMarID = np.where(
                cond | (df.DestAirID == val[0]), val[1], df.DestMarID.values)
        # Recheck condition (In case both DestAirID and DestMarID are not available)
        cond = (df.DestMarID <= 30001) | (
            df.DestMarID >= 36845) | (df.DestMarID.isnull())
        df = df.loc[~cond, :]

    if flag['Div']:
        cond = (df.Div.isnull()) | (df.Div > 1) | (df.Div < 0)
        cols = ['ArrTime', 'ArrDelay', 'AcElaTime', 'AirTime', 'TxI', 'WhOn']
        var = df.loc[cond, :][cols]
        if (var.isnull().sum().isin([6]).sum()) == (df.loc[cond, :].shape[0]):
            df.loc[cond, 'Div'] = 1
        else:
            df = df.loc[~cond, :]
        df.loc[df.Div == 1, cols] = np.nan

    df.loc[df.Div == 1, 'CnclCd'] = 0

    if flag['Cncl']:
        cond = (df.Cncl.isnull()) | (df.Cncl > 1) | (df.Cncl < 0)
        cols = ['DepTime', 'DepDelay', 'TxO', 'WhOff', 'ArrTime',
                'ArrDelay', 'AcElaTime', 'AirTime', 'TxI', 'WhOn']
        var = df.loc[cond, :][cols]
        if (var.isnull().sum().isin([10]).sum()) == (df.loc[cond, :].shape[0]):
            df.loc[cond, 'Cncl'] = 1
        else:
            df = df.loc[~cond, :]

    cols = ['DepTime', 'DepDelay', 'TxO', 'WhOff', 'ArrTime',
            'ArrDelay', 'AcElaTime', 'AirTime', 'TxI', 'WhOn']
    df.loc[df.Cncl == 1, cols] = np.nan

    # There is no way to guess cancellation code
    if flag['CnclCd']:
        cond = (df.CnclCd.notna()) | (df.CnclCd.isin(range(5)))
        df = df.loc[cond, :]
    # There is no way to guess Scheduled Departure Time
    if flag['ScDepTime']:
        cond = (df.ScDepTime.isnull()) | (
            df.ScDepTime < 0) | (df.ScDepTime > 2400)
        df = df.loc[~cond, :]
    # There is no way to guess Scheduled Arrival Time
    if flag['ScArrTime']:
        cond = (df.ScArrTime.isnull()) | (
            df.ScArrTime < 0) | (df.ScArrTime > 2400)
        df = df.loc[~cond, :]
    # There is no way to guess Scheduled Elapsed Time
    if flag['ScElaTime']:
        cond = df.ScElaTime.isnull()
        df = df.loc[~cond, :]

    # Let's shrink size of the dataframe!
    types = {0: 'int8', 1: object, 2: 'category', 3: 'category', 4: 'int16', 5: 'int16',
             6: 'int32', 7: 'int16', 8: 'int32', 19: 'bool', 20: 'category', 21: 'bool',
             25: 'int16', 26: 'int16', 27: 'int16', 28: 'int16', 29: 'int16', 30: 'int16'}
    for key, val in types.items():
        conv_type(df, key, val)

    # Let's shrink size of the dataframe!
    types = {0: 'int16', 1: object, 2: object, 3: object, 4: object, 5: object, 6: 'int32',
             7: 'float32', 8: 'float32', 9: 'float32'}
    for key, val in types.items():
        conv_type(airport, key, val)

    cols = ['ScDepTime', 'ScArrTime', 'DepTime', 'ArrTime', 'WhOff', 'WhOn']
    for name in cols:
        df[name] = df_man(df[name])

    df['Date'] = df.Date.str.replace('-', '')

    for col_name in cols:
        df[col_name] = df.Date + ' ' + df[col_name]

    cols = ['DepDelay', 'TxO', 'TxI', 'ArrDelay',
        'ScElaTime', 'AcElaTime', 'AirTime']
    for col in cols:
        conv_timedelta(df, col, 'min')
    # for name in cols:
    #     df.loc[:, name] = df_par(df.loc[:, name], pd.to_timedelta)

    # No need to have Date column
    df = df.drop(columns=['Date'])
    # Temp assignment for re-evaluation of nan entries
    # Actual arrival time might be the day after
    cols = ['ScDepTime', 'ScArrTime', 'DepTime', 'ArrTime', 'WhOff', 'WhOn']
    for col_name in cols:
        df[col_name] = df_datetime(df[col_name])

    df = df.reset_index(drop=True)
    utc_org = pd.merge(df, airport[['AirID', 'UTC']], left_on='OrgAirID',
                       right_on='AirID', how='left')['UTC']
    utc_dest = pd.merge(df, airport[['AirID', 'UTC']], left_on='DestAirID',
                        right_on='AirID', how='left')['UTC']
    diff = utc_dest - utc_org
    df['TimeZoneDiff'] = pd.to_timedelta((diff / 100) * 60, unit='m')
    # Re-evaluate nans
    df = reval_nan(df)
    df = df.reset_index(drop=True)
    et = time.time()
    print("Recovering data has completed in {:.2f} seconds".format(et-st))
    return df.sort_values('ScDepTime')
