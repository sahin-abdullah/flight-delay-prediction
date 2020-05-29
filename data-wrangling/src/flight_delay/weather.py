import time
import numpy as np
import progressbar
import pandas as pd
from urllib.request import urlopen


def get_data(df, airport, icao, sd, ed):
    uniqs = uniq_id(airport, df['OrgAirID'], df['DestAirID'])
    uniqs = pd.merge(airport, uniqs)
    uniqs = pd.merge(uniqs, icao, how='left')
    uniqs.loc[uniqs.ICAO.isnull(), 'ICAO'] = 'K' + uniqs.loc[uniqs.ICAO.isnull()].Code.values
    df['Temp_O'], df['DewPoint_O'], df['RelHum_O'], df['HeatInd_O'], df['Dir_O'], df['WindSp_O'], df['Visib_O'], \
        df['Temp_D'], df['DewPoint_D'], df['RelHum_D'], df['HeatInd_D'], df['Dir_D'], df['WindSp_D'], df['Visib_D'] = [np.nan] * 14
    bar = progressbar.ProgressBar(max_value=uniqs.shape[0])
    for index, row in uniqs.iterrows():
        uri = uriformat(row, sd, ed)
        data = download_data(uri)
        out = open('temp.csv', "w")
        out.write(data)
        out.close()
        weather = pd.read_csv('temp.csv', sep=',', low_memory=False)
        weather = weather[weather.isnull().sum(axis=1) == 0]
        weather.columns = ['Station', 'Date', 'Temp', 'DewPoint', 'RelHum', 'HeatInd', 'Dir', 'WindSp', 'Visib']
        weather['Date'] = pd.to_datetime(weather['Date'])
        cond = (df.OrgAirID == row.AirID)
        df = df.sort_values('ScDepTime')
        temp = pd.merge_asof(df.loc[cond].iloc[:, :31], weather, left_on='ScDepTime',
                             right_on='Date', direction='nearest')
        temp = temp[['Temp', 'DewPoint', 'RelHum', 'HeatInd', 'Dir', 'WindSp', 'Visib']]
        df.loc[cond, ['Temp_O', 'DewPoint_O', 'RelHum_O', 'HeatInd_O', 'Dir_O', 'WindSp_O', 'Visib_O']] = temp.values
        cond = (df.DestAirID == row.AirID)
        df = df.sort_values('ScArrTime')
        temp = pd.merge_asof(df.loc[cond].iloc[:, :31], weather, left_on='ScArrTime',
                             right_on='Date', direction='nearest')
        temp = temp[['Temp', 'DewPoint', 'RelHum', 'HeatInd', 'Dir', 'WindSp', 'Visib']]
        df.loc[cond, ['Temp_D', 'DewPoint_D', 'RelHum_D', 'HeatInd_D', 'Dir_D', 'WindSp_D', 'Visib_D']] = temp.values
        del temp
        bar.update(index+1)
    return df.sort_values('ScDepTime')


def uniq_id(airport, orig, dest):
    all_ids = orig.append(dest)
    all_ids = all_ids.drop_duplicates()
    all_ids = all_ids.to_frame(name='AirID')
    all_ids = pd.merge(airport, all_ids)
    return all_ids


# def date_range(df, airport):
#     for idx in range(airport.shape[0]):
#         min_date = np.nanmin([df[df.OrgAirID == airport.loc[idx, 'AirID']].ScDepTime.min(),
#                               df[df.DestAirID == airport.loc[idx, 'AirID']].ScArrTime.min()])
#         airport.loc[idx, 'StartDate'] = (min_date - np.timedelta64(1, 'D')).strftime('%Y-%m-%d')
#         max_date = np.nanmin([df[df.OrgAirID == airport.loc[idx, 'AirID']].ScDepTime.max(),
#                               df[df.DestAirID == airport.loc[idx, 'AirID']].ScArrTime.max()])
#         airport.loc[idx, 'EndDate'] = (max_date + np.timedelta64(1, 'D')).strftime('%Y-%m-%d')
#     return airport


def uriformat(airport, sd, ed):
    station = airport.ICAO
    year1 = sd[:4]
    month1 = sd[5:7]
    day1 = sd[8:10]
    year2 = ed[:4]
    month2 = ed[5:7]
    day2 = ed[8:10]
    uri = 'https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?station={stationID}&data=tmpf' \
          '&data=dwpf&data=relh&data=feel&data=drct&data=sped&data=vsby&year1={year_start}&month1={month_start}' \
          '&day1={day_start}&year2={year_end}&month2={month_end}&day2={day_end}&tz=Etc%2FUTC&format=onlycomma' \
          '&latlon=no&missing=null&trace=T&direct=no&report_type=1&report_type=2'.format(stationID=station,
                                                                                         year_start=year1,
                                                                                         month_start=month1,
                                                                                         day_start=day1,
                                                                                         year_end=year2,
                                                                                         month_end=month2,
                                                                                         day_end=day2)
    return uri


def download_data(uri):
    """Fetch the data from the IEM
    The IEM download service has some protections in place to keep the number
    of inbound requests in check.  This function implements an exponential
    backoff to keep individual downloads from erroring.
    Args:
      uri (string): URL to fetch
    Returns:
      string data
    """
    attempt = 0
    while attempt < 6:
        try:
            data = urlopen(uri, timeout=300).read().decode("utf-8")
            if data is not None and not data.startswith("ERROR"):
                return data
        except Exception as exp:
            print("download_data(%s) failed with %s" % (uri, exp))
            time.sleep(5)
        attempt += 1

    print("Exhausted attempts to download, returning empty data")
    return ""
