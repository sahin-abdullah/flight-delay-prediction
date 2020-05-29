# Utile functions
import numpy as np
import pandas as pd
import multiprocessing as mp


def enable_download_headless(browser, download_dir):
    """
    This function is mandatory to download files on chromedriver in headless mode
    """
    browser.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')
    params = {'cmd': 'Page.setDownloadBehavior', 'params': {'behavior': 'allow', 'downloadPath': download_dir}}
    browser.execute("send_command", params)


# Convert to timedelta
def conv_timedelta(df, col, metric):
    df[col] = pd.to_timedelta(df[col], unit=metric)
    return


# Convert dtype
def conv_type(df, col, typ):
    df.iloc[:, col] = df.iloc[:, col].astype(typ)
    return


def create_report(df):
    temp = df.describe()
    # Create a data frame to nicely show summary of the data
    d = {'Data Type': df.dtypes, 'Null Entries': df.isnull().sum() / df.shape[0] * 100,
         'Minimum': temp.loc['min'], '25th Percentile': temp.loc['25%'],
         '75th Percentile': temp.loc['75%'], 'Maximum': temp.loc['max']}
    summary = pd.DataFrame(data=d, index=df.columns)
    summary['Null Entries'] = summary['Null Entries'].map('{:,.4f}%'.format)
    return summary


def df_man(df):
    df = df.astype(str).replace('\.0', '', regex=True)
    df = df.applymap(lambda x: x.zfill(4))
    return df


def df_par(df, func):
    df_split = np.array_split(df, mp.cpu_count())
    pool = mp.Pool(mp.cpu_count())
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    del df_split
    return df


def df_datetime(df):
    mask = df.str.contains(' 24')
    df.loc[mask] = df.loc[mask].str.replace(' 24', ' 23')
    df = pd.to_datetime(df, errors='coerce', format='%Y%m%d %H%M')
    df.loc[mask] = df.loc[mask] + pd.Timedelta(hours=1)
    return df
