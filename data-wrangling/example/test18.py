import os
import sys

parent_dir = '..'
script_dir = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(os.path.join(script_dir, parent_dir), 'src')))
# from src.acquire import acquire
from flight_delay import wrangling
from flight_delay import weather

# NOAA API key
token = 'zJIZpRSHUipDYEvQDaIbfDoSkVALfNUk'
date_start = '2018-01'
date_end = '2018-12'
p1 = '~/Documents/flight_delay/data/flight_data'
# p1, p2 = wrangling.selen(date_start, date_end)
# wrangling.combine_csv(p1, p2)
df, airport, icao, n, d = wrangling.import_csv(p1, date_start, date_end)
df, airport, flag, date3 = wrangling.init_check(df, airport, date_start, date_end, d)
df = wrangling.recover(df, airport, flag, date3, n, d)
df = weather.get_data(df, airport, icao, date_start, date_end)
