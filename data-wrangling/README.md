## Data Wrangling Requirements
1-Windows Subsystem for Linux (WSL) \
2-Python3 \
3-Pandas \
4-Numpy \
5-Datetime \
6-Multiprocessing (for linux or jupyter notebook) \
7-Selenium \
8-Progressbar \
9-Urllib3 \
10-csvkit \
11-Google Chrome (83.0.4103.61) and Chromedriver (ChromeDriver 83.0.4103.14)

## Usage
```
python flight_delay.py YYYY-MM YYYY-MM output_filename
```
Example
```
python flight_delay.py 2012-01 2012-12 2012_final
```
The filename should not have .csv file extension. Any flight and weather data between given date range will be ready under ~/data/flight_data folder.
