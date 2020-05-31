## Data Wrangling Requirements
1-[Windows Subsystem for Linux (WSL)](https://docs.microsoft.com/en-us/windows/wsl/install-win10) \
2-[Python3](https://www.python.org/downloads/) \
3-Pandas ```pip install pandas``` \
4-Numpy ```pip install numpy``` \
5-Datetime ```pip install datetime```\
6-Multiprocessing (for linux or jupyter notebook) ```pip install multiprocessing```\
7-Selenium ```pip install selenium```\
8-Progressbar ```pip install progressbar2```\
9-Urllib3 ```pip install urllib3```\
10-csvkit ```pip install --upgrade csvkit```\
11-Google Chrome (83.0.4103.61) and [Chromedriver (83.0.4103.14)](https://chromedriver.chromium.org/downloads)

## Usage
```
python flight_delay.py YYYY-MM YYYY-MM output_filename
```
Example
```
python flight_delay.py 2012-01 2012-12 2012_final
```
The filename should not have .csv file extension. Any flight and weather data between given date range will be ready under ~/data/flight_data folder.
