import os
import sys
import wrangling
import weather


def argcheck(parser):
    assert len(parser) == 4, "Number of arguments passed is missing. Usage: python flight_delay YYYY-MM YYYY-MM output_filename"
    date_start = parser[1]
    date_end = parser[2]
    dt_check = date_start.split('-')
    assert len(
        dt_check[0]) == 4, "Year format is wrong. Usage: python flight_delay YYYY-MM YYYY-MM output_filename"
    assert len(
        dt_check[1]) == 2, "Month format is wrong. Usage: python flight_delay YYYY-MM YYYY-MM output_filename"
    dt_check = date_end.split('-')
    assert len(
        dt_check[0]) == 4, "Year format is wrong. Usage: python flight_delay YYYY-MM YYYY-MM output_filename"
    assert len(
        dt_check[1]) == 2, "Month format is wrong. Usage: python flight_delay YYYY-MM YYYY-MM output_filename"
    filename = parser[3]
    return date_start, date_end, filename


if __name__ == '__main__':
    parser = sys.argv
    date_start, date_end, filename = argcheck(parser)
    p1, p2 = wrangling.acquire(date_start, date_end)
    wrangling.combine_csv(p1, p2)
    df, airport, icao, n, d = wrangling.import_csv(p1, date_start, date_end)
    df, airport, flag, date3 = wrangling.init_check(
        df, airport, date_start, date_end, d)
    df = wrangling.recover(df, airport, flag, date3, n, d)
    df = weather.data(df, airport, icao, date_start, date_end)
    df.to_csv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(
        __file__))), 'data/flight_data', filename + '.csv'), header=True, index=False)
    print('Your data is ready under ~/data/flight_data folder.')
