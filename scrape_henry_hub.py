import numpy as np
from bs4 import BeautifulSoup as BS
import requests
import pandas as pd
import sys


def daily(html):
    soup = BS(html)
    rows = soup.select('table[summary] tr')

    # skip header
    df = pd.DataFrame(rows[1:])

    # remove empty columns
    df.replace(r'^\s*$', np.nan, regex=True, inplace=True)
    df.dropna(axis='columns', how='all', inplace=True)

    # reshape
    df = df.applymap(lambda x: x.get_text())
    df.columns = ['week'] + list(range(5))
    df['week'] = df['week'].apply(lambda week: pd.to_datetime(week[:week.find('to')].replace('-', '')))
    df.set_index('week', inplace=True)
    df = df.stack().reset_index()
    df.columns = ['start_date', 'offset_date', 'price']
    df.index = df['start_date'] + pd.to_timedelta(df['offset_date'], unit='d')

    # clean
    df.drop(columns=['start_date', 'offset_date'], inplace=True)
    df.replace(r'^\s*$', np.nan, regex=True, inplace=True)
    df.dropna(axis='rows', how='all', inplace=True)

    return df


def weekly(html):

    soup = BS(html)
    rows = soup.select('table[SUMMARY] tr')
    df = pd.DataFrame(rows[2:], columns=['tag'])
    df['year_month'] = df['tag'].apply(lambda x: x.select_one('.B6'))
    df.dropna(inplace=True)

    data = {}

    def unpack_data(tag):
        strings = (i.strip() for i in tag.strings if i.strip())
        year = next(strings).split('-')[0]
        while True:
            try:
                date = next(strings)
                data[f'{year} {date}'] = [next(strings)]
            except StopIteration:
                break

    df['tag'].apply(lambda x: unpack_data(x))
    df = pd.DataFrame(data).transpose()
    df.index = pd.to_datetime(df.index)

    return df


price_extractor = {
    'D': daily,
    'W': weekly
}

file_names = {
    'D': 'daily_prices.csv',
    'W': 'weekly_prices.csv',
}


def load_prices(granularity='D'):
    """
    Loads prices from http://www.eia.gov/dnav/ng/hist/rngwhhdm.htm and stores them in a csv file.
    :param granularity: 'D' for days, 'W' for weeks, 'M' for months, 'A' for years, default is 'D'
    """

    url = f'https://www.eia.gov/dnav/ng/hist/rngwhhd{granularity}.htm'

    html = requests.get(url).text
    df = price_extractor[granularity](html)

    df.to_csv(file_names[granularity], header=False)


if __name__ == '__main__':
    load_prices(*sys.argv[1:])
