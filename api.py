import logging
import os
import os.path
import time
import re
import requests as rq
import json

import bs4
import pandas as pd

import config

MARKET_REGION = '10000030'

logger = logging.getLogger(__name__)

def get_history(type_id, region_id=MARKET_REGION):
    """Get the market history for item with given type_id"""

    crest_history_url = ('https://crest-tq.eveonline.com/market/{region_id}/history/?type='
                         'https://public-crest.eveonline.com/inventory/types/{type_id}/')

    try:
        req = rq.get(crest_history_url.format(region_id=MARKET_REGION, type_id=type_id))

    except Exception as e:
        logger.error('Failed to get info for typeID - {} - with error {}'.format(type_id, e))
        return None

    return json.dumps(req.json())


def get_orders(type_id, region_id=MARKET_REGION):
    """Get the current market orders for an item in a region.
    API has 6 min cache time."""

    crest_order_url = ('https://crest-tq.eveonline.com/market/{region_id}/orders/{order_type}/?type='
                       'https://public-crest.eveonline.com/inventory/types/{type_id}/')

    # api only returns buys or sells, so need two calls to get both
    responses = []
    for order_type in ['buy', 'sell']:
        resp = rq.get(crest_order_url.format(region_id=region_id, 
                                             order_type=order_type, 
                                             type_id=type_id))
        resp_json = resp.json()
        responses.extend(resp_json['items'])

    return json.dumps(responses)


def get_transactions(key_id, access_code):
    """Get the transaction history for the character with the given API key"""

    xml_url = 'https://api.eveonline.com/char/WalletTransactions.xml.aspx?keyID={}&vCode={}&rowCount={}'

    key_id = '442571'
    access_code = '7qdTnpfrBfL3Gw2elwKaT9SsGkn6O5gwV3QUM77S3pHPanRBzzDyql5pCUU7V0bS'

    # query the API and parse the returned xml with beautifulsoup
    response = rq.get(xml_url.format(key_id, access_code, 2560))
    soup = bs4.BeautifulSoup(response.text, 'lxml')

    # put the transactions in a dataframe
    rows = soup.findAll('row')
    row_dicts = [row.attrs for row in soup.findAll('row')]
    df = pd.DataFrame(row_dicts)

    # convert columns to the appropriate datatypes
    numeric_cols = ['clientid', 'clienttypeid', 'journaltransactionid',
        'price', 'quantity', 'stationid', 'transactionid','typeid']

    datetime_cols = ['transactiondatetime']

    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric)
    df[datetime_cols] = df[datetime_cols].apply(pd.to_datetime)

    return df.to_csv(index=False)


def process_orders_json(filename):
    """Read a scraped market order json file and return a Dataframe
    with the raw market data"""

    df = pd.read_json(os.path.join(config.orders_dir, filename))
    
    try:
        # pull the type_id and name out of the 'type' dictionary
        df['type_id'] = df.type.apply(lambda row: int(row['id']))
        df['type_name'] = df.type.apply(lambda row: row['name'])
        
        # pull the location_id and name out of the 'location' dictionary
        df['location_id'] = df.location.apply(lambda row: int(row['id']))
        df['location_name'] = df.location.apply(lambda row: row['name'])
        
        # add information about when the file was created
        modified = time.gmtime(os.path.getmtime(os.path.join(config.orders_dir, filename)))
        df['recorded'] = pd.to_datetime(time.strftime('%Y-%m-%d %H:%M', modified))

        # convert the 'issued' column to a datetime
        df['issued'] = pd.to_datetime(df['issued'])

        # drop unneccesary columns
        str_cols = [col for col in df.columns if '_str' in col]
        df = df.drop(str_cols, axis=1)
        df = df.drop(['href', 'location', 'type'], axis=1)
    
        return df

    except:

        return pd.DataFrame()
    

def process_history_json(filename):
    """Read a scraped market history json file and return a Dataframe
    with the raw market data"""

    # pull the type_id out of the filename since it's not saved in the json
    type_id = re.match("history_([0-9]*)\.json", filename).group(1)

    with open(os.path.join(config.history_dir, filename)) as data_file:
        data = json.load(data_file)

    raw_df = pd.DataFrame.from_dict(data['items'], orient='columns')

    # convert date column to datetime objects
    raw_df['date'] = pd.to_datetime(raw_df['date'])

    # add a type_id column
    raw_df['type_id'] = int(type_id)

    # add information about when the file was created
    modified = time.gmtime(os.path.getmtime(os.path.join(config.history_dir, filename)))
    raw_df['recorded'] = pd.to_datetime(time.strftime('%Y-%m-%d %H:%M', modified))

    # add a column with the volume in ISK
    raw_df['volume_isk'] = raw_df.avgPrice * raw_df.volume

    # drop some columns
    del raw_df['orderCount_str']
    del raw_df['volume_str']

    return raw_df
