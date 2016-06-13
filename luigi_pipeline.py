import json
import os.path
import logging
import sys
import sqlite3
import datetime as dt
import requests as rq
import numpy as np
import sqlalchemy
import pandas as pd
import luigi

logger = logging.getLogger(__name__)
stdout_handler = logging.StreamHandler(sys.stdout)
logger.addHandler(stdout_handler)
logger.setLevel(logging.DEBUG)

data_dir = '/home/ec2-user/scraped/'
SQLITE_DB = 'evetrader.db'

INV_TYPES = pd.read_csv('invTypes.csv')
ENGINE = sqlalchemy.create_engine('sqlite:////home/ec2-user/' + SQLITE_DB)

def estimate_highlow_volumes(row):
    """Estimate buy and sell volumes by assuming all items were sold at
    either the max or min price"""

    # | lowPrice , highPrice  |   | lowVol  |   | avgPrice * volume |
    #                           x             =
    # | 1        , 1          |   | highVol |   | volume |

    a_matrix = [[row.lowPrice, row.highPrice], [1, 1]]
    b_matrix = [row.avgPrice * row.volume, row.volume]

    try:
        lowVol, highVol = np.linalg.solve(a_matrix, b_matrix)
    except np.linalg.LinAlgError:
        return (np.nan, np.nan)

    return (highVol, lowVol)


def process_scraped_json(filename, data_dir='/home/ec2-user/scraped/'):
    with open(os.path.join(data_dir, filename)) as data_file:
        data = json.load(data_file)

    raw_df = pd.DataFrame.from_dict(data['items'], orient='columns')

    # convert date column to datetime objects, set as index
    raw_df['date'] = pd.to_datetime(raw_df['date'])
    raw_df = raw_df.set_index('date')

    # drop some columns
    del raw_df['orderCount_str']
    del raw_df['volume_str']

    # add some columns
    raw_df['volume_isk'] = raw_df.avgPrice * raw_df.volume
    raw_df['type_id'] = filename.split('.')[0]

    raw_df['highVol'], raw_df['lowVol'] = zip(*raw_df.apply(estimate_highlow_volumes, axis=1))

    return raw_df


ACTIVE_TYPE_IDS = pd.read_csv('active_type_ids.csv').type_id.values
HISTORY_JSON_DIR = '/home/ec2-user/scraped/'

class ProcessHistoryJSONFiles(luigi.Task):

    def output(self):
        return luigi.LocalTarget('processed_history_data.csv')

    def requires(self):
        return []

        #return [the download and filter json files task]

    def run(self):

        df_list = []
        for i, json_file in enumerate(os.listdir(HISTORY_JSON_DIR)):
            df = process_scraped_json(json_file)

            if i % 100 == 0:
                logger.info('Done with {}/{}'.format(i, len(os.listdir(HISTORY_JSON_DIR))))

            df_list.append(df)

        df_final = pd.concat(df_list)

        with self.output().open('w') as out_file:
            df_final.to_csv(out_file)


class PushProcessedHistoryToDB(luigi.Task):

    def output(self):
        # TODO: fix / find a better way for this
        return luigi.LocalTarget('PushProcessedHistoryToDB-Done.luigi')

    def requires(self):
        return ProcessHistoryJSONFiles()

    def run(self):

        con = sqlite3.connect(SQLITE_DB)

        with self.input().open('r') as input_file:
            df = pd.read_csv(input_file)
            df.to_sql('history', con, if_exists='replace', index=True)

        # touch the output file to track that the task is done
        # TODO: figure out SQL DB outputs in luigi
        with self.output().open('w') as output_file:
            pass


HISTORY_TABLE = 'history'

class DownloadOrdersForTopNTypeIDs(luigi.Task):

    n = luigi.IntParameter()

    def output(self):
        #return luigi.LocalTarget(self.__name__ + '-Done.luigi')
        return luigi.LocalTarget('TopNMarketOrders.csv')

    def requires(self):
        return PushProcessedHistoryToDB()

    def run(self):

        SQLITE_DATE_FMT = '%Y-%m-%d'
        type_names = INV_TYPES[['typeID', 'typeName']].set_index('typeID')

        # only pull history from the past two weeks
        # TODO: make this variable
        today = dt.date.today()
        two_wks_ago = today - dt.timedelta(days=14)
        sql_query = 'SELECT * FROM history WHERE date BETWEEN "{}" AND "{}"'.format(
            two_wks_ago.strftime(SQLITE_DATE_FMT),
            today.strftime(SQLITE_DATE_FMT))

        # connect to sqlite, pull in the history data
        engine = sqlalchemy.create_engine('sqlite:////home/ec2-user/' + SQLITE_DB)
        connection = engine.connect()
        df = pd.read_sql_query(sql_query, connection, parse_dates=['date'])

        volume_avgs = (df[['type_id', 'volume_isk', 'volume', 'lowVol', 'highVol']]
                       .groupby('type_id')
                       .mean()
                       .sort_values('volume_isk', ascending=False)
                       .join(type_names))

        type_ids = volume_avgs.head(self.n).index.values

        df_list = []
        for type_id in type_ids:
            df_list.append(get_market_orders(type_id=type_id))

        df = pd.concat(df_list)

        with self.output().open('w') as out_file:
            df.to_csv(out_file, index=False)


def get_market_orders(region_id='10000030', type_id='34'):
    """Get the current market orders for an item in a region.

    API has 6 min cache time."""

    crest_order_url = 'https://crest-tq.eveonline.com/market/{}/orders/{}/?type=https://public-crest.eveonline.com/types/{}/'

    dfs = []
    for order_type in ['buy', 'sell']:

        resp = rq.get(crest_order_url.format(region_id, order_type, type_id))
        resp_json = resp.json()
        dfs.append(pd.DataFrame(resp_json['items']))

    orders = pd.concat(dfs)

    def extract_station_id(loc_dict):
        """Pull the station ID out of the CREST callback url, add as a column"""
        return int(loc_dict['id'])
    orders['station_id'] = orders.location.apply(extract_station_id)

    def extract_type_id(type_dict):
        """Pull the type ID out of the CREST callback url, add as a column"""
        return int(type_dict['id'])
    orders['type_id'] = orders.type.apply(extract_type_id)

    # convert 'issued' column to datetime objects
    orders['issued'] = pd.to_datetime(orders.issued)

    # add type_name from INV_TYPES
    orders['type_name'] = INV_TYPES.loc[INV_TYPES.typeID == orders.type_id.values[0]].typeName.values[0]

    orders = orders[['buy', 'duration', 'id',
                     'issued', 'minVolume', 'price', 'range',
                     'type_id', 'volume', 'volumeEntered',
                     'station_id', 'type_name']]

    # TODO add logging of snapshot to database
    return orders.sort_values('price')


class PushMarketOrdersToDB(luigi.Task):

    def output(self):
        # TODO: fix / find a better way for this
        return luigi.LocalTarget('PushMarketOrdersToDB-Done.luigi')

    def requires(self):
        return DownloadOrdersForTopNTypeIDs()

    def run(self):

        # TODO: standardize to use sqlalchemy everywhere
        # use one engine for whole pipeline
        con = sqlite3.connect(SQLITE_DB)

        with self.input().open('r') as input_file:
            df = pd.read_csv(input_file)
            df.to_sql('orders', con, if_exists='replace', index=False)

        # touch the output file to track that the task is done
        # TODO: figure out SQL DB outputs in luigi
        with self.output().open('w') as output_file:
            pass

def calculate_margin(type_id, db_conn):

    sql_query = 'SELECT * FROM orders WHERE type_id == {}'.format(type_id)
    df = pd.read_sql_query(sql_query, db_conn)

    try:
        # take the most common station_id among orders to be the trade hub
        trade_hub_id = df.groupby('station_id').count().buy.idxmax()
    except ValueError:
        return (np.nan, np.nan, np.nan, np.nan)

    max_buy = df.loc[df.buy==1].price.max()
    min_sell = df.loc[(df.buy==0) & (df.station_id==trade_hub_id)].price.min()

    BROKER_FEE = 0.026
    SALES_TAX = 0.012

    margin = (min_sell - max_buy) / max_buy
    margin_actual = (min_sell * (1 - BROKER_FEE - SALES_TAX) - max_buy * (1 + BROKER_FEE)) / max_buy * (1 + BROKER_FEE)

    return (max_buy, min_sell, margin, margin_actual)

class CreateFeaturesTable(luigi.Task):

    n = luigi.IntParameter()

    def output(self):
        return luigi.LocalTarget('CreateFeaturesTable-Done.luigi')

    def requires(self):
        return PushMarketOrdersToDB()

    def run(self):

        SQLITE_DATE_FMT = '%Y-%m-%d'
        db_conn = ENGINE.connect()

        today = dt.date.today()
        two_wks_ago = today - dt.timedelta(days=14)

        sql_query = 'SELECT * FROM history WHERE date BETWEEN "{}" AND "{}"'.format(
            two_wks_ago.strftime(SQLITE_DATE_FMT),
            today.strftime(SQLITE_DATE_FMT))

        df = pd.read_sql_query(sql_query, db_conn, parse_dates=['date'])

        type_name_df = INV_TYPES[['typeID', 'typeName']].set_index('typeID')

       # get daily average of features from the past two weeks for top n type_ids (by ISK volume)
        volume_avgs = (df[['type_id', 'volume_isk', 'volume', 'lowVol', 'highVol']]
                       .groupby('type_id')
                       .mean()
                       .sort_values('volume_isk', ascending=False)
                       .join(type_name_df)
                       .dropna()
                       .head(self.n))

        margins = {}

        # make a dict of the features for each type_id: {type_id: {max_buy, min_sell ...}}
        for type_id in volume_avgs.index.values:
            cols = ['max_buy', 'min_sell', 'margin', 'margin_actual']
            margins[type_id] = dict(zip(cols, calculate_margin(type_id, db_conn)))

        # convert the dict of dicts to a df
        margin_df = pd.DataFrame(margins).transpose()

        # join the margin info with the volume info
        feature_df = margin_df.join(volume_avgs).dropna()

        # add some more feature columns
        feature_df['margin_isk'] = feature_df.margin_actual * feature_df.volume_isk
        feature_df['limitVol'] = feature_df.loc[:, ['lowVol', 'highVol']].min(axis=1)

        feature_df = feature_df.sort_values('margin_isk', ascending=False)

        feature_df.to_sql('features', db_conn, if_exists='replace', index=False)

        with self.output().open('w') as out_file:
            pass

if __name__ == '__main__':
    luigi.run()
