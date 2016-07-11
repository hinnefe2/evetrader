import json
import os.path
import logging
import sys
import threading
import datetime as dt
import requests as rq
import bs4
import numpy as np
import sqlalchemy
import pandas as pd
import luigi

logger = logging.getLogger(__name__)
stdout_handler = logging.StreamHandler(sys.stdout)
logger.addHandler(stdout_handler)
logger.setLevel(logging.DEBUG)

SQLITE_DB = 'evetrader.db'
ENGINE = sqlalchemy.create_engine('sqlite:///' + os.path.join('.', SQLITE_DB))

ACTIVE_TYPE_IDS = pd.read_csv('active_type_ids.csv').type_id.values
INV_TYPES = pd.read_csv('invTypes.csv')

HISTORY_JSON_DIR = './market_history/'
DONEFILES_DIR = './donefiles/'

HISTORY_TABLE = 'history'
FEATURES_TABLE = 'features'
ORDERS_TABLE = 'orders'

MARKET_REGION = '10000030'

def get_item_history(type_ids, region_id=MARKET_REGION):
    """Get the market history for all items in 'type_ids' """

    crest_history_url = 'https://crest-tq.eveonline.com/market/{region_id}/history/?type=https://public-crest.eveonline.com/inventory/types/{type_id}/'

    for i, type_id in enumerate(type_ids):

        try:
            assert type(int(type_id)) == int, 'could not cast type_id to int'
            req = rq.get(crest_history_url.format(region_id=region_id, type_id=type_id))

        except Exception as e:
            logger.error('Failed to get info for typeID - {} - with error {}'.format(type_id, e))
            return None

        save_path = HISTORY_JSON_DIR +'{region_id}/{type_id}.json'
        with open(save_path.format(region_id=region_id, type_id=type_id), 'w') as outfile:
            json.dump(req.json(), outfile)


def process_scraped_json(filename):
    """Read a scraped market history json file and return a Dataframe
    indexed by date with the raw market data as well as some derived statistics."""

    with open(os.path.join(HISTORY_JSON_DIR + MARKET_REGION, filename)) as data_file:
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


def get_market_orders(region_id='10000030', type_id='34'):
    """Get the current market orders for an item in a region.
    API has 6 min cache time."""

    crest_order_url = 'https://crest-tq.eveonline.com/market/{}/orders/{}/?type=https://public-crest.eveonline.com/inventory/types/{}/'

    dfs = []
    for order_type in ['buy', 'sell']:
        resp = rq.get(crest_order_url.format(region_id, order_type, type_id))
        resp_json = resp.json()
        dfs.append(pd.DataFrame(resp_json['items']))

    orders = pd.concat(dfs)

    def extract_station_id(loc_dict):
        """Pull the station ID out of the CREST callback url"""
        return int(loc_dict['id'])
    orders['station_id'] = orders.location.apply(extract_station_id)

    def extract_type_id(type_dict):
        """Pull the type ID out of the CREST callback url"""
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


def calculate_margin(type_id, db_conn):
    """Calculate the margin for an given item using orders stored in the database"""

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


class DownloadHistoryJSONFiles(luigi.Task):
    """Download the market history for all actively traded items"""

    def output(self):
        return luigi.LocalTarget(os.path.join(DONEFILES_DIR, 'DownloadHistoryJSONFiles-Done.luigi'))

    def requires(self):
        return []

    def run(self):

        n_threads = 150
    
        type_ids = pd.read_csv('active_type_ids.csv').type_id.values

        threads = [threading.Thread(name = 'thread_{}'.format(i), 
                                    target = get_item_history, 
                                    args = [type_ids[i::n_threads]])
                   for i in range(n_threads)]
    
        # start all the threads
        for t in threads:
            t.start()

        # wait for all threads to complete
        for t in threads:
            t.join()

        # touch the donefile to let luigi know this task is done
        with self.output().open('w') as output_file:
            pass

class ProcessHistoryJSONFiles(luigi.Task):

    def output(self):
        return luigi.LocalTarget('processed_history_data.csv')

    def requires(self):
        return DownloadHistoryJSONFiles()

        #return [the download and filter json files task]

    def run(self):
        """Read all the scraped JSON files and put them into a single csv file"""

        df_list = []
        for i, json_file in enumerate(os.listdir(HISTORY_JSON_DIR + MARKET_REGION)):
            df = process_scraped_json(json_file)

            if i % 100 == 0:
                logger.info('Done with {}/{}'.format(i, len(os.listdir(HISTORY_JSON_DIR + MARKET_REGION))))

            df_list.append(df)

        df_final = pd.concat(df_list)

        with self.output().open('w') as out_file:
            df_final.to_csv(out_file)


class PushProcessedHistoryToDB(luigi.Task):

    def output(self):
        # TODO: fix / find a better way for this
        return luigi.LocalTarget(os.path.join(DONEFILES_DIR, 'PushProcessedHistoryToDB-Done.luigi'))

    def requires(self):
        return ProcessHistoryJSONFiles()

    def run(self):

        db_conn = ENGINE.connect()

        with self.input().open('r') as input_file:
            df = pd.read_csv(input_file)
            df.to_sql('history', db_conn, if_exists='replace', index=True, chunksize=1000)

        # touch the output file to track that the task is done
        # TODO: figure out SQL DB outputs in luigi
        with self.output().open('w') as output_file:
            pass


class DownloadOrdersForTopNTypeIDs(luigi.Task):

    #n = luigi.IntParameter()
    n = 100

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
        # TODO: change this back to 14 days
        two_wks_ago = today - dt.timedelta(days=30)
        sql_query = 'SELECT * FROM history WHERE date BETWEEN "{}" AND "{}"'.format(
            two_wks_ago.strftime(SQLITE_DATE_FMT),
            today.strftime(SQLITE_DATE_FMT))

        # connect to sqlite, pull in the history data
        db_conn = ENGINE.connect()
        df = pd.read_sql_query(sql_query, db_conn, parse_dates=['date'])

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


class PushMarketOrdersToDB(luigi.Task):

    def output(self):
        # TODO: fix / find a better way for this
        return luigi.LocalTarget(os.path.join(DONEFILES_DIR, 'PushMarketOrdersToDB-Done.luigi'))

    def requires(self):
        return DownloadOrdersForTopNTypeIDs()

    def run(self):

        # TODO: standardize to use sqlalchemy everywhere
        # use one engine for whole pipeline
        db_conn = ENGINE.connect()

        with self.input().open('r') as input_file:
            df = pd.read_csv(input_file)
            df.to_sql('orders', db_conn, if_exists='replace', index=False)

        # touch the output file to track that the task is done
        # TODO: figure out SQL DB outputs in luigi
        with self.output().open('w') as output_file:
            pass

class AddRecentTransactionsToDB(luigi.Task):
    """Download the character's transactions from the XML API"""

    def output(self):
        return luigi.LocalTarget(os.path.join(DONEFILES_DIR,'AddRecentTransactionsToDB-Done.luigi'))

    def requires(self):
        return []

    def run(self):

        db_conn = ENGINE.connect()

        api_url = 'https://api.eveonline.com/char/WalletTransactions.xml.aspx?keyID={}&vCode={}&rowCount={}'
        key_id = '442571'
        access_code = '7qdTnpfrBfL3Gw2elwKaT9SsGkn6O5gwV3QUM77S3pHPanRBzzDyql5pCUU7V0bS'

        # query the API and parse the returned xml with beautifulsoup
        req = rq.get(api_url.format(key_id, access_code, 2560))
        soup = bs4.BeautifulSoup(req.text, 'lxml')

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

        # get the existing transactions ids
        existing_ids = pd.read_sql_table('transactions', db_conn).transactionid.values

        # pull out the new transactions and put them in the database
        new_transactions = df.loc[~df.transactionid.isin(existing_ids)]
        new_transactions.to_sql('transactions', db_conn, if_exists='append', index=False)

    
class CreateFeaturesTable(luigi.Task):

    #n = luigi.IntParameter()
    n = 100

    def output(self):
        return luigi.LocalTarget(os.path.join(DONEFILES_DIR,'CreateFeaturesTable-Done.luigi'))

    def requires(self):
        return [PushProcessedHistoryToDB(), PushMarketOrdersToDB()]

    def run(self):

        SQLITE_DATE_FMT = '%Y-%m-%d'
        db_conn = ENGINE.connect()

        today = dt.date.today()
        two_wks_ago = today - dt.timedelta(days=14)

        # get the type_ids of the the top n items by 2-week average isk volume
        sql_query = 'SELECT * FROM history WHERE date BETWEEN "{}" AND "{}"'.format(
            two_wks_ago.strftime(SQLITE_DATE_FMT),
            today.strftime(SQLITE_DATE_FMT))

        df = pd.read_sql_query(sql_query, db_conn, parse_dates=['date'])

        top_n_type_ids = (df[['type_id', 'volume_isk']]
                            .groupby('type_id')
                            .mean()
                            .sort_values('volume_isk', ascending=False)
                            .head(self.n)
                            .index.values)
        print(top_n_type_ids)

        # generate each feature as a pd.Series or pd.DataFrame, indexed by type_id
        features_list = []

        #############################
        # FEATURES: n-day average of history statistics (prices, volumes)
        #############################

        type_name_df = INV_TYPES[['typeID', 'typeName']].set_index('typeID').ix[top_n_type_ids]

        def get_n_day_avgs(n_days):
            """Get averages of volume, volume_isk, lowVol, highVol, limitVol over past n days"""
            
            period_end = dt.date.today()
            period_start = today - dt.timedelta(days=n_days)
    
            sql_query = 'SELECT * FROM history WHERE date BETWEEN "{}" AND "{}"'.format(
                period_start.strftime(SQLITE_DATE_FMT),
                period_end.strftime(SQLITE_DATE_FMT))
    
            df = pd.read_sql_query(sql_query, db_conn, parse_dates=['date'])

            volume_avgs = (df[['type_id', 'volume_isk', 'volume', 'lowVol', 'highVol', 'avgPrice']]
                           .groupby('type_id')
                           .mean()
                           .sort_values('volume_isk', ascending=False))

            volume_avgs['limitVol'] = volume_avgs.loc[:, ['lowVol', 'highVol']].min(axis=1)

            volume_avgs.columns = ['{}_{}day'.format(column_name, n_days) for column_name in volume_avgs.columns]

            return volume_avgs

        volumes_1day = get_n_day_avgs(1)
        volumes_7day = get_n_day_avgs(7)
        volumes_14day = get_n_day_avgs(14)
        volumes_30day = get_n_day_avgs(30)

        features_list.extend([volumes_1day, volumes_7day, volumes_14day, volumes_30day])

        #############################
        # FEATURES: current margins on orders
        #############################

        # calculate the various margins for each type_id, store in dict: {type_id: (max_buy, min_sell ...)}
        margins = {}
        for type_id in top_n_type_ids:
            cols = ['max_buy', 'min_sell', 'margin', 'margin_actual']
            margins[type_id] = dict(zip(cols, calculate_margin(type_id, db_conn)))

        # convert the dict of dicts to a df
        margins_df = pd.DataFrame(margins).transpose()

        features_list.append(margins_df)

        #############################
        # FEATURES: statistics about orders
        #############################

        sql_query = 'SELECT type_id, issued  FROM orders'.format(type_id)
        orders_df = pd.read_sql_query(sql_query, db_conn)

        # number of active orders
        n_active_orders = orders_df.groupby('type_id').count()
        n_active_orders.columns = ['n_active_orders']

        # TODO implement this
        # time since last order update
        #t_since_last_order = 

        features_list.append(n_active_orders)
        
        #############################
        # join all features
        #############################

        # join all the feature sub-dataframes
        feature_df = type_name_df.join(features_list)

        # add some derived feature columns
        feature_df['margin_isk_7day'] = feature_df.margin_actual * feature_df.limitVol_7day * feature_df.avgPrice_7day

        feature_df.to_sql('features', db_conn, if_exists='replace', index=False)

        with self.output().open('w') as out_file:
            pass


if __name__ == '__main__':
    luigi.run()
