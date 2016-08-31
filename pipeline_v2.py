import json
import os.path
import logging
import sys
import datetime as dt
import requests as rq
import bs4
import numpy as np
import sqlalchemy
import pandas as pd
import luigi

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s',
                    stream=sys.stdout)
logger = logging.getLogger(__name__)

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

def get_history_api_result(type_id):
    """Get the market history for item with given type_id"""

    crest_history_url = ('https://crest-tq.eveonline.com/market/{region_id}/history/?type='
                         'https://public-crest.eveonline.com/inventory/types/{type_id}/')

    try:
        req = rq.get(crest_history_url.format(region_id=MARKET_REGION, type_id=type_id))

    except Exception as e:
        logger.error('Failed to get info for typeID - {} - with error {}'.format(type_id, e))
        return None

    return json.dumps(req.json())


class GetMarketHistory(luigi.Task):

    type_id = luigi.Parameter()

    def requires(self):
        return []

    def output(self):
        filename = os.path.join(HISTORY_JSON_DIR, "history_{}.json".format(self.type_id))
        return luigi.LocalTarget(filename)

    def run(self):
        result = get_history_api_result(self.type_id)
        
        outfile = self.output().open('w')
        outfile.write(result)
        outfile.close()


class GetAllMarketHistory(luigi.WrapperTask):

    def requires(self):

        type_id_list = ACTIVE_TYPE_IDS[:100]

        for type_id in type_id_list:
            yield GetMarketHistory(type_id)
