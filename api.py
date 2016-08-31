import logging
import requests as rq
import json

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
    pass


def process_history_json(filename):
    """Read a scraped market history json file and return a Dataframe
    with the raw market data"""

    with open(os.path.join(HISTORY_JSON_DIR + MARKET_REGION, filename)) as data_file:
        data = json.load(data_file)

    raw_df = pd.DataFrame.from_dict(data['items'], orient='columns')

    # convert date column to datetime objects
    raw_df['date'] = pd.to_datetime(raw_df['date'])

    # drop some columns
    del raw_df['orderCount_str']
    del raw_df['volume_str']

    return raw_df
