"""Script to scrape the market history of items from the EVE CREST API"""

import requests as rq
import logging
import threading
import json
import time

history_url = 'https://crest-tq.eveonline.com/market/{region_id}/types/{type_id}/history/'
save_path = 'hist_data/{region_id}/{type_id}.json'

rens_region_id = '10000030'

# set up the logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# create a file handler
handler = logging.FileHandler('scraper.log')
handler.setLevel(logging.INFO)

# create a logging format
formatter = logging.Formatter('%(asctime)s - %(threadName)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(handler)


def get_item_history(*type_ids):
    """Get the market history for all items in 'type_ids' """

    region_id = rens_region_id

    for type_id in type_ids:
        try:
            int(type_id)
            time.sleep(1)
            req = rq.get(history_url.format(region_id=region_id, type_id=type_id))
        except Exception as e:
            logger.error('Failed to get info for typeID - {} - with error {}'.format(type_id, e))
            return None

        with open(save_path.format(region_id=region_id, type_id=type_id), 'w') as outfile:
            json.dump(req.json(), outfile)
            logger.info('Got info for typeID {type_id}'.format(type_id=type_id))


if __name__ == '__main__':

    n_threads = 150

    with open('type_ids.csv', 'r') as infile:
        type_ids_str = infile.read().splitlines()

    type_ids = [int(t) for t in type_ids_str]

    threads = [threading.Thread(name = 'thread_{}'.format(i), 
                                target = get_item_history, 
                                args = type_ids[i::n_threads])
               for i in range(n_threads)]

    for t in threads:
        t.start()
