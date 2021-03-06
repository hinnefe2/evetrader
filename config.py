import os.path
import yaml

import pandas as pd

# directory to hold 'donefiles' for indicating
# when a database task is complete
donefiles_dir = "donefiles"

# directory to hold API market history data
history_dir = "market_history"

# directory to hold API market order data
orders_dir = "market_orders"

# directory to hold API transaction data
txns_dir = "market_txns"

# directory for static resources, like active type_ids
# and type_id - type_name lookup file
static_dir = "static"

# read in the list of item type_ids for items that
# are actively traded
#active_type_ids = pd.read_csv(os.path.join(static_dir, 'active_type_ids.csv')).type_id.values[:10]
active_type_ids = pd.read_csv('static/1bilweekly.csv').type_id.values

# location_id of station in which we're trading
station_id = 60004588   # Rens

# read the API keys from the credentials file
with open('credentials.yaml', 'r') as infile:
    creds = yaml.load(infile.read())
