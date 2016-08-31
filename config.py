import pandas as pd

# read in the list of item type_ids for items that
# are actively traded
active_type_ids = pd.read_csv('active_type_ids.csv').type_id.values[:100]

# directory to hold 'donefiles' for indicating
# when a database task is complete
donefiles_dir = "donefiles"

# directory to hold API market history data
history_dir = "market_history"

# directory to hold API market order data
orders_dir = "market_orders"
