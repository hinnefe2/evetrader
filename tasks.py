import os.path
import sqlalchemy as sqla

import luigi
import pandas as pd

import config
from api import get_history, get_orders, get_transactions, process_history_json, process_orders_json
from features import build_all_features


class GetMarketHistory(luigi.Task):
    """Query the market history API endpoint for the given type_id
    and write the result to a json file"""

    type_id = luigi.IntParameter()

    def requires(self):
        return []

    def output(self):
        filename = os.path.join(config.history_dir, "history_{}.json".format(self.type_id))
        return luigi.LocalTarget(filename)

    def run(self):
        result = get_history(self.type_id)

        outfile = self.output().open('w')
        outfile.write(result)
        outfile.close()


class GetAllMarketHistory(luigi.WrapperTask):
    """Query the market history API endpoint for every type_id
    specified in the config file and write the results to individual files"""

    def requires(self):

        type_id_list = config.active_type_ids

        for type_id in type_id_list:
            yield GetMarketHistory(type_id)


class GetMarketOrders(luigi.Task):
    """Query the market orders API endpoint for the given type_id
    and write the result to a json file"""

    type_id = luigi.IntParameter()

    def requires(self):
        return []

    def output(self):
        filename = "orders_{}.json".format(self.type_id)
        return luigi.LocalTarget(os.path.join(config.orders_dir, filename))

    def run(self):
        result = get_orders(self.type_id)

        outfile = self.output().open('w')
        outfile.write(result)
        outfile.close()


class GetAllMarketOrders(luigi.WrapperTask):
    """Query the market orders API endpoint for every type_id
    specified in the config file and write the results to individual files"""

    def requires(self):

        type_id_list = config.active_type_ids

        for type_id in type_id_list:
            yield GetMarketOrders(type_id)


class GetTransactions(luigi.Task):
    """Query the character transaction API endpoint and retrieve
    the character transactions"""

    def requires(self):
        return []

    def output(self):
        filename = "transactions.csv"
        return luigi.LocalTarget(os.path.join(config.txns_dir, filename))

    def run(self):
        result = get_transactions(config.creds['key_id'], config.creds['access_code'])

        outfile = self.output().open('w')
        outfile.write(result)
        outfile.close()


class LoadAPIDataToDatabase(luigi.Task):

    def requires(self):
        return [GetAllMarketHistory(), GetAllMarketOrders(), GetTransactions()]

    def output(self):
        filename = "{}.done".format(self.__class__.__name__)
        return luigi.LocalTarget(os.path.join(config.donefiles_dir, filename))

    def run(self):

        db_conn = sqla.create_engine('sqlite:///evetrader.sqlite3').connect()

        # load all the downloaded history data into dataframes
        history_dfs = []
        for filename in os.listdir(config.history_dir):
            history_dfs.append(process_history_json(filename))
        all_history = pd.concat(history_dfs)

        # load all the downloaded orders data into dataframes
        orders_dfs = []
        for filename in os.listdir(config.orders_dir):
            orders_dfs.append(process_orders_json(filename))
        all_orders = pd.concat(orders_dfs)

        # load all the downloaded transaction data into a dataframe
        # first find the last transaction already stored in the db
        query = "SELECT MAX(transactionid) FROM transactions"
        try:
            max_existing_txn_id = pd.read_sql(query, db_conn)['MAX(transactionid)'].values[0]
        except:
            max_existing_txn_id = 0
        # next read the downloaded txns and pull out the new ones
        filename = os.path.join(config.txns_dir, 'transactions.csv')
        txns_df = pd.read_csv(filename, parse_dates=['transactiondatetime'])
        txns_df = txns_df.loc[txns_df.transactionid > max_existing_txn_id]

        # load all the data into the database
        all_history.to_sql('history', db_conn, if_exists='replace')
        all_orders.to_sql('orders', db_conn, if_exists='append')
        txns_df.to_sql('transactions', db_conn, if_exists='append')

        # touch the donefile
        self.output().open('w').close()


class BuildFeaturesTable(luigi.Task):

    def requires(self):
        return LoadAPIDataToDatabase()

    def output(self):
        filename = "{}.done".format(self.__class__.__name__)
        return luigi.LocalTarget(os.path.join(config.donefiles_dir, filename))

    def run(self):

        db_conn = sqlite3.connect('evetrader.sqlite3')

        features_df = build_all_features()
        features_df.to_sql('features', db_conn, if_exists='replace')

        # touch the donefile
        self.output().open('w').close()
