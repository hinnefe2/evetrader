import os.path
import sqlite3

import luigi
import pandas as pd

import config
from api import get_history, get_orders, process_history_json, process_orders_json
from features import build_all_features


class GetMarketHistory(luigi.Task):
    """Query the market history API endpoint for the given type_id
    and write the result to a json file"""

    type_id = luigi.Parameter()

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

    type_id = luigi.Parameter()

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


class LoadAPIDataToDatabase(luigi.Task):

    def requires(self):
        return [GetAllMarketHistory(), GetAllMarketOrders()]

    def output(self):
        filename = "{}.done".format(self.__class__.__name__)
        return luigi.LocalTarget(os.path.join(config.donefiles_dir, filename))

    def run(self):

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

        db_conn = sqlite3.connect('evetrader.sqlite3')

        all_history.to_sql('history', db_conn, if_exists='append')
        all_orders.to_sql('orders', db_conn, if_exists='append')

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
