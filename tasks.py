import os.path

import luigi
import pandas as pd

import config
from api import get_history, get_orders
from api import process_history_json

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
        return luigi.LocalTarget(os.path.join(config.orders_dir,filename))

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
            yield GetMarketHistory(type_id)


class LoadAPIDataToDatabase(luigi.Task):

    def requires(self):
        return [GetAllMarketHistory, GetAllMarketOrders]

    def output(self):
        filename = "{}.done".format(self.__class__.__name__)
        return luigi.LocalTarget(os.path.join(config.donefiles_dir, filename))

    def run(self):

        history_dfs = []
        for filename in os.listdir(config.history_dir):
            history_dfs.append(process_history_json(filename))
        all_history = pd.concat(history_dfs)
