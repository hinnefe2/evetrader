import datetime as dt

import pandas as pd
import sqlalchemy as sqla

import config

db_conn = sqla.create_engine('sqlite:///evetrader.sqlite3').connect()


def build_all_features():

    # build all the features as individual Series objects, then join them all on type_id
    return pd.concat([subclass().get_values() for subclass in FeatureBase.__subclasses__()], axis=1)


class FeatureBase:
    """Base class for all feature classes"""

    try:
        orders = pd.read_sql_table('orders', db_conn)
        history = pd.read_sql_table('history', db_conn)
        
        # only consider sell orders in the same station
        sell_mask = (orders.buy == 0) & (orders.location_id == config.station_id)

        # only consider buy orders either in the same station or that are region-wide
        buy_mask = (orders.buy == 1) & ((orders.location_id == config.station_id) | 
                                        (orders.range == 'region'))

        sells = orders.loc[sell_mask]
        buys = orders.loc[buy_mask]

    # if the database tables don't exist yet
    except ValueError:
        pass

    def __init__(self):
        self.name = self.__class__.__name__.lower()

    def _build(self):
        """Return this feature as a pandas series indexed by type_id"""
        return 

    def get_values(self):
        """Return this feature as a Series indexed by type_id and named after the feature"""
        return self._build().rename(self.name)


class MarginISK(FeatureBase):

    def _build(self):
        result = (self.sells.groupby('type_id').price.min() 
                  - self.buys.groupby('type_id').price.max())
        
        return result
    
    
class MarginPercent(FeatureBase):

    def _build(self):
        result = ((self.sells.groupby('type_id').price.min() 
                   - self.buys.groupby('type_id').price.max()) 
                   / self.buys.groupby('type_id').price.max())
        
        return result
    
    
class VolumeISKAvg1Week(FeatureBase):
    
    def _build(self):
        result = (self.history
                      .loc[self.history.date >= dt.datetime.today() - dt.timedelta(days=7)]
                      .groupby('type_id')
                      .mean()
                      .volume_isk)
        return result
    

class VolumeUnitsAvg1Week(FeatureBase):
    
    def _build(self):
        result = (self.history
                      .loc[self.history.date >= dt.datetime.today() - dt.timedelta(days=7)]
                      .groupby('type_id')
                      .mean()
                      .volume)
        return result

 
class NumCompetingSellOrders(FeatureBase):
    
    def _build(self):
        result = self.sells.groupby('type_id').id.count()
        return result
    
    
class NumCompetingBuyOrders(FeatureBase):
    
    def _build(self):
        result = self.buys.groupby('type_id').id.count()
        return result
    
    
class MinutesSinceSellUpdate(FeatureBase):
    
    def _build(self):
        result = (self.sells
                      .groupby('type_id')
                      .issued
                      .max()
                      .apply(lambda row: (dt.datetime.utcnow() - row).total_seconds() / 60))
        
        return result
    

class MinutesSinceBuyUpdate(FeatureBase):
    
    def _build(self):
        result = (self.buys
                      .groupby('type_id')
                      .issued
                      .max()
                      .apply(lambda row: (dt.datetime.utcnow() - row).total_seconds() / 60))
        
        return result
