# helper functions
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from nempy import markets, time_sequential
from nempy.historical_inputs import loaders, mms_db, \
    xml_cache, units, demand, interconnectors, constraints
import warnings
    
    
def perdelta(start_date, end_date, min_interval):
    """
    Function accessed by create_dispatch_list. Generates List of Datetimes between 2 dates.
    perdelta SourceCode retrieved and modified from: 
    https://stackoverflow.com/questions/10688006/generate-a-list-of-datetimes-between-an-interval
    """
    start = datetime.strptime(start_date,'%Y/%m/%d %H:%M:%S')    
    end = datetime.strptime(end_date,'%Y/%m/%d %H:%M:%S')  
    delta = timedelta(minutes=min_interval)
    
    curr = start
    while curr <= end:
        yield curr
        curr += delta

def create_dispatch_list(start_date, end_date, min_interval=5):
    """
    Generates a list of dispatch intervals between two dates.
    Inputs: start_date, end_date >> passed as string
    """
    result = []
    for n in perdelta(start_date, end_date, min_interval):
        result = result + [datetime.strftime(n,'%Y/%m/%d %H:%M:%S')]
    return result

def append_rp_unitinfo(unit_info, crm_provider):
    # new_storage = pd.DataFrame([{'unit': bess_param['unit'],
    #                            'region': bess_param['region'],
    #                            'dispatch_type': bess_param['dispatch_type'],
    #                             'loss_factor': bess_param['loss_factor']}])
    # unit_info = pd.concat([unit_info,new_storage],ignore_index=True)
    
    unit_info = pd.concat([unit_info,crm_provider.loc[:,['unit','region','dispatch_type','loss_factor']]]\
                                 ,ignore_index=True)
    return unit_info

def append_rp_volumebids(volume_bids, crm_provider):
    # new_storage = pd.DataFrame(data=[[bess_param['unit'],'energy'] + [0.0]*10], columns=volume_bids.columns)
    # new_storage['1'] = bess_param['relief_MW']
    # volume_bids = pd.concat([volume_bids,new_storage],ignore_index=True)
    
    fill_zeros = pd.DataFrame([[0.0]*11])
    fill_zeros.columns = fill_zeros.columns.map(str)
    fill_zeros.drop(['0','1'],axis=1,inplace=True)
    
    df = crm_provider.loc[:,['unit','service','relief_MW']].reset_index(drop=True)
    df = df.rename(columns={'relief_MW': '1'})
    df = pd.concat([df,fill_zeros],axis=1)
    
    volume_bids = pd.concat([volume_bids,df],ignore_index=True)   
    return volume_bids

def append_rp_pricebids(price_bids, crm_provider):
    # new_storage = pd.DataFrame(data=[[bess_param['unit'],'energy'] + [bess_param['default_offer']]*10], columns=price_bids.columns)
    # price_bids = pd.concat([price_bids,new_storage],ignore_index=True)

    fill_zeros = pd.DataFrame([[0.0]*11])
    fill_zeros.columns = fill_zeros.columns.map(str)
    fill_zeros.drop(['0','1'],axis=1,inplace=True)
    
    df = crm_provider.loc[:,['unit','service','default_offer']].reset_index(drop=True)
    df = df.rename(columns={'default_offer': '1'})
    df = pd.concat([df,fill_zeros],axis=1)
    
    price_bids = pd.concat([price_bids,df],ignore_index=True)   
    return price_bids

def append_rp_lhs(unit_generic_lhs, crm_provider):

    filt_constraint = unit_generic_lhs[unit_generic_lhs['set'] == crm_provider['set'].reset_index(drop=True)[0]]
    filt_mirror = filt_constraint[filt_constraint['unit'] == crm_provider['mirror_coeff'].reset_index(drop=True)[0]]
    
    # new_storage = pd.DataFrame([{'set': bess_param['constraint'],
    #                             'unit': bess_param['unit'],
    #                             'service': 'energy',
    #                             'coefficient': -1*filt_mirror['coefficient'].values[0]
    #                            }])
    # unit_generic_lhs = pd.concat([unit_generic_lhs,new_storage],ignore_index=True)
    df = crm_provider
    df.insert(1,'coefficient',-1*filt_mirror['coefficient'].values[0])

    unit_generic_lhs = pd.concat([unit_generic_lhs,df.loc[:,['set','unit','service','coefficient']]]\
                                 ,ignore_index=True) 
    
    return unit_generic_lhs

def append_rbuy_rhs(generic_rhs, crm_buyer_param):
    generic_rhs = pd.concat([generic_rhs,crm_buyer_param.loc[:,['set','rhs','type']]]\
                            ,ignore_index=True)
    return generic_rhs

def append_rbuy_violationcosts(violation_costs, crm_buyer_param):
    violation_costs = pd.concat([violation_costs,crm_buyer_param.loc[:,['set','cost']]]\
                                ,ignore_index=True)
    return violation_costs

def append_rbuy_lhs(unit_generic_lhs, crm_buyer_param):
    unit_generic_lhs = pd.concat([unit_generic_lhs,crm_buyer_param.loc[:,['set','unit','service','coefficient']]]\
                                 ,ignore_index=True)
    return unit_generic_lhs

def format_crm_buyers(df):
    df.insert(0,'set','CRM_'+df['unit'])
    df.insert(1,'rhs',df['dispatch'])
    df.insert(2,'type','>=')
    df.insert(3,'cost', 10000000.0)
    df.insert(4,'coefficient', 1.0)
    return df



def market_revenue(units, prices, dispatch):

    adjusted_prices = pd.merge(units,prices,how='inner', \
                               on='region')
        
    adjusted_prices['mlf_price'] = adjusted_prices['price'] * adjusted_prices['loss_factor']
    
    energy_only = dispatch[dispatch['service'] == 'energy']
    
    energy_revenue = pd.merge(adjusted_prices,energy_only, \
                              how='inner',left_on=['interval','unit'], \
                                  right_on=['interval','unit'])
    
    energy_revenue['revenue'] = energy_revenue['mlf_price'] * energy_revenue['dispatch']*(1/12)
       
    energy_revenue.loc[energy_revenue['dispatch_type'] == 'load','revenue'] \
        = energy_revenue['revenue'] * -1
        
    return energy_revenue[['interval','unit','dispatch_type','revenue']] 


def relief_revenue(units, prices, dispatch):
    signed_dispatch = dispatch.merge(right=units.loc[:,['unit','dispatch_type']], on='unit')
    signed_dispatch.loc[:,'dispatch'] = np.where(signed_dispatch['dispatch_type']=='load',\
                                                 -1*signed_dispatch['dispatch'],signed_dispatch['dispatch'])
    signed_dispatch['revenue'] = signed_dispatch['dispatch'] * prices['price'].values[0] * (1/12)
    
    return signed_dispatch.loc[:,['unit','revenue']]

def validate_spot_with_crm_error(interval, orig_market, new_market):
    const_df = new_market._constraints_rhs_and_type['generic']
    const_data = const_df[const_df['set'].str.contains('CRM_')]
    const_data = const_data[~const_data['set'].str.contains('STORAGE')]
    print(const_data)

    dis_1 = orig_market.get_unit_dispatch()
    dis_1.insert(0,'interval',interval)
    dis_1 = dis_1[dis_1['service'] == 'energy']

    dis_2 = new_market.get_unit_dispatch()
    dis_2.insert(0,'interval',interval)
    dis_2 = dis_2[dis_2['service'] == 'energy']

    for row in const_data['set']:
        unit_id = row[4:]
        const_value = float(const_data.loc[const_data['set'] == row, 'rhs'])
        
        unit_dis_1 = dis_1[dis_1['unit'] == unit_id]
        unit_dis_2 = dis_2[dis_2['unit'] == unit_id]
        unit_dis_diff = round(unit_dis_2['dispatch'] - unit_dis_1['dispatch'],2)
    
        error = float(round(unit_dis_diff - const_value,2)) # in MWs
        if error < -0.01:
            warnings.warn(f"Spot Market Dispatch does not satisfy CRM participant constraint by {error} MWs")
    
    if const_data.empty:
        error = np.NaN
    return pd.DataFrame({'interval': [interval], 'error': [error]}) 

class relief_market:
    
    def __init__(self, eligible_units, energy_prc):
        self._market = None
        self._unit_info = eligible_units
        self._region = list(self._unit_info['region'].unique())
        self._volume_bids = None
        self._price_bids = None
        self._cleared_energy_prc = energy_prc
        
        self.config()
        
    def config(self):
        # Define crm units as those involved in specific constraint   
        self._unit_info['dispatch_type'] = 'load'
        self._unit_info['loss_factor'] = 1.0
        
        # Add storage unit as the congestion relief provider
        new_storage = pd.DataFrame([{'unit': 'STORAGE',
                                    'region': self._unit_info['region'][0],
                                    'dispatch_type': 'generator',
                                    'loss_factor': 1.0}])
        self._unit_info = pd.concat([self._unit_info,new_storage],ignore_index=True)

        # Create Local Congestion Relief Market
        self._market = markets.SpotMarket(unit_info=self._unit_info, market_regions=self._region)
        
        # Set null demand
        demand = pd.DataFrame({
        'region': [self._region[0]],
        'demand': [0.0]
        })
        self._market.set_demand_constraints(demand)
        return
        
    def default_bids_offers(self, storage_mw=100.0, storage_offer=10.0):
        # Define Congestion Relief Volume Bids
        volume_bids = pd.DataFrame({
            'unit': self._unit_info['unit'].to_list(),
            '1': [0.0]*len(self._unit_info['unit'].to_list())
        })
        volume_bids.loc[volume_bids['unit']=='STORAGE','1'] = storage_mw
        self._volume_bids = volume_bids
                
        # Define Congestion Relief Price Bids
        price_bids = pd.DataFrame({
            'unit': self._unit_info['unit'].to_list(),
            '1': [0.0]*len(self._unit_info['unit'].to_list())    
        })
        price_bids.loc[volume_bids['unit']=='STORAGE','1'] = storage_offer
        self._price_bids = price_bids
        return
        
    def bid_into_crm(self, duid, price, volume):
        self._volume_bids.loc[self._volume_bids['unit'] == duid,'1'] = volume
        self._price_bids.loc[self._price_bids['unit'] == duid,'1'] = price
        return

    def cap_price(self, spotpricecap = None):
        # Caps storage from exerting market power
        if float(self._price_bids.loc[self._price_bids['unit'] == 'STORAGE','1']) > spotpricecap:
            self._price_bids.loc[self._price_bids['unit'] == 'STORAGE','1'] = spotpricecap   

        return
    
    def cap_relief_provision(self, dispatch_const_diff):
        unit_list = list(self._unit_info['unit'])
        sel_units = dispatch_const_diff[dispatch_const_diff['unit'].isin(unit_list)]
        unit_list.remove('STORAGE')
        for unit in unit_list:
            max_relief = float(sel_units[sel_units['unit'] == unit]['dispatch_diff'])
            if float(self._volume_bids.loc[self._volume_bids['unit'] == unit,'1']) > max_relief:
                self._volume_bids.loc[self._volume_bids['unit'] == unit,'1'] = max_relief
        return
    
    def get_unit_info(self):
        return self._unit_info
        
    def get_vol_bids(self):
        return self._volume_bids
    
    def get_prc_bids(self):
        return self._price_bids
        
    def dispatch(self, trim_price=True):
        self._market.set_unit_volume_bids(self._volume_bids)
        self._market.set_unit_price_bids(self._price_bids)
        self._market.dispatch()
        
        # Where units are buying quantity display as 'load' by negating dispatch
        raw_dispatch = self._market.get_unit_dispatch()
        raw_dispatch.loc[raw_dispatch['unit'] != 'STORAGE','dispatch'].apply(lambda x: -1*x)
        
        prices = self._market.get_energy_prices()
        if trim_price:
            prices['price'] = self._cleared_energy_prc
        
        return {'units': self._market._unit_info, 'prices': prices, 'dispatch': raw_dispatch}
    
    