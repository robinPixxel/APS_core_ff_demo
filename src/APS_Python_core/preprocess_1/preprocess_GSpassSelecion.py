# X=pd.DataFrame({'a':[1,2,3,3],'b':[[1,5],[1,5],[2,5],[2,6]]})
# X['r'] = X.groupby('a')['b'].rank(method='dense')
import pandas as pd
import logging
from APS_Python_core.utils import get_conflicting_dict,get_prev_TW_index # get_thermal_bucket
from concurrent.futures import ThreadPoolExecutor
from APS_Python_core.themal_buckets import get_thermal_bucket
import time
import json

class GSPassPreprocess:
    def __init__(self, GS_pass_df,config,system_requirment_input_dict,logger):
        '''
        GS_pass_df : ['gs_id', 'AOS', 'LOS', 'sat_id', 'aos_offset', 'los_offset']
        '''
        self.GS_pass_df = GS_pass_df
        self.GS_pass_df['sat_id'] = self.GS_pass_df['sat_id'].astype(str)
        self.GS_pass_df['gs_id'] = self.GS_pass_df['gs_id'].astype(str)
        self.system_requirment_input_dict = system_requirment_input_dict

        self.setup_time_S2G = 120
        self.setup_time_G2S = 120 
        
        self.config = config
        self.logger = logger
        
        self.data = {}   

    def create_dict(self):
        # get satellite and GS ID
        self.data['satellite_id'] = list(self.GS_pass_df ['sat_id'].unique())
        self.data['GS_id'] = list(self.GS_pass_df ['gs_id'].unique())

        #concat sat_id and gsID
        self.GS_pass_df['concat_gsid_satid'] = self.GS_pass_df['sat_id'] + '_' + self.GS_pass_df['gs_id']
        self.data['Concat_SatIdGSID'] = list(self.GS_pass_df['concat_gsid_satid'].unique())
        #self.GS_pass_df.groupby(['SatID','GsID']).agg(TW_start=('abs_start')

        # to get dict concat satID and gsID list with sat as key
        self.GS_pass_df['TW'] = self.GS_pass_df[['aos_offset','los_offset']].apply(list,axis=1)
        self.GS_pass_df['tw_rank'] = self.GS_pass_df.groupby(['concat_gsid_satid'])['TW'].rank(method='dense')
        self.GS_pass_groupedBySat_df0 = self.GS_pass_df.groupby(['sat_id']).agg(csg_list=('concat_gsid_satid',set)).reset_index()
        self.data['csgList_s'] = dict(zip(self.GS_pass_groupedBySat_df0['sat_id'],\
                                                  self.GS_pass_groupedBySat_df0['csg_list']))
        self.GS_pass_groupedByGS_df0 = self.GS_pass_df.groupby(['gs_id']).agg(csg_list=('concat_gsid_satid',set)).reset_index()
        self.data['csgList_gs'] = dict(zip(self.GS_pass_groupedByGS_df0['gs_id'],\
                                                  self.GS_pass_groupedByGS_df0['csg_list']))
        
        #1. to get dict concat TW list with concat sat and gs  as key
        #2. to get dict concat TW Index list with concat sat and gs  as key
        self.GS_pass_grouped_df = self.GS_pass_df.groupby(['concat_gsid_satid']).agg(tw_list=('TW',list),\
                                                                               tw_index_list=('tw_rank',list)).reset_index()
       
        self.data['TW_list_SatIdGSID'] = dict(zip(self.GS_pass_grouped_df['concat_gsid_satid'],\
                                                  self.GS_pass_grouped_df['tw_list']))
        self.data['TW_index_list_SatIdGSID'] = dict(zip(self.GS_pass_grouped_df['concat_gsid_satid'],\
                                                        self.GS_pass_grouped_df['tw_index_list']))

        #1. to get dict TW  with concat sat and gs and TWIndex  as key
        self.GS_pass_df['concat_gsid_satid_twindex'] = self.GS_pass_df['concat_gsid_satid']+'_'+ self.GS_pass_df['tw_rank'].astype(str)
        self.GS_pass_groupedBySat_df0 = self.GS_pass_df.groupby(['sat_id']).agg(csgk_list=('concat_gsid_satid_twindex',set)).reset_index()
        self.data['csgkList_s'] = dict(zip(self.GS_pass_groupedBySat_df0['sat_id'],\
                                                  self.GS_pass_groupedBySat_df0['csgk_list']))
        self.GS_pass_groupedByGS_df0 = self.GS_pass_df.groupby(['gs_id']).agg(csgk_list=('concat_gsid_satid_twindex',set)).reset_index()
        self.data['csgkList_gs'] = dict(zip(self.GS_pass_groupedByGS_df0['gs_id'],\
                                                  self.GS_pass_groupedByGS_df0['csgk_list']))
        

        self.data['TW_csgk'] = dict(zip(self.GS_pass_df['concat_gsid_satid_twindex'],\
                                                  self.GS_pass_df['TW']))
        
        # SG1K1G2K2_pair :to get conflicting pairs for same satellite two GS concflicing.
        # SG1K1G2K2_pair :to get conflicting pairs for same satellite two GS concflicing.  
        self.data['SG1K1G2K2_pair'] = {}
        self.data['SG1K1G2K2_pair']['sgk_list'] = self.data['csgkList_s']
        self.data['SG1K1G2K2_pair']['domain_of_csgk'] = {}
        self.data['SG1K1G2K2_pair']['domain_of_csgk'] = get_conflicting_dict(self.GS_pass_df,self.data['SG1K1G2K2_pair']['domain_of_csgk'],self.setup_time_S2G,'sat_id')
                                                    
        
        self.data['GS1K1S2K2_pair'] = {}
        self.data['GS1K1S2K2_pair']['sgk_list'] = self.data['csgkList_gs']
        self.data['GS1K1S2K2_pair']['domain_of_csgk'] = {}
        self.data['GS1K1S2K2_pair']['domain_of_csgk'] = get_conflicting_dict(self.GS_pass_df,self.data['GS1K1S2K2_pair']['domain_of_csgk'],self.setup_time_G2S)
                   
             
        self.data['get_satellite'] =  dict(zip(self.GS_pass_df['concat_gsid_satid'],\
                                                  self.GS_pass_df['sat_id']))

        self.data['get_grondstation'] = dict(zip(self.GS_pass_df['concat_gsid_satid'],\
                                                  self.GS_pass_df['gs_id']))
        self.data['get_AOS'] = dict(zip(self.GS_pass_df['concat_gsid_satid_twindex'],\
                                                  self.GS_pass_df['aos_offset']))
        self.data['get_LOS'] = dict(zip(self.GS_pass_df['concat_gsid_satid_twindex'],\
                                                  self.GS_pass_df['los_offset']))
               
        
        # initial_thermal_value_and capacity
        self.data['thermal_capacity'] = {s: self.system_requirment_input_dict["thermal_data_"][(s,"XBT")][1] for s in self.data['satellite_id']}
        self.data['initial_thermal_value'] = {s: self.system_requirment_input_dict["thermal_data_"][(s,"XBT")][0] for s in self.data['satellite_id']}

        #Setup time

        self.data['gs_pass_setup_times'] = self.system_requirment_input_dict["setup_time_"]



    def get_thermal_constraints_data(self):

        self.logger.info("=====GA pASS THERMAL======")
        if self.config['constraints']['Thermal_constraints_GS_pass']:
            self.logger.info("=====GA pASS THERMAL Preprocess======")

            with ThreadPoolExecutor() as executor:
               
                self.data['heatTimeBucket_SCT_dict__s']  = {s:executor.submit(get_thermal_bucket, self.data['initial_thermal_value'][s], \
                                                self.system_requirment_input_dict["thermal_data_"][(s,"XBT")][2] ,\
                                                self.system_requirment_input_dict["thermal_data_"][(s,"XBT")][3],
                                                self.data['thermal_capacity'][s],\
                                                self.system_requirment_input_dict["thermal_data_"][(s,"XBT")][4],\
                                                self.system_requirment_input_dict["thermal_data_"][(s,"XBT")][5],\
                                                self.system_requirment_input_dict["thermal_data_"][(s,"XBT")][6],\
                                                self.system_requirment_input_dict["thermal_data_"][(s,"XBT")][7],\
                                                self.system_requirment_input_dict["thermal_data_"][(s,"XBT")][8],\
                                                operation = "downlinking_from_Readout").result() for s in self.data['satellite_id']}
                

            self.data['max_XBT_heat_dict'] = {s : self.data['heatTimeBucket_SCT_dict__s'][s]['max_time_heat'] \
                                              for s in self.data['satellite_id'] }
            self.logger.info("max_XBT_heat_dict: %s", json.dumps(self.data["max_XBT_heat_dict"]))
        
            for s in self.data['satellite_id'] :
                del self.data['heatTimeBucket_SCT_dict__s'][s]['max_time_heat']
           
            self.data['prev_tWList__s_TWI_dict__s'] = {}
            GS_pass_df_copy = self.GS_pass_df.copy()
            GS_pass_df_copy.rename(columns={'aos_offset':'start_time','los_offset':'end_time'},inplace=True)
            GS_pass_df_copy.sort_values(["start_time"],inplace=True) # not necessary by satellite as we are filtering satellite 
    
            for s in self.data['satellite_id']:
                to_get_prev_index_df = GS_pass_df_copy[GS_pass_df_copy['sat_id']==s]
                #for global s_TWI
                self.data['prev_tWList__s_TWI_dict__s'][s] = get_prev_TW_index(to_get_prev_index_df,'concat_gsid_satid_twindex','concat_gsid_satid')
            
            self.GS_pass_df['list'] = self.GS_pass_df[['concat_gsid_satid','sat_id','concat_gsid_satid_twindex','aos_offset','los_offset','tw_rank']].apply(lambda a : [a['sat_id'],a['concat_gsid_satid'],a['concat_gsid_satid_twindex'],a['aos_offset'],a['los_offset'],a['tw_rank']],axis=1)
            self.data['prev_thermal_list'] = list(self.GS_pass_df['list'])

    def preprocess(self):
        
        self.create_dict()
        
        self.get_thermal_constraints_data()
       
        return self.data
        

    