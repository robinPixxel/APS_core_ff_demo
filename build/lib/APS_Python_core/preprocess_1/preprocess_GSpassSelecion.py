# X=pd.DataFrame({'a':[1,2,3,3],'b':[[1,5],[1,5],[2,5],[2,6]]})
# X['r'] = X.groupby('a')['b'].rank(method='dense')
import pandas as pd
from APS_Python_core.utils import get_flag_for_gs_pass
from APS_Python_core.utils import get_conflicting_dict,get_thermal_bucket,get_prev_TW_index

class GSPassPreprocess:
    def __init__(self, GS_pass_df,config):
        '''
        GS_pass_df : ['GsID', 'AOS', 'LOS', 'SatID', 'AOSOffset', 'LOSOffset']
        '''
        self.GS_pass_df = GS_pass_df
        self.GS_pass_df['SatID'] = self.GS_pass_df['SatID'].astype(str)
        self.GS_pass_df['GsID'] = self.GS_pass_df['GsID'].astype(str)
        self.setup_time_S2G = 120 
        self.setup_time_G2S = 120 
        self.config = config
        
        self.data = {}   

    # def get_conflicting_dict(self,different_setup_time,conflicting_by = 'SatID',conflicting_on = 'GsID', different_master_key = 'GS1K1S2K2_pair'):

    #     for on_item in self.GS_pass_df[conflicting_on].unique():
    #         this_df = self.GS_pass_df[self.GS_pass_df[conflicting_on] == on_item ]
    #         self.data[different_master_key]['sgk_list'] [on_item] = this_df['concat_gsid_satid_TWIndex'].unique()
    #         for csgk in this_df['concat_gsid_satid_TWIndex'].unique():
    #             that_df = this_df[this_df['concat_gsid_satid_TWIndex'] == csgk]
    #             this_LOS = list(that_df['LOSOffset'].unique())[0]
    #             this_AOS = list(that_df['AOSOffset'].unique())[0]
    #             this_by_item = list(that_df[conflicting_by].unique())[0]

    #             that_df1 = this_df[this_df['AOSOffset'] >= different_setup_time  + this_LOS]
    #             that_df2 = this_df[this_df['LOSOffset'] <= this_AOS - different_setup_time]
    #             that_df3 = pd.concat([that_df1,that_df2])
                
    #             not_needed = list(that_df3['concat_gsid_satid_TWIndex'].unique())
    #             that_df = this_df[~this_df['concat_gsid_satid_TWIndex'].isin(not_needed)]

    #             that_df = that_df[that_df[conflicting_by] != this_by_item ]
    #             that_df = that_df[that_df['concat_gsid_satid_TWIndex'] != csgk]

                
    #             self.data[different_master_key]['domain_of_csgk'] [csgk] = list(that_df['concat_gsid_satid_TWIndex'].unique())
                    
    #     pass     

    def create_dict(self):
        # get satellite and GS ID
        self.data['satellite_id'] = list(self.GS_pass_df ['SatID'].unique())
        self.data['GS_id'] = list(self.GS_pass_df ['GsID'].unique())

        #concat satID and gsID
        self.GS_pass_df['concat_gsid_satid'] = self.GS_pass_df['SatID'] + '_' + self.GS_pass_df['GsID']
        self.data['Concat_SatIdGSID'] = list(self.GS_pass_df['concat_gsid_satid'].unique())
        #self.GS_pass_df.groupby(['SatID','GsID']).agg(TW_start=('abs_start')

        # to get dict concat satID and gsID list with sat as key
        self.GS_pass_df['TW'] = self.GS_pass_df[['AOSOffset','LOSOffset']].apply(list,axis=1)
        self.GS_pass_df['TW_rank'] = self.GS_pass_df.groupby(['concat_gsid_satid'])['TW'].rank(method='dense')
        self.GS_pass_groupedBySat_df0 = self.GS_pass_df.groupby(['SatID']).agg(csg_list=('concat_gsid_satid',set)).reset_index()
        self.data['csgList_s'] = dict(zip(self.GS_pass_groupedBySat_df0['SatID'],\
                                                  self.GS_pass_groupedBySat_df0['csg_list']))
        self.GS_pass_groupedByGS_df0 = self.GS_pass_df.groupby(['GsID']).agg(csg_list=('concat_gsid_satid',set)).reset_index()
        self.data['csgList_gs'] = dict(zip(self.GS_pass_groupedByGS_df0['GsID'],\
                                                  self.GS_pass_groupedByGS_df0['csg_list']))
        
        #1. to get dict concat TW list with concat sat and gs  as key
        #2. to get dict concat TW Index list with concat sat and gs  as key
        self.GS_pass_grouped_df = self.GS_pass_df.groupby(['concat_gsid_satid']).agg(TW_list=('TW',list),\
                                                                               TW_index_list=('TW_rank',list)).reset_index()
       
        self.data['TW_list_SatIdGSID'] = dict(zip(self.GS_pass_grouped_df['concat_gsid_satid'],\
                                                  self.GS_pass_grouped_df['TW_list']))
        self.data['TW_index_list_SatIdGSID'] = dict(zip(self.GS_pass_grouped_df['concat_gsid_satid'],\
                                                        self.GS_pass_grouped_df['TW_index_list']))

        #1. to get dict TW  with concat sat and gs and TWIndex  as key
        self.GS_pass_df['concat_gsid_satid_TWIndex'] = self.GS_pass_df['concat_gsid_satid']+'_'+ self.GS_pass_df['TW_rank'].astype(str)
        self.GS_pass_groupedBySat_df0 = self.GS_pass_df.groupby(['SatID']).agg(csgk_list=('concat_gsid_satid_TWIndex',set)).reset_index()
        self.data['csgkList_s'] = dict(zip(self.GS_pass_groupedBySat_df0['SatID'],\
                                                  self.GS_pass_groupedBySat_df0['csgk_list']))
        self.GS_pass_groupedByGS_df0 = self.GS_pass_df.groupby(['GsID']).agg(csgk_list=('concat_gsid_satid_TWIndex',set)).reset_index()
        self.data['csgkList_gs'] = dict(zip(self.GS_pass_groupedByGS_df0['GsID'],\
                                                  self.GS_pass_groupedByGS_df0['csgk_list']))
        

        self.data['TW_csgk'] = dict(zip(self.GS_pass_df['concat_gsid_satid_TWIndex'],\
                                                  self.GS_pass_df['TW']))
        
        # SG1K1G2K2_pair :to get conflicting pairs for same satellite two GS concflicing.
        # SG1K1G2K2_pair :to get conflicting pairs for same satellite two GS concflicing.  
        self.data['SG1K1G2K2_pair'] = {}
        self.data['SG1K1G2K2_pair']['sgk_list'] = self.data['csgkList_s']
        self.data['SG1K1G2K2_pair']['domain_of_csgk'] = {}
        self.data['SG1K1G2K2_pair']['domain_of_csgk'] = get_conflicting_dict(self.GS_pass_df,self.data['SG1K1G2K2_pair']['domain_of_csgk'],self.setup_time_S2G,'SatID')
                                                    
        
        self.data['GS1K1S2K2_pair'] = {}
        self.data['GS1K1S2K2_pair']['sgk_list'] = self.data['csgkList_gs']
        self.data['GS1K1S2K2_pair']['domain_of_csgk'] = {}
        self.data['SG1K1G2K2_pair']['domain_of_csgk'] = get_conflicting_dict(self.GS_pass_df,self.data['GS1K1S2K2_pair']['domain_of_csgk'],self.setup_time_G2S)
                   
             
        self.data['get_satellite'] =  dict(zip(self.GS_pass_df['concat_gsid_satid'],\
                                                  self.GS_pass_df['SatID']))

        self.data['get_grondstation'] = dict(zip(self.GS_pass_df['concat_gsid_satid'],\
                                                  self.GS_pass_df['GsID']))
        self.data['get_AOS'] = dict(zip(self.GS_pass_df['concat_gsid_satid_TWIndex'],\
                                                  self.GS_pass_df['AOSOffset']))
        self.data['get_LOS'] = dict(zip(self.GS_pass_df['concat_gsid_satid_TWIndex'],\
                                                  self.GS_pass_df['LOSOffset']))
               
        
        # initial_thermal_value_and capacity
        self.data['thermal_capacity'] = {s: 70 for s in self.data['satellite_id']}
        self.data['initial_thermal_value'] = {s: 20 for s in self.data['satellite_id']}

    def get_thermal_constraints_data(self):
        print("=====GA pASS THERMAL======")
        if self.config['constraints']['Thermal_constraints_GS_pass']:
            self.data['heatTimeBucket_SCT_dict__s'] = {
                                        s : get_thermal_bucket(self.data['initial_thermal_value'][s], \
                                        self.config['thermal_parameters']['XBT_heat_eqn'] ,\
                                        self.config['thermal_parameters']['XBT_cool_eqn'],\
                                        self.data['thermal_capacity'][s]) \
                                        for s in self.data['satellite_id']
                                        }
        
            self.data['max_XBT_heat_dict'] = {s : self.data['heatTimeBucket_SCT_dict__s'][s]['max_time_heat'] \
                                              for s in self.data['satellite_id'] }
        
            for s in self.data['satellite_id'] :
                del self.data['heatTimeBucket_SCT_dict__s'][s]['max_time_heat']
            print(self.data['heatTimeBucket_SCT_dict__s'],self.data['heatTimeBucket_SCT_dict__s'])
            self.data['prev_tWList__s_TWI_dict__s'] = {}
            GS_pass_df_copy = self.GS_pass_df.copy()
            GS_pass_df_copy.rename(columns={'AOSOffset':'start_time','LOSOffset':'end_time'},inplace=True)
            GS_pass_df_copy.sort_values(["concat_gsid_satid","start_time"],inplace=True) # not necessary by satellite as we are filtering satellite 
            print(len(self.data['satellite_id']))
            for s in self.data['satellite_id']:
                to_get_prev_index_df =GS_pass_df_copy[GS_pass_df_copy['SatID']==s]
                #for global s_TWI
                self.data['prev_tWList__s_TWI_dict__s'][s] = get_prev_TW_index(to_get_prev_index_df,'TW_rank','concat_gsid_satid')
            self.GS_pass_df['list'] = self.GS_pass_df[['GsID','SatID','TW_rank','AOSOffset','LOSOffset']].apply(lambda a : [a['SatID'],a['GsID'],a['TW_rank'],a['AOSOffset'],a['LOSOffset']],axis=1)
            self.data['prev_thermal_list'] = list(self.GS_pass_df['list'])

    def preprocess(self):
        
        self.create_dict()
        
        self.get_thermal_constraints_data()
       
        return self.data
        

    