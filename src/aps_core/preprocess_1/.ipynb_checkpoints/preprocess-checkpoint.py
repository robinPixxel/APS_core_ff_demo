# X=pd.DataFrame({'a':[1,2,3,3],'b':[[1,5],[1,5],[2,5],[2,6]]})
# X['r'] = X.groupby('a')['b'].rank(method='dense')
import pandas as pd
from utils import get_flag_for_gs_pass

class GSPassPreprocess:
    def __init__(self, GS_pass_df):
        '''
        GS_pass_df : ['GsID', 'AOS', 'LOS', 'SatID', 'AOSOffset', 'LOSOffset', 'Eclipse']
        '''
        self.GS_pass_df = GS_pass_df
        self.GS_pass_df['SatID'] = self.GS_pass_df['SatID'].astype(str)
        self.GS_pass_df['GsID'] = self.GS_pass_df['GsID'].astype(str)
        self.setup_time_S2G = 120 
        self.setup_time_G2S = 120 
        
        self.data = {}

    def get_temporal_data(self):
        self.GS_pass_df.sort_values(by='AOSOffset',inplace=True)

        GS_pass_df_groupedSatID = self.GS_pass_df.groupby('SatID').agg(TW_list = ('TW',list),\
                                       TW_index = ('TW_rank',list),\
                                        GS_list=('GsID',list)).reset_index()
        
        df1 = pd.DataFrame({"time_index": [i for i in range(12*60*60)],
                          "SatID":[list(GS_pass_df_groupedSatID['SatID']) for i in range(12*60*60) ]})
        df2 = df1.explode('SatID')
        df3 = pd.merge(df2,GS_pass_df_groupedSatID,on='SatID',how='left')
        
        df3['flag'] = df3[['time_index','TW_list','TW_index','GS_list']]\
                    .apply(lambda a: get_flag_for_gs_pass(a['time_index'],\
                                                          a['TW_list'],\
                                                          a['TW_index'],a['GS_list']),axis =1 )
        
        
        df4 = df3[['time_index','SatID','flag']]
        df4[df4['flag']!= -1]

        self.data['temporal_data'] = {}
        for s in df4['SatID'].unique():
            df5 = df4[df4['SatID']==s]
            self.data['temporal_data'][s] = dict(zip(df5['time_index'],df5['flag']))
 
        

    def create_dict(self):
        # get satellite and GS ID
        
        self.data['satellite_id'] = list(self.GS_pass_df ['SatID'].unique())
        self.data['GS_id'] = list(self.GS_pass_df ['GsID'].unique())
        self.GS_pass_df['concat_gsid_satid'] = self.GS_pass_df['SatID'] + '_' + self.GS_pass_df['GsID']
        self.data['Concat_SatIdGSID'] = list(self.GS_pass_df['concat_gsid_satid'].unique())
        #self.GS_pass_df.groupby(['SatID','GsID']).agg(TW_start=('abs_start')
        self.GS_pass_df['TW'] = self.GS_pass_df[['AOSOffset','LOSOffset']].apply(list,axis=1)
        self.GS_pass_df['TW_rank'] = self.GS_pass_df.groupby(['concat_gsid_satid'])['TW'].rank(method='dense')

        self.GS_pass_grouped_df0 = self.GS_pass_df.groupby(['SatID']).agg(csg_list=('concat_gsid_satid',set)).reset_index()
        self.data['csgList_s'] = dict(zip(self.GS_pass_grouped_df0['SatID'],\
                                                  self.GS_pass_grouped_df0['csg_list']))
        
        self.GS_pass_grouped_df = self.GS_pass_df.groupby(['concat_gsid_satid']).agg(TW_list=('TW',list),\
                                                                               TW_index_list=('TW_rank',list)).reset_index()
       
        self.data['TW_list_SatIdGSID'] = dict(zip(self.GS_pass_grouped_df['concat_gsid_satid'],\
                                                  self.GS_pass_grouped_df['TW_list']))
        self.data['TW_index_list_SatIdGSID'] = dict(zip(self.GS_pass_grouped_df['concat_gsid_satid'],\
                                                        self.GS_pass_grouped_df['TW_index_list']))

        
        self.GS_pass_df['concat_gsid_satid_TWIndex'] = self.GS_pass_df['concat_gsid_satid']+'_'+ self.GS_pass_df['TW_rank'].astype(str)
        self.data['eclipse'] = dict(zip(self.GS_pass_df['concat_gsid_satid_TWIndex'],self.GS_pass_df['Eclipse']))
        
        self.data['TW_csgk'] = dict(zip(self.GS_pass_df['concat_gsid_satid_TWIndex'],\
                                                  self.GS_pass_df['TW']))
        
        self.data['SG1K1G2K2_pair'] = {}
        self.data['SG1K1G2K2_pair']['sgk_list'] = {}
        self.data['SG1K1G2K2_pair']['domain_of_csgk'] = {}
        for s in self.GS_pass_df['SatID'].unique():
            this_df = self.GS_pass_df[self.GS_pass_df['SatID'] == s ]
            self.data['SG1K1G2K2_pair']['sgk_list'] [s] = this_df['concat_gsid_satid_TWIndex'].unique() 
            for csgk in this_df['concat_gsid_satid_TWIndex'].unique():
                that_df = this_df[this_df['concat_gsid_satid_TWIndex'] == csgk]
                this_LOS = list(that_df['LOSOffset'].unique())[0]
                this_AOS = list(that_df['AOSOffset'].unique())[0]
                this_ground_station = list(that_df['GsID'].unique())[0]
                
                that_df = this_df[this_df['AOSOffset'] >= self.setup_time_S2G  + this_LOS]
                that_df = this_df[this_df['GsID'] != this_ground_station ]
                that_df = this_df[this_df['concat_gsid_satid_TWIndex'] != csgk]
                
                
                self.data['SG1K1G2K2_pair']['domain_of_csgk'] [csgk] = list(that_df['concat_gsid_satid_TWIndex'].unique())

        self.data['GS1K1S2K2_pair'] = {}
        self.data['GS1K1S2K2_pair']['sgk_list'] = {}
        self.data['GS1K1S2K2_pair']['domain_of_csgk'] = {}
        for g in self.GS_pass_df['GsID'].unique():
            this_df = self.GS_pass_df[self.GS_pass_df['GsID'] == g ]
            self.data['GS1K1S2K2_pair']['sgk_list'] [g] = this_df['concat_gsid_satid_TWIndex'].unique()
            for csgk in this_df['concat_gsid_satid_TWIndex'].unique():
                that_df = this_df[this_df['concat_gsid_satid_TWIndex'] == csgk]
                this_LOS = list(that_df['LOSOffset'].unique())[0]
                this_AOS = list(that_df['AOSOffset'].unique())[0]
                this_satellite = list(that_df['SatID'].unique())[0]
                
                that_df = this_df[this_df['AOSOffset'] >= self.setup_time_G2S + this_LOS]
                that_df = this_df[this_df['SatID'] != this_ground_station ]
                that_df = this_df[this_df['concat_gsid_satid_TWIndex'] != csgk]
                self.data['GS1K1S2K2_pair']['domain_of_csgk'] [csgk] = list(that_df['concat_gsid_satid_TWIndex'].unique())
                        
        self.data['get_satellite'] =  dict(zip(self.GS_pass_df['concat_gsid_satid'],\
                                                  self.GS_pass_df['SatID']))

        self.data['get_grondstation'] = dict(zip(self.GS_pass_df['concat_gsid_satid'],\
                                                  self.GS_pass_df['GsID']))
        self.data['get_AOS'] = dict(zip(self.GS_pass_df['concat_gsid_satid_TWIndex'],\
                                                  self.GS_pass_df['AOSOffset']))
        self.data['get_LOS'] = dict(zip(self.GS_pass_df['concat_gsid_satid_TWIndex'],\
                                                  self.GS_pass_df['LOSOffset']))

    
    def preprocess(self):
        
        self.create_dict()
        
        self.get_temporal_data()
       
        return self.data
        

    