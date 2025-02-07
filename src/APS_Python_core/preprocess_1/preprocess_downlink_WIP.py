import pandas as pd
import datetime
import math
from APS_Python_core.utils import get_flag_for_image,remove_opportunities_conflict_GSpass,get_EcStEnd_list,get_delivery_time

class DownlinkingPreProcess:
    def __init__(self, image_table_df,img_capture_result,config):
        '''
        img_capture_result : only gs passs result
        
        '''
        self.image_table_df = image_table_df
        self.img_capture_result = img_capture_result
        self.config = config
        self.base_time_stamp = config['base_time_stamp_downlink']

    def preprocess(self):
        all_satellite_list = list(self.image_table_df['sat_id'].unique())
        all_capture_list = list(self.image_table_df['image_id'].unique())
        all_gs_list = list(self.img_capture_result['gs_id'].unique())

        #1
        self.img_capture_result.sort_values(by='start_time',inplace=True)
        gs_list_df = self.img_capture_result.groupby('sat_id').agg( gs_list = ('gs_id',set)).reset_index()
        gs_list_df['sat_priority_list'] = gs_list_df['gs_list'].apply(lambda a : [i + 1 for i in range(len(a))])
        
        gs_list_df['sat_priority_dict'] = gs_list_df[['gs_list','sat_priority_list']].\
            apply(lambda a : {v:a['sat_priority_list'][i] for i,v in enumerate(a['gs_list']) },axis=1)

        ground_station_list__sat = dict(zip(gs_list_df['sat_id'],gs_list_df['gs_list']))
        sat_gs_prioritydict__sat = dict(zip(gs_list_df['sat_id'],gs_list_df['sat_priority_dict']))


        sat_in_gsPass_set = set(self.img_capture_result['sat_id'])
        sat_in_imageTable_set = set(self.image_table_df['sat_id'])
        satinImage_butNotInGsPass = sat_in_imageTable_set - sat_in_gsPass_set
        ground_station_list__sat.update({s:[] for s in satinImage_butNotInGsPass })
        sat_gs_prioritydict__sat.update({s:{} for s in satinImage_butNotInGsPass})

        #2
        capture_list_df = self.image_table_df.groupby('sat_id').agg( capture_list = ('image_id',set)).reset_index()
        capture_list__sat = dict(zip(capture_list_df['sat_id'],capture_list_df['capture_list']))
        
        #3
        downlinkGlobalPriority__imgID = dict(zip(self.image_table_df['image_id'],self.image_table_df['global_priority'] ))

        # [(N_tilestrips*512) x (N_bands x 8192)] x 1.02 x 10
        # imageSize = (((df_DSP[n,5]*512)*(df_DSP[n,7]*8192))*1.02*10)/(8*1024*1024*1024)

        #4 TODO1
        self.image_table_df['memory_per_tileStrip'] = (self.image_table_df['bands']* 8192) * (1 * 512) *1.02 * 10 /(8*1024*1024*1024)#TODO1
        self.image_table_df['time_per_tileStrip'] = self.image_table_df['memory_per_tileStrip'] * 0.5 # ASSuming 0.5 GBPS # TODO2
        time_per_tileStrip__imgID = dict(zip(self.image_table_df['image_id'],self.image_table_df['time_per_tileStrip'] ))
        
        #5
        NoOfTileStrip__imgID = dict(zip(self.image_table_df['image_id'],self.image_table_df['tilestrips'] ))

        #6
        self.img_capture_result['TW'] = self.img_capture_result[['start_time','end_time']].apply(lambda a : [a['start_time'],a['end_time']],axis = 1)
        self.img_capture_result['tw_index'] = self.img_capture_result.groupby(['sat_id','gs_id'])['TW'].rank(method='dense')

        satGs_grouped_TWlist_df = self.img_capture_result.groupby(['sat_id','gs_id']).agg(tw_list= ('TW',list),\
                                                                                        tw_index_list=('tw_index',list)).reset_index()
        satGs_grouped_TWlist_df['concat_sat_gs'] = satGs_grouped_TWlist_df['sat_id'] + '_' + satGs_grouped_TWlist_df['gs_id']
        TW_list__concatSatGs = dict(zip(satGs_grouped_TWlist_df['concat_sat_gs'],satGs_grouped_TWlist_df['tw_list']))
        TW_index_list__concatSatGs = dict(zip(satGs_grouped_TWlist_df['concat_sat_gs'],satGs_grouped_TWlist_df['tw_index_list']))

        #7 
        bands__imgID = dict(zip(self.image_table_df['image_id'],self.image_table_df['bands'] ))

        #8
        #globalpriority__imgID = dict(zip(self.image_table_df['image_id'],self.image_table_df['global_priority'] ))

        #9
        self.image_table_df['base_time_stamp'] = self.base_time_stamp
        
        #====================================TO BE TAKEN=================================================================================
        
        # based on standard , expeditory and super expeditory delivery
        self.image_table_df['capture_date'] = pd.to_datetime(self.image_table_df['capture_date'])
        self.image_table_df['deliver_date'] = self.image_table_df[['delivery_type','capture_date']].apply(lambda a : get_delivery_time(a['delivery_type'],a['capture_date']),axis =1  )
        self.image_table_df['diff'] = abs(pd.to_datetime(self.image_table_df['deliver_date']) - pd.to_datetime(self.image_table_df['base_time_stamp']))

        self.image_table_df['diff'] = self.image_table_df[['diff']].apply(lambda a : abs(a['diff'].total_seconds()),axis=1)
        self.image_table_df['local_priority_based_on_due_delivery'] = self.image_table_df['diff'].apply(lambda a : 1/(a/3600))
        
        #====================================TO BE TAKEN=================================================================================

        self.image_table_df['normalized_lp_dd'] = self.image_table_df['local_priority_based_on_due_delivery']/self.image_table_df['local_priority_based_on_due_delivery'].max()
        self.image_table_df['normalized_gp'] = self.image_table_df['global_priority']/self.image_table_df['global_priority'].max()
        self.image_table_df['normalized_gp'] = self.image_table_df[['normalized_gp','global_priority']].apply(lambda a : a['global_priority'] if a['global_priority'] >= 1000 else a['normalized_gp'],axis = 1 )

        LP_DD_Priority_imgID = dict(zip(self.image_table_df['image_id'],self.image_table_df['normalized_lp_dd'] ))

        self.image_table_df['normalized_tp'] = 0.9 * self.image_table_df['normalized_lp_dd'] + 0.1 * self.image_table_df['normalized_gp']
        totalPriority_imgID = dict(zip(self.image_table_df['image_id'],self.image_table_df['normalized_tp'] ))

        return {
                "all_capture_list":all_capture_list,\
                "all_satellite_list":all_satellite_list,\
                "ground_station_list__sat": ground_station_list__sat,\
                "capture_list__sat": capture_list__sat,\
                "downlinkGlobalPriority__imgID":downlinkGlobalPriority__imgID,\
                "time_per_tileStrip__imgID":time_per_tileStrip__imgID,\
                "NoOfTileStrip__imgID": NoOfTileStrip__imgID,\
                "TW_list__concatSatGs" : TW_list__concatSatGs,\
                "TW_index_list__concatSatGs": TW_index_list__concatSatGs,\
                "bands__imgID": bands__imgID,\
                "totalPriority_imgID":totalPriority_imgID,\
                "sat_gs_prioritydict__sat":sat_gs_prioritydict__sat,\
                # "assured_downlinking_imageSatIdList_list":assured_downlinking_imageSatIdList_list,\
                # "tile_strip_no__concatSatidImgId": tile_strip_no__concatSatidImgId,\
                "LP_DD_Priority_imgID":LP_DD_Priority_imgID
                }



        #ground_station_list__sat = dict(zip(gs_list_df['sat_id'],gs_list_df['gs_list']))


    