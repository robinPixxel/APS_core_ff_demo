import pandas as pd
import datetime
import math
import logging
from APS_Python_core.utils import remove_opportunities_conflict_GSpass,get_EcStEnd_list,get_prev_TW_index,get_readout_TW,get_conflicting_dict,check_opportunities_conflict_GSpass #, get_thermal_bucket
import time
from concurrent.futures import ThreadPoolExecutor
import json
from APS_Python_core.themal_buckets import get_thermal_bucket


class ImageAquisitionProcess:
    def __init__(self, image_opportunity_df,Gs_pass_output_df,eclipse_df_dict,config,system_requirment_input_dict,logger ):
        '''
        image_opportunity_df : 'sat_id','strip_id','aoi_id','opportunity_start_time','opportunity_end_time','opportunity_start_offset','opportunity_end_offset','order_validity_end','cloud_cover','off_nadir','assured_task','eclipse'
        Gs_pass_output_df : 'sat_id','gs_id','start_time','end_time','eclipse'
        '''
        self.image_opportunity_df = image_opportunity_df
        self.system_requirment_input_dict = system_requirment_input_dict
        self.gsPass_df = Gs_pass_output_df
        self.setup_time =  system_requirment_input_dict["setup_time_"][("sat_id","strip_id")]
        self.eclipse_df_dict = eclipse_df_dict
        self.config = config
        self.logger = logger
        self.data = {}

    def get_priorities(self):
  
        self.image_opportunity_df['X'] = self.image_opportunity_df[['opportunity_start_time','opportunity_start_offset']].apply(lambda a: pd.to_datetime(a['opportunity_start_time']) - pd.DateOffset(seconds=a['opportunity_start_offset']),axis=1)
        self.image_opportunity_df['Y'] = self.image_opportunity_df[['opportunity_end_time','opportunity_end_offset']].apply(lambda a: pd.to_datetime(a['opportunity_end_time']) - pd.DateOffset(seconds=a['opportunity_end_offset']),axis=1)
        
        base_time_stamp = self.image_opportunity_df["X"].to_list()[0]

        self.logger.info("len_of_unique_base_time="+str(len(set(self.image_opportunity_df["X"].to_list()))))

        self.image_opportunity_df['base_timestamp'] = base_time_stamp
        self.image_opportunity_df['base_timestamp'] = self.image_opportunity_df['base_timestamp'].astype(str)
        self.image_opportunity_df['due_date_end_offset'] = pd.to_datetime(self.image_opportunity_df['order_validity_end']) - pd.to_datetime(self.image_opportunity_df['base_timestamp'])#self.image_opportunity_df['base_timestamp']
        self.image_opportunity_df['due_seconds_diff'] = self.image_opportunity_df[['due_date_end_offset']].apply(lambda a : a['due_date_end_offset'].total_seconds(),axis=1)
        #==============================================================================================================
        self.image_opportunity_df['local_priority_due_date'] = self.image_opportunity_df['due_seconds_diff'].apply(lambda a : abs(1/(a+0.0001)) )# 48 becuause if due date is less than 2 days from now it will exponentially increase for that denominator should be less than 1 .
        self.image_opportunity_df['local_priority_CC_based'] = abs(1 / (self.image_opportunity_df['cloud_cover']+0.0001)/(10  ))
        self.image_opportunity_df['local_priority_offNadir'] = abs(1 / (self.image_opportunity_df['off_nadir']+0.0001)/(8  ))

        #==============================================================================================================
        self.image_opportunity_df['normalized_local_priority_due_date'] = (self.image_opportunity_df['local_priority_due_date'] / self.image_opportunity_df['local_priority_due_date'].max())*1000#/ (self.image_opportunity_df['local_priority_due_date'].max() - self.image_opportunity_df['local_priority_due_date'].min())
        self.image_opportunity_df['normalized_local_priority_cc_based'] = (self.image_opportunity_df['local_priority_CC_based'] / self.image_opportunity_df['local_priority_CC_based'].max())*1000#/ (self.image_opportunity_df['local_priority_CC_based'].max() - self.image_opportunity_df['local_priority_CC_based'].min())
        self.image_opportunity_df['normalized_local_priority_off_nadir'] = (self.image_opportunity_df['local_priority_offNadir'] / self.image_opportunity_df['local_priority_offNadir'].max())*1000#/ (self.image_opportunity_df['local_priority_offNadir'].max() - self.image_opportunity_df['local_priority_offNadir'].min())
        max_GP_= self.image_opportunity_df[self.image_opportunity_df['global_priority']<1000]['global_priority'].max()
        self.image_opportunity_df['normalized_global_priority'] = (self.image_opportunity_df['global_priority'] / max_GP_)*1000#/ (self.image_opportunity_df['global_priority'].max() - self.image_opportunity_df['global_priority'].min()-0.00001)
        self.image_opportunity_df['normalized_global_priority'] = self.image_opportunity_df[['normalized_global_priority','global_priority']].apply(lambda a: a['global_priority']*1000 if  a['global_priority'] >=1000 else a['normalized_global_priority'],axis = 1)
        self.image_opportunity_df['normalized_local_priority_due_date'] = self.image_opportunity_df[['normalized_local_priority_due_date','due_seconds_diff']].apply(lambda a: abs(1/(a['due_seconds_diff']+0.001))*24*60*1000*1000 if  a['due_seconds_diff'] <= 24*60 else a['normalized_local_priority_due_date'],axis = 1)
        #==============================================================================================================
       
        self.image_opportunity_df['normalized_total_priority'] =  self.config['GP_weight']*self.image_opportunity_df['normalized_global_priority'] + \
                                                       self.config['DDLP_weight']*self.image_opportunity_df['normalized_local_priority_due_date']+\
                                                       self.config['CCLP_weight']*self.image_opportunity_df['normalized_local_priority_cc_based'] +\
                                                       self.config['ONLP_weight']*self.image_opportunity_df['normalized_local_priority_off_nadir']
        
        GlobalPriority__csjk = dict(zip(self.image_opportunity_df['concat_sat_id_encoded_strip_id_tw_index'],self.image_opportunity_df['normalized_global_priority']))
        TotalPriority__csjk = dict(zip(self.image_opportunity_df['concat_sat_id_encoded_strip_id_tw_index'],self.image_opportunity_df['normalized_total_priority']))
        
        self.image_opportunity_df['normalized_local_priority'] = self.image_opportunity_df['normalized_local_priority_due_date'] + self.image_opportunity_df['normalized_local_priority_cc_based'] +  self.image_opportunity_df['normalized_local_priority_off_nadir'] 
        Local_Priority__csjk = dict(zip(self.image_opportunity_df['concat_sat_id_encoded_strip_id_tw_index'],self.image_opportunity_df['normalized_local_priority']))

        original_stripLevel_df = self.image_opportunity_df[['encoded_strip_id','normalized_total_priority','normalized_local_priority','normalized_global_priority']]#.drop_duplicates()

        original_strip_grouped_Level_df = original_stripLevel_df.groupby('encoded_strip_id').agg(mean_gp=('normalized_global_priority',pd.Series.mean),\
                                                                                       mean_lP=('normalized_local_priority',pd.Series.mean),\
                                                                                       mean_tP=('normalized_total_priority',pd.Series.mean)).reset_index()
        
        TotalPriority__csj = dict(zip(original_strip_grouped_Level_df['encoded_strip_id'],original_strip_grouped_Level_df['mean_tP']))
        GlobalPriority__csj = dict(zip(original_strip_grouped_Level_df['encoded_strip_id'],original_strip_grouped_Level_df['mean_gp']))
        LocalPriority__csj = dict(zip(original_strip_grouped_Level_df['encoded_strip_id'],original_strip_grouped_Level_df['mean_lP']))



        self.success_metric_before.update({'original_Total_GP':original_strip_grouped_Level_df['mean_gp'].sum(),\
           'original_Total_LP':original_strip_grouped_Level_df['mean_lP'].sum(),\
           'original_Total_TP':original_strip_grouped_Level_df['mean_tP'].sum(),\
           'total_opportunities':original_strip_grouped_Level_df['encoded_strip_id'].nunique() })
        
        return {
               
                "GlobalPriority__csjk":GlobalPriority__csjk,\
                "TotalPriority__csjk":TotalPriority__csjk,\
                "Local_Priority__csjk":Local_Priority__csjk,\
                "TotalPriority__csj":TotalPriority__csj,\
                "GlobalPriority__csj":GlobalPriority__csj,\
                "Local_Priority__csj":LocalPriority__csj    
            }
                    
    def create_MTW_PTW(self):
        '''
        create mmeory time windows and power time windows list
        'satId','st','et','encoded_strip_id',eclipse,gs_pass_list,gs_TW_list 
        
        '''

        #Concating image and gspass table
        selected_image_opportunity_df = self.image_opportunity_df[['sat_id','encoded_strip_id','opportunity_start_offset','opportunity_end_offset','tw_index','gs_pass_conflict_list','flag_gs_pass_conflict']]
        gsPass_df = self.gsPass_df[['sat_id','gs_id','start_time','end_time','tw_index']]
        selected_image_opportunity_df.rename(columns = {'opportunity_start_offset':'start_time','opportunity_end_offset':'end_time'},inplace=True)
        imgGS_union_df = pd.concat([selected_image_opportunity_df,gsPass_df],join='outer')
        
        #logic for adding TW where no gs pass and imging can happen at all
        imgGS_union_df.sort_values(by='start_time',inplace=True)
        imgGS_union_df['till_now_max'] = imgGS_union_df.groupby('sat_id')['end_time'].cummax()
        imgGS_union_df['prev_max'] = imgGS_union_df.groupby('sat_id')['till_now_max'].shift(1)

        imgGS_union_noOperation_df = imgGS_union_df[imgGS_union_df['start_time'] > imgGS_union_df['prev_max'] + 1] 
        imgGS_union_noOperation_df['start_time1'] = imgGS_union_noOperation_df['prev_max'] + 1 #TODO1 +1 is okay ?
        imgGS_union_noOperation_df['end_time1'] = imgGS_union_noOperation_df['start_time'] - 1
        imgGS_union_noOperation_df['encoded_strip_id'] = 'no_i_no_g'
        imgGS_union_noOperation_df['gs_id'] = 'no_i_no_g'

        #adding First TW where no imaging and gs pass can happen at all

        '''
        First_TW_df = imgGS_union_df.groupby('sat_id').agg(max_end_time =('start_time','min')).reset_index()
        First_TW_df['max_end_time'] = First_TW_df['max_end_time'] -  1
        First_TW_df['min_start_time'] = 0

        First_TW_df1 = pd.DataFrame({'sat_id':list(First_TW_df['sat_id']),\
                            'end_time1': list(last_TW_df['max_end_time']),\
                            })

        First_TW_df1['start_time1'] = 0
        First_TW_df1['encoded_strip_id'] = 'no_i_no_g'
        '''

        #adding last TW where no imaging and gs pass can happen at all 
        '''
        # last_TW_df = imgGS_union_df.groupby('sat_id').agg(min_start_time =('end_time','max')).reset_index()
       
        # last_TW_df['min_start_time'] = last_TW_df['min_start_time'] +  1
        # last_TW_df1 = pd.DataFrame({'sat_id':list(last_TW_df['sat_id']),\
        #                     'start_time1': list(last_TW_df['min_start_time']),\
        #                     })

        # last_TW_df1['end_time1'] = self.config['scheduled_Hrs']*3600 - 1 #40
        # last_TW_df1['encoded_strip_id'] = 'no_i_no_g'
        # last_TW_df1['gs_id'] = 'no_i_no_g'
        '''
        #concat first and last TW
        '''
        # imgGS_union_noOperation_df = pd.concat([imgGS_union_noOperation_df,last_TW_df1])
        # imgGS_union_noOperation_dfimgGS_union_noOperation_df = pd.concat([imgGS_union_df1,last_TW_df1])
        '''
        #getting table having all TW with events Imaging,GSPass,noImgNoGsPass
        imgGS_union_noOperation_df = imgGS_union_noOperation_df.drop(['start_time', 'end_time','till_now_max','prev_max'], axis=1)
        imgGS_union_noOperation_df.rename(columns={'start_time1':'start_time','end_time1':'end_time'},inplace=True)
        #imgGS_union_df1 ==> contains TW without img and without gs pass  table without eclipse divide
        memory_based_df = pd.concat([imgGS_union_df,imgGS_union_noOperation_df])

        #Adding global TW index assuming all TW will be different for each opportunity of satellite. No eclipse and sunlit bifurcation
        memory_based_df['global_tw'] = memory_based_df[['start_time','end_time']].apply(list,axis=1)
        
        #Assuming different TW for each satellite 
        memory_based_df['memory_global_tw_index'] = memory_based_df.groupby('sat_id').cumcount()+1
        memory_based_df['memory_global_tw_index'] = memory_based_df['memory_global_tw_index'].astype(float)

        memory_based_df['concat_sat_mgwi'] = memory_based_df['sat_id'].astype('str') + '_' + memory_based_df['memory_global_tw_index'].astype(str)  #'memory_global_tw'
        TW__csn = dict(zip(memory_based_df['concat_sat_mgwi'],memory_based_df['global_tw']))

        #================================================================================================================================================================================================
        memory_based_df_only_gs = memory_based_df[memory_based_df['gs_id'] != 'no_i_no_g']
        memory_based_df_only_gs = memory_based_df_only_gs[~memory_based_df_only_gs['gs_id'].isnull()]
        memory_based_df_only_gs['concat_sat_gs_twi'] = memory_based_df_only_gs['sat_id'].astype('str') + '_' + memory_based_df_only_gs['gs_id'].astype('str') + '_' +  memory_based_df_only_gs['tw_index'].astype(str)

        global_twI__concat_sat_gs_twi = dict(zip(memory_based_df_only_gs['concat_sat_gs_twi'],memory_based_df_only_gs["memory_global_tw_index"]))

        conflict_strip_gs_df = memory_based_df[memory_based_df["flag_gs_pass_conflict"] == 1]
        conflict_strip_gs_df['conflict_strip_gs_concat_list'] =  conflict_strip_gs_df[['sat_id','gs_pass_conflict_list']].apply(lambda a : [a['sat_id'] + '_' + str(k[0]) +'_'+ str(k[1]) for k in a['gs_pass_conflict_list']],axis = 1  )
       
        conflict_strip_gs_df['conflict_strip_gs_mgtwi_list'] =  conflict_strip_gs_df[['conflict_strip_gs_concat_list']].apply(lambda a : [global_twI__concat_sat_gs_twi[k_] for k_ in a['conflict_strip_gs_concat_list']],axis = 1 )
        
        conflict_strip_gs_df['gs_list'] = conflict_strip_gs_df[['gs_pass_conflict_list']].apply(lambda a : [k[0] for k in a['gs_pass_conflict_list']] , axis = 1)
        conflict_strip_gs_df = conflict_strip_gs_df.explode(["gs_list","conflict_strip_gs_mgtwi_list"]).reset_index(drop=True)
        conflict_strip_gs_df['list_foramt_memory'] = conflict_strip_gs_df[["sat_id","encoded_strip_id","start_time","end_time","tw_index","gs_list","conflict_strip_gs_mgtwi_list"]].\
                                                       apply(lambda a : [a['start_time'],a['end_time'],a['sat_id'],a['conflict_strip_gs_mgtwi_list'],a['gs_list'],a['encoded_strip_id'],a["tw_index"]],axis=1)

        both_img_gs_list = list(conflict_strip_gs_df['list_foramt_memory'])

        #================================================================================================================================================================================================
        All_imgGS__group_union_withouEclipse_df = memory_based_df.groupby(['sat_id']).agg(global_tw_index_list = ('memory_global_tw_index',list)).reset_index()
        MemoryglobalTWindexSortedList__s = dict(zip(All_imgGS__group_union_withouEclipse_df['sat_id'],All_imgGS__group_union_withouEclipse_df['global_tw_index_list']))

        #PREV TIME WINDOW INDEX DICTIONARY
        #creating dict for each sat_globalTWIndex as key with value list of globalTWIndex that has happended before sat_globalTWIndex.
        prev_tWList__s_TWI_dict__s = {}
        prev_ImagingTWList__s_TWI_dict__s = {}
        memory_based_df.sort_values("start_time",inplace=True) # not necessary by satellite as we are filtering satellite 
        for s in memory_based_df['sat_id'].unique():
            to_get_prev_index_df =memory_based_df[memory_based_df['sat_id']==s]
            #for global s_TWI
            prev_tWList__s_TWI_dict__s[s] = get_prev_TW_index(to_get_prev_index_df)

            # Separate for Imaging
            to_get_Imagingprev_index_df = to_get_prev_index_df[to_get_prev_index_df['encoded_strip_id'] != 'no_i_no_g']
            to_get_Imagingprev_index_df = to_get_Imagingprev_index_df[~to_get_Imagingprev_index_df['encoded_strip_id'].isnull()]
            prev_ImagingTWList__s_TWI_dict__s[s] = get_prev_TW_index(to_get_Imagingprev_index_df)
    
        #======= #======= #======= #======= #======= #======= #======= #======= #======= #======= #======= #======= #======= #======= 
        #======= #======= #======= #======= #======= #======= #======= #======= #======= #======= #======= #======= #======= #======= 

        #Creating memory format based(without eclipse sunlit bifurcation) list for cases-> noImgNoGs,onlyImg,onlyGsPass
        
        #1 = noImgNoGs = [st,et,s,n]
        NoImgGSWithoutEclipse_df = memory_based_df[memory_based_df['gs_id']=='no_i_no_g']
        NoImgGSWithoutEclipse_df['list_foramt_memory'] = NoImgGSWithoutEclipse_df[['sat_id','start_time','end_time','memory_global_tw_index']].\
            apply(lambda a : [a['start_time'],a['end_time'],a['sat_id'],a['memory_global_tw_index']],axis=1)
        Memory_NoimageGs_TW_list = list(NoImgGSWithoutEclipse_df['list_foramt_memory'])

        #2 = onlyImg = [st,et,s,n,j,k]
        onlyimgGs_unionWithout_eclipse_df = memory_based_df[memory_based_df['gs_id']!='no_i_no_g']
        Final_Img_union_without_eclipse_df = onlyimgGs_unionWithout_eclipse_df[~onlyimgGs_unionWithout_eclipse_df['encoded_strip_id'].isnull()]
        Final_Img_union_without_eclipse_df['list_foramt_memory'] = Final_Img_union_without_eclipse_df[['sat_id','start_time','end_time','memory_global_tw_index','encoded_strip_id','tw_index']].\
            apply(lambda a : [a['start_time'],a['end_time'],a['sat_id'],a['memory_global_tw_index'],a['encoded_strip_id'],a['tw_index']],axis=1)
        Memory_onlyImgTW_list = list(Final_Img_union_without_eclipse_df['list_foramt_memory'])
        #3 = onlygs =[st,et,s,n,g]
        Final_onlygs_union_without_eclipse_df = onlyimgGs_unionWithout_eclipse_df[~onlyimgGs_unionWithout_eclipse_df['gs_id'].isnull()]
        Final_onlygs_union_without_eclipse_df['list_foramt_memory'] = Final_onlygs_union_without_eclipse_df[['sat_id','start_time','end_time','memory_global_tw_index','gs_id']].\
            apply(lambda a : [a['start_time'],a['end_time'],a['sat_id'],a['memory_global_tw_index'],a['gs_id']],axis=1)
        Memory_onlyGsTW_list = list(Final_onlygs_union_without_eclipse_df['list_foramt_memory'])
        
        #======= #======= #======= #======= #======= #======= #======= #======= #======= #======= #======= #======= #======= #======= 
        #======= #======= #======= #======= #======= #======= #======= #======= #======= #======= #======= #======= #======= #======= 

        #adding enclipse and sunlit based window bifurcation assuming only either one of eclipse/sunlit will happen during gs pass .
        memory_based_copy_df = memory_based_df.copy() 
        
        memory_based_copy_df['ec_st_end_list'] = memory_based_copy_df[['sat_id','start_time','end_time']].\
                                          apply( lambda a : \
                                          get_EcStEnd_list(a['start_time'],a['end_time'],df = self.eclipse_df_dict[a['sat_id']] ),axis= 1 )
        #imgGS_union_df1['EcStEnd_list'] ==> [[st,et,eclipse],[st,et,eclipse]]
        memory_based_copy_df['len_ec_st_end_list'] = memory_based_copy_df['ec_st_end_list'].apply(lambda a : len(a))
        self.logger.info("len_before_eclipse_transition_divide="+str(len(memory_based_copy_df)))
        memory_based_copy_df = memory_based_copy_df.explode('ec_st_end_list')
        self.logger.info('len_after(after explode)_eclipse_transition_divide='+str(len(memory_based_copy_df)))

        memory_based_copy_df['new_eclipse'] = memory_based_copy_df['ec_st_end_list'].apply(lambda a : a[0])
        memory_based_copy_df['new_start_time'] = memory_based_copy_df['ec_st_end_list'].apply(lambda a : a[1])
        memory_based_copy_df['new_end_time'] = memory_based_copy_df['ec_st_end_list'].apply(lambda a : a[2])

        #Z1
        check_l1 = len(memory_based_copy_df)
        #print(memory_based_copy_df[memory_based_copy_df['new_start_time'] =='NA'])
        memory_based_copy_df = memory_based_copy_df[memory_based_copy_df['new_start_time']!='NA']
        check_l2 = len(memory_based_copy_df)
        if check_l1!=check_l2:
            self.logger.error("Before_len_Ambiguous_event_transition_divide="+str(len(check_l1)))
            self.logger.error("After_len_of_ambiguous_event_transition_divide="+str(len(check_l1)))
            self.logger.error("something is wrong in eclipse data or opprtunity start time or  end time")
        memory_based_copy_df.drop(['start_time','end_time','ec_st_end_list'],axis=1,inplace=True)
        memory_based_copy_df.rename(columns={'new_start_time':'start_time','new_end_time':'end_time','new_eclipse':'eclipse'},inplace=True)  
        memory_based_copy_df = memory_based_copy_df.drop(['till_now_max','prev_max'], axis=1)
        memory_based_copy_df = memory_based_copy_df.sort_values( by='start_time')
        memory_based_copy_df['power_global_tw'] = memory_based_copy_df[['start_time','end_time']].apply(list,axis=1)

        #========++++++++++=====================+++++++++=============================+++++++++=============================================================
        #power based or after eclipse divide , new TIME window index addition
        same_as_memory_global_tw_index_df = memory_based_copy_df[memory_based_copy_df['len_ec_st_end_list'] == 1]
        same_as_memory_global_tw_index_df['power_global_tw_index'] = same_as_memory_global_tw_index_df['memory_global_tw_index']
        different_from_memory_global_tw_index_df = memory_based_copy_df[memory_based_copy_df['len_ec_st_end_list'] > 1]
        different_from_memory_global_tw_index_df['power_global_tw_index'] = different_from_memory_global_tw_index_df.groupby('sat_id').cumcount()+1 # changed GTWI for those TW having both  sunlit and eclipse
        different_from_memory_global_tw_index_df['power_global_tw_index'] = different_from_memory_global_tw_index_df['power_global_tw_index'] + 0.5 # add decimal so that it does not match with any other GTW
        self.logger.info("len_power_same_as_memory_global_tw_index_df="+str(len(same_as_memory_global_tw_index_df)))
        self.logger.info("len_power_different_from_memory_global_tw_index_df="+str(len(different_from_memory_global_tw_index_df)))
        power_based_df = pd.concat([same_as_memory_global_tw_index_df,different_from_memory_global_tw_index_df])
        power_based_df.sort_values('start_time',inplace=True)

        # Dedicated TW for Readout 
        # eclipse    Common_r
        #  0           1
        #  0           1
        #  0           1
        #  1            2
        # 1             2
        #  1             2
        # 0             3
        # 0             3
        # 0             3
        dedicated_readout_df = get_readout_TW(power_based_df)
        
        dedicatedReadoutTWlist__concat_sat_memoryTWindex = dict(zip(dedicated_readout_df['concat_sat_memory_tw_index'],dedicated_readout_df['list_format']))
        DROPriority__concat_sat_memoryTWindex = dict(zip(dedicated_readout_df['concat_sat_memory_tw_index'],dedicated_readout_df['readout_priority']))

        dedicated_readout_grouped_df = dedicated_readout_df.groupby('sat_id').agg(TW_list = ('memory_global_tw_index',set)).reset_index()
        dedicatedReadoutTWIndex__sat = dict(zip(dedicated_readout_grouped_df['sat_id'],dedicated_readout_grouped_df['TW_list']))

        prev_dedicatedReadoutIndex__s_TWI_dict__s = {}
        dedicated_readout_df.sort_values(by='start_time',inplace = True)
        for s in dedicated_readout_df['sat_id'].unique():
            to_get_readout_prev_index_df =dedicated_readout_df[dedicated_readout_df['sat_id']==s]
            prev_dedicatedReadoutIndex__s_TWI_dict__s[s] = get_prev_TW_index(to_get_readout_prev_index_df,'memory_global_tw_index')

        #ADDING prev TW list for power based format TW table  
        prev_power_tWList__s_TWI_dict__s = {}
        for s in power_based_df['sat_id'].unique():
            to_get_power_prev_index_df =power_based_df[power_based_df['sat_id']==s]

            prev_power_tWList__s_TWI_dict__s[s] = get_prev_TW_index(to_get_power_prev_index_df,'power_global_tw_index')
    
        #ENtire dict of power based TW GTWI(global time window index) list
        imgGS_Grouped_union_df2 = power_based_df.groupby(['sat_id']).agg(global_tw_index_list = ('power_global_tw_index',list)).reset_index()
        PowerglobalTWindexSortedList__s = dict(zip(imgGS_Grouped_union_df2['sat_id'],imgGS_Grouped_union_df2['global_tw_index_list']))

        #power based TW list for cases : noImgNoGs,onlyImg,onlyGs
        FinalNoImgGS_union_df = power_based_df[power_based_df['gs_id'] == 'no_i_no_g']
        FinalNoImgGS_union_df['list_foramt_memory_power'] = FinalNoImgGS_union_df[['encoded_strip_id','sat_id','start_time','end_time','tw_index','eclipse','power_global_tw_index']].\
            apply(lambda a : [a['start_time'],a['end_time'],a['sat_id'],a['encoded_strip_id'],a['tw_index'],a['eclipse'],a['power_global_tw_index']],axis=1)
        Power_NoimageGs_TW_list = list(FinalNoImgGS_union_df['list_foramt_memory_power'])

        #1 = noImgNoGs = [st,et,s,j,k,eclipse,n]
        onlyimgGs_union_df = power_based_df[power_based_df['gs_id'] != 'no_i_no_g']
        Final_Img_union_df = onlyimgGs_union_df[~onlyimgGs_union_df['encoded_strip_id'].isnull()] # i<->g # i <-> !g
        Final_Img_union_df['list_foramt_memory_power'] = Final_Img_union_df[['encoded_strip_id','sat_id','start_time','end_time','tw_index','eclipse','power_global_tw_index']].\
            apply(lambda a : [a['start_time'],a['end_time'],a['sat_id'],a['encoded_strip_id'],a['tw_index'],a['eclipse'],a['power_global_tw_index']],axis=1)
        
        #2 = OnlyImg = [st,et,s,j,k,eclipse,n]
        Power_image_TW_list = list(Final_Img_union_df['list_foramt_memory_power'])
        Final_GS_union_df = onlyimgGs_union_df[~onlyimgGs_union_df['gs_id'].isnull()]
        Final_GS_union_df['list_foramt_memory_power'] = Final_GS_union_df[['gs_id','sat_id','start_time','end_time','tw_index','eclipse','power_global_tw_index']].\
            apply(lambda a : [a['start_time'],a['end_time'],a['sat_id'],a['gs_id'],a['tw_index'],a['eclipse'],a['power_global_tw_index']],axis=1)
        #3 = OnlyGS = [st,et,s,g,k,eclipse,n]
        Power_GS_TW_list = list(Final_GS_union_df['list_foramt_memory_power'])
        #=================#=================#=================#=================#=================#=================#=================#=================#=================#=================#=================#=================
        return {"PowerglobalTWindexSortedList__s":PowerglobalTWindexSortedList__s,\
                "MemoryglobalTWindexSortedList__s":MemoryglobalTWindexSortedList__s,\
                "Power_GS_TW_list":Power_GS_TW_list,\
                "Memory_onlyGsTW_list":Memory_onlyGsTW_list,\
                "Power_image_TW_list":Power_image_TW_list,\
                "Memory_onlyImgTW_list":Memory_onlyImgTW_list,\
                "Power_NoimageGs_TW_list":Power_NoimageGs_TW_list,\
                "Memory_NoimageGs_TW_list":Memory_NoimageGs_TW_list,\
                "prev_tWList__s_TWI_dict__s":prev_tWList__s_TWI_dict__s,\
                "prev_ImagingTWList__s_TWI_dict__s":prev_ImagingTWList__s_TWI_dict__s,\
                "prev_power_tWList__s_TWI_dict__s":prev_power_tWList__s_TWI_dict__s,\
                "TW_df_withoutEclipseDivide_df":memory_based_df,\
                "TW_df_withEclipseDivide_df":power_based_df,\
                "dedicatedReadoutTWlist__concat_sat_memoryTWindex":dedicatedReadoutTWlist__concat_sat_memoryTWindex,\
                "power_based_df":power_based_df,\
                "success_metric_before":self.success_metric_before,\
                "TW__csn":TW__csn,\
                "memory_based_df":memory_based_df,\
                "dedicated_readout_df":dedicated_readout_df,\
                "dedicatedReadoutTWIndex__sat":dedicatedReadoutTWIndex__sat,\
                "prev_dedicatedReadoutIndex__s_TWI_dict__s":prev_dedicatedReadoutIndex__s_TWI_dict__s,\
                "DROPriority__concat_sat_memoryTWindex":DROPriority__concat_sat_memoryTWindex,\
                "both_img_gs_list":both_img_gs_list}
    
    def create_latest_dict(self):
        # original_gs_pass_df
        self.gsPass_df['sat_id'] = self.gsPass_df['sat_id'].astype(str)
        self.gsPass_df['gs_id'] = self.gsPass_df['gs_id'].astype(str)
        self.image_opportunity_df['sat_id'] = self.image_opportunity_df['sat_id'].astype(str)
        self.image_opportunity_df['strip_id'] = self.image_opportunity_df['strip_id'].astype(str)
        self.image_opportunity_df['aoi_id'] = self.image_opportunity_df['aoi_id'].astype(str)
        self.image_opportunity_df['encoded_strip_id'] =   self.image_opportunity_df['strip_id'] + '_' + self.image_opportunity_df['aoi_id']
        original_gs_pass_df = self.gsPass_df
        # conflict_gs_img_pass
        self.gsPass_df = self.gsPass_df[['sat_id','gs_id','start_time','end_time']].drop_duplicates()
        self.gsPass_df['tw_gs'] = self.gsPass_df[['start_time','end_time']].apply(list,axis=1)
        self.gsPass_df['tw_index'] = self.gsPass_df.groupby(['gs_id','sat_id'])['tw_gs'].rank(method='dense')

        original_gs_pass_grouped_df = self.gsPass_df.groupby('sat_id').agg(tw_list_gspass = ('tw_gs',list),\
                                                             gs_list = ('gs_id',list),\
                                                             tw_index_gs_list = ('tw_index',list)).reset_index()
        
        self.image_opportunity_df['TW'] = self.image_opportunity_df[['opportunity_start_offset','opportunity_end_offset']].apply(list,axis=1)
        
        
        self.image_opportunity_df = pd.merge(self.image_opportunity_df,original_gs_pass_grouped_df,on='sat_id',how='left')
        self.image_opportunity_df[['tw_list_gspass', 'gs_list']] = self.image_opportunity_df[['tw_list_gspass','gs_list']].fillna(value='NA')
        
        self.image_opportunity_df['gs_pass_conflict_list'] = self.image_opportunity_df[['TW','tw_list_gspass','gs_list','tw_index_gs_list']]\
        .apply( lambda a : check_opportunities_conflict_GSpass(a['TW'],a['tw_list_gspass'],a['gs_list'],a['tw_index_gs_list'],setup_time=self.setup_time), axis = 1)

        self.image_opportunity_df['flag_gs_pass_conflict'] = self.image_opportunity_df[['TW','tw_list_gspass']].apply( lambda a : remove_opportunities_conflict_GSpass(a['TW'],a['tw_list_gspass'],setup_time=self.setup_time), axis = 1)
       
        conflict_image_opportunity_df = self.image_opportunity_df[self.image_opportunity_df['flag_gs_pass_conflict'] == 1] #TODO1
        self.success_metric_before = {'conflict_images':list(conflict_image_opportunity_df['encoded_strip_id'].unique()) }

        self.image_opportunity_df['tw_index'] = self.image_opportunity_df.groupby(['encoded_strip_id','sat_id'])['TW'].rank(method='dense')
        self.image_opportunity_df['concat_sat_id_encoded_strip_id_tw_index'] = self.image_opportunity_df['sat_id'] +'_' + \
                                                                           self.image_opportunity_df['encoded_strip_id'] +\
                                                                           '_' + self.image_opportunity_df['tw_index'].astype(str)
        
        
        imgery_sat_id_list = list(self.image_opportunity_df['sat_id'].unique())
        only_gs_sat_id_list = list(original_gs_pass_df[~original_gs_pass_df['sat_id'].isin(imgery_sat_id_list)]['sat_id'].unique())
        only_gs_list = list(original_gs_pass_df["gs_id"].unique())
        encoded_strip_id_list = self.image_opportunity_df['encoded_strip_id'].unique()
        unique_img_opportunities_list = list(self.image_opportunity_df['concat_sat_id_encoded_strip_id_tw_index'].unique())

        image_opportunity_grouped_df = self.image_opportunity_df.groupby('encoded_strip_id').agg(opportunity_list = ('concat_sat_id_encoded_strip_id_tw_index',list)).reset_index()
        self.image_opportunity_grouped_df1 = self.image_opportunity_df.groupby('sat_id').agg(opportunity_list = ('concat_sat_id_encoded_strip_id_tw_index',set)).reset_index()

        cs1j2k2Domainlist__cs1j1k1 = {}
        #sj1k1j2k2_dict['domain_of_csjk'] ={}
        cs1j2k2Domainlist__cs1j1k1 = get_conflicting_dict(self.image_opportunity_df,cs1j2k2Domainlist__cs1j1k1,self.setup_time,'sat_id',\
                                                          'concat_sat_id_encoded_strip_id_tw_index','opportunity_end_offset',\
                                                            'opportunity_start_offset')
    

        csjkSet__s = dict(zip(self.image_opportunity_grouped_df1['sat_id'],self.image_opportunity_grouped_df1['opportunity_list']))

        csjkList__j = dict(zip(image_opportunity_grouped_df['encoded_strip_id'],\
                                                        image_opportunity_grouped_df['opportunity_list']))
       
        TW__csjk = dict(zip(self.image_opportunity_df['concat_sat_id_encoded_strip_id_tw_index'],\
                                                  self.image_opportunity_df['TW']))
        
        stripid__encodedstripID = dict(zip(self.image_opportunity_df['encoded_strip_id'],\
                                                        self.image_opportunity_df['strip_id']))
        
        AOIid__encodedstripID = dict(zip(self.image_opportunity_df['encoded_strip_id'],\
                                                        self.image_opportunity_df['aoi_id']))
        
        
        camera_memory_capacity__s = {s: self.system_requirment_input_dict["memory_data_"][s,"NCCM"][1] for s in imgery_sat_id_list+only_gs_sat_id_list}
        initial_camera_memory_value__s = {s: self.system_requirment_input_dict["memory_data_"][s,"NCCM"][0] for s in imgery_sat_id_list+only_gs_sat_id_list}

        readout_memory_capacity__s = {s: self.system_requirment_input_dict["memory_data_"][s,"SSD"][1] for s in imgery_sat_id_list+only_gs_sat_id_list}
        initial_readout_camera_memory_value__s = {s: self.system_requirment_input_dict["memory_data_"][s,"SSD"][0] for s in imgery_sat_id_list+only_gs_sat_id_list}

        power_capacity__s = {s: self.system_requirment_input_dict["power_data_"][s][1] for s in imgery_sat_id_list+only_gs_sat_id_list}
        initial_power_value__s = {s: self.system_requirment_input_dict["power_data_"][s][1] for s in imgery_sat_id_list+only_gs_sat_id_list}
        min_power_value__s = {s: self.system_requirment_input_dict["power_data_"][s][2] for s in imgery_sat_id_list+only_gs_sat_id_list}
        power_transfer__s_operation = self.system_requirment_input_dict["power_transfer_"]


        camera_thermal_capacity__s = {s: self.system_requirment_input_dict["thermal_data_"][(s,"camera_detector")][1] for s in imgery_sat_id_list+only_gs_sat_id_list}
        camera_initial_thermal_value__s = {s: self.system_requirment_input_dict["thermal_data_"][(s,"camera_detector")][0] for s in imgery_sat_id_list+only_gs_sat_id_list}

        readout_thermal_capacity__s = {s: self.system_requirment_input_dict["thermal_data_"][(s,"NCCM")][1] for s in imgery_sat_id_list+only_gs_sat_id_list}
        readout_initial_thermal_value__s = {s: self.system_requirment_input_dict["thermal_data_"][(s,"NCCM")][0] for s in imgery_sat_id_list+only_gs_sat_id_list}

        imaging_rate__s =   {s: self.system_requirment_input_dict["mem_transfer_SatLvlData_"][s][0] for s in imgery_sat_id_list+only_gs_sat_id_list}#7 #gbps
        downlinking_rate__sg = {(s,g): self.system_requirment_input_dict["mem_transfer_gsLvlData_"][(s,g)][0] for s in imgery_sat_id_list+only_gs_sat_id_list for g in only_gs_list}
        Readout_rate__s = {s: self.system_requirment_input_dict["mem_transfer_SatLvlData_"][s][1] for s in imgery_sat_id_list+only_gs_sat_id_list}


        if self.config['constraints']['thermal_constraint_imaging']:
            self.logger.info("=====IMAGING THEMAL preprocess======")
      
            with ThreadPoolExecutor() as executor:

                
                heatCameraTimeBucket_SCT_dict__s  = {s:executor.submit(get_thermal_bucket, camera_initial_thermal_value__s[s], \
                                                self.system_requirment_input_dict["thermal_data_"][(s,"camera_detector")][2] ,\
                                                self.system_requirment_input_dict["thermal_data_"][(s,"camera_detector")][3],
                                                camera_thermal_capacity__s[s],\
                                                self.system_requirment_input_dict["thermal_data_"][(s,"camera_detector")][4],\
                                                self.system_requirment_input_dict["thermal_data_"][(s,"camera_detector")][5],\
                                                self.system_requirment_input_dict["thermal_data_"][(s,"camera_detector")][6],\
                                                self.system_requirment_input_dict["thermal_data_"][(s,"camera_detector")][7],\
                                                self.system_requirment_input_dict["thermal_data_"][(s,"camera_detector")][8],\
                                                operation = "Imaging").result() for s in imgery_sat_id_list+only_gs_sat_id_list}
                
           
            max_camera_heat_dict = {s : heatCameraTimeBucket_SCT_dict__s[s]['max_time_heat'] for s in imgery_sat_id_list+only_gs_sat_id_list }
            self.logger.info("max_camera_heat_dict: %s", json.dumps(max_camera_heat_dict))

            for s in imgery_sat_id_list+only_gs_sat_id_list :
                del heatCameraTimeBucket_SCT_dict__s[s]['max_time_heat']
           
        else:
            heatCameraTimeBucket_SCT_dict__s = {}
            max_camera_heat_dict = {}

        if self.config['constraints']['thermal_constraint_readout']:
            self.logger.info("=====READOUT THERMAL preprocess======")

            with ThreadPoolExecutor() as executor:
                
                heatTimeBucket_SCT_dict__s  = {s:executor.submit(get_thermal_bucket, readout_initial_thermal_value__s[s], \
                                                self.system_requirment_input_dict["thermal_data_"][(s,"NCCM")][2] ,\
                                                self.system_requirment_input_dict["thermal_data_"][(s,"NCCM")][3],
                                                readout_thermal_capacity__s[s],\
                                                self.system_requirment_input_dict["thermal_data_"][(s,"NCCM")][4],\
                                                self.system_requirment_input_dict["thermal_data_"][(s,"NCCM")][5],\
                                                self.system_requirment_input_dict["thermal_data_"][(s,"NCCM")][6],\
                                                self.system_requirment_input_dict["thermal_data_"][(s,"NCCM")][7],\
                                                self.system_requirment_input_dict["thermal_data_"][(s,"NCCM")][8],\
                                                operation = "Readout").result() for s in imgery_sat_id_list+only_gs_sat_id_list}
                
          
            max_readout_heat_dict = {s : heatTimeBucket_SCT_dict__s[s]['max_time_heat'] for s in imgery_sat_id_list+only_gs_sat_id_list }
            self.logger.info("max_readout_heat_dict: %s", json.dumps(max_readout_heat_dict))
            for s in imgery_sat_id_list+only_gs_sat_id_list :
                del heatTimeBucket_SCT_dict__s[s]['max_time_heat']
        else:
            heatTimeBucket_SCT_dict__s ={}
            max_readout_heat_dict = {}
        return {"encoded_stripId_list":encoded_strip_id_list,\
                "imgery_sat_id_list":imgery_sat_id_list,\
                "only_gs_sat_id_list":only_gs_sat_id_list,\
                "unique_img_opportunities_list":unique_img_opportunities_list,\
                "TW__csjk":TW__csjk,\
                "csjkList__j":csjkList__j,\
                "csjkSet__s":csjkSet__s,\
                "cs1j2k2Domainlist__cs1j1k1":cs1j2k2Domainlist__cs1j1k1,\
                "camera_memory_capacity__s":camera_memory_capacity__s,\
                "initial_camera_memory_value__s":initial_camera_memory_value__s,\
                "readout_memory_capacity__s":readout_memory_capacity__s,\
                "power_capacity__s":power_capacity__s,\
                "min_power_value__s":min_power_value__s,\
                "initial_power_value__s":initial_power_value__s,\
                "initial_readout_camera_memory_value__s":initial_readout_camera_memory_value__s,\
                "heatCameraTimeBucket_SCT_dict__s":heatCameraTimeBucket_SCT_dict__s,\
                "heatTimeBucket_SCT_dict__s":heatTimeBucket_SCT_dict__s,\
                "max_camera_heat_dict":max_camera_heat_dict,\
                "max_readout_heat_dict":max_readout_heat_dict,\
                "stripid__encodedstripID":stripid__encodedstripID,\
                "AOIid__encodedstripID":AOIid__encodedstripID,\
                "imaging_rate__s":imaging_rate__s,\
                "downlinking_rate__sg ":downlinking_rate__sg,\
                "Readout_rate__s":Readout_rate__s,\
                "power_transfer__s_operation":power_transfer__s_operation,\
              
                 }


    def preprocess(self):
        model_input_data_dict = self.create_latest_dict()

        priority_dict = self.get_priorities()
        model_input_data_dict.update(priority_dict)
        #model_input_data_dict['active_assured_strip_id_list'] = []#assured_strip_id_list

        MTW_PTW_dict = self.create_MTW_PTW()
        model_input_data_dict.update(MTW_PTW_dict)

        return model_input_data_dict
