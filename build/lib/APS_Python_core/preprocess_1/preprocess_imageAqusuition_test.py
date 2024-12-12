import pandas as pd
import datetime
import math
from APS_Python_core.utils import get_flag_for_image,remove_opportunities_conflict_GSpass,get_EcStEnd_list,get_prev_TW_index,get_thermal_bucket,get_readout_TW,get_active_assured_task,get_conflicting_dict

class ImageAquisitionProcess:
    def __init__(self, image_opportunity_df,Gs_pass_output_df,eclipse_df_dict,config ):
        '''
        image_opportunity_df : 'SatID','StripID','AoiID','OpportunityStartTime','OpportunityEndTime','OpportunityStartOffset','OpportunityEndOffset','OrderValidityEnd','CloudCover','OffNadir','assured_task','Eclipse'
        Gs_pass_output_df : 'SatID','gsID','start_time','end_time','Eclipse'
        '''
        image_opportunity_df
        self.image_opportunity_df = image_opportunity_df
        self.gsPass_df = Gs_pass_output_df
        self.setup_time =  120 
        self.eclipse_df_dict = eclipse_df_dict
        self.config = config
        self.base_time_stamp = config['base_time_stamp']
        self.data = {}

    def get_priorities(self):
  
        self.image_opportunity_df['X'] = self.image_opportunity_df[['OpportunityStartTime','OpportunityStartOffset']].apply(lambda a: pd.to_datetime(a['OpportunityStartTime']) - pd.DateOffset(seconds=a['OpportunityStartOffset']),axis=1)
        self.image_opportunity_df['Y'] = self.image_opportunity_df[['OpportunityEndTime','OpportunityEndOffset']].apply(lambda a: pd.to_datetime(a['OpportunityEndTime']) - pd.DateOffset(seconds=a['OpportunityEndOffset']),axis=1)
        #base_time_stamp = self.image_opportunity_df["X"].mean()
        base_time_stamp = self.image_opportunity_df["X"].to_list()[0]
        #base_time_stamp = base_time_stamp #self.base_time_stamp
        self.image_opportunity_df['base_timestamp'] = base_time_stamp
        self.image_opportunity_df['due_date_end_offset'] = pd.to_datetime(self.image_opportunity_df['OrderValidityEnd']) - pd.to_datetime(self.image_opportunity_df['base_timestamp'])#self.image_opportunity_df['base_timestamp']
        self.image_opportunity_df['due_seconds_diff'] = self.image_opportunity_df[['due_date_end_offset']].apply(lambda a : a['due_date_end_offset'].total_seconds(),axis=1)
        #==============================================================================================================
        self.image_opportunity_df['local_priority_due_date'] = self.image_opportunity_df['due_seconds_diff'].apply(lambda a : abs(1/(a+0.0001)) )# 48 becuause if due date is less than 2 days from now it will exponentially increase for that denominator should be less than 1 .
        self.image_opportunity_df['local_priority_CC_based'] = abs(1 / (self.image_opportunity_df['CloudCover']+0.0001)/(10  ))
        self.image_opportunity_df['local_priority_offNadir'] = abs(1 / (self.image_opportunity_df['OffNadir']+0.0001)/(8  ))

        #==============================================================================================================
        self.image_opportunity_df['normalized_local_priority_due_date'] = (self.image_opportunity_df['local_priority_due_date'] / self.image_opportunity_df['local_priority_due_date'].max())*1000#/ (self.image_opportunity_df['local_priority_due_date'].max() - self.image_opportunity_df['local_priority_due_date'].min())
        self.image_opportunity_df['normalized_local_priority_CC_based'] = (self.image_opportunity_df['local_priority_CC_based'] / self.image_opportunity_df['local_priority_CC_based'].max())*1000#/ (self.image_opportunity_df['local_priority_CC_based'].max() - self.image_opportunity_df['local_priority_CC_based'].min())
        self.image_opportunity_df['normalized_local_priority_offNadir'] = (self.image_opportunity_df['local_priority_offNadir'] / self.image_opportunity_df['local_priority_offNadir'].max())*1000#/ (self.image_opportunity_df['local_priority_offNadir'].max() - self.image_opportunity_df['local_priority_offNadir'].min())
        max_GP_= self.image_opportunity_df[self.image_opportunity_df['Priority']<1000]['Priority'].max()
        self.image_opportunity_df['normalized_GlobalPriority'] = (self.image_opportunity_df['Priority'] / max_GP_)*1000#/ (self.image_opportunity_df['Priority'].max() - self.image_opportunity_df['Priority'].min()-0.00001)
        self.image_opportunity_df['normalized_GlobalPriority'] = self.image_opportunity_df[['normalized_GlobalPriority','Priority']].apply(lambda a: a['Priority']*1000 if  a['Priority'] >=1000 else a['normalized_GlobalPriority'],axis = 1)
        self.image_opportunity_df['normalized_local_priority_due_date'] = self.image_opportunity_df[['normalized_local_priority_due_date','due_seconds_diff']].apply(lambda a: abs(1/(a['due_seconds_diff']+0.001))*24*60*1000*1000 if  a['due_seconds_diff'] <= 24*60 else a['normalized_local_priority_due_date'],axis = 1)
        #==============================================================================================================
       
        self.image_opportunity_df['normalized_Total_Priority'] =  self.config['GP_weight']*self.image_opportunity_df['normalized_GlobalPriority'] + \
                                                       self.config['DDLP_weight']*self.image_opportunity_df['normalized_local_priority_due_date']+\
                                                       self.config['CCLP_weight']*self.image_opportunity_df['normalized_local_priority_CC_based'] +\
                                                       self.config['ONLP_weight']*self.image_opportunity_df['normalized_local_priority_offNadir']
        
        #self.image_opportunity_df['normalized_Total_Priority'] = self.image_opportunity_df['normalized_Total_Priority'].astype(int)
        GlobalPriority__csjk = dict(zip(self.image_opportunity_df['concat_SatID_encodedStripId_TWindex'],self.image_opportunity_df['normalized_GlobalPriority']))
        TotalPriority__csjk = dict(zip(self.image_opportunity_df['concat_SatID_encodedStripId_TWindex'],self.image_opportunity_df['normalized_Total_Priority']))
        
        self.image_opportunity_df['normalized_Local_Priority'] = self.image_opportunity_df['normalized_local_priority_due_date'] + self.image_opportunity_df['normalized_local_priority_CC_based'] +  self.image_opportunity_df['normalized_local_priority_offNadir'] 
        Local_Priority__csjk = dict(zip(self.image_opportunity_df['concat_SatID_encodedStripId_TWindex'],self.image_opportunity_df['normalized_Local_Priority']))

        original_stripLevel_df = self.image_opportunity_df[['encoded_stripId','normalized_Total_Priority','normalized_Local_Priority','normalized_GlobalPriority']]#.drop_duplicates()

        original_strip_grouped_Level_df = original_stripLevel_df.groupby('encoded_stripId').agg(mean_GP=('normalized_GlobalPriority',pd.Series.mean),\
                                                                                       mean_LP=('normalized_Local_Priority',pd.Series.mean),\
                                                                                       mean_TP=('normalized_Total_Priority',pd.Series.mean)).reset_index()
        
        TotalPriority__csj = dict(zip(original_strip_grouped_Level_df['encoded_stripId'],original_strip_grouped_Level_df['mean_TP']))
        GlobalPriority__csj = dict(zip(original_strip_grouped_Level_df['encoded_stripId'],original_strip_grouped_Level_df['mean_GP']))
        LocalPriority__csj = dict(zip(original_strip_grouped_Level_df['encoded_stripId'],original_strip_grouped_Level_df['mean_LP']))



        self.success_metric_before.update({'original_Total_GP':original_strip_grouped_Level_df['mean_GP'].sum(),\
           'original_Total_LP':original_strip_grouped_Level_df['mean_LP'].sum(),\
           'original_Total_TP':original_strip_grouped_Level_df['mean_TP'].sum(),\
           'total_opportunities':original_strip_grouped_Level_df['encoded_stripId'].nunique() })
    

        assured_tasking_duedatebased_df  = self.image_opportunity_df[self.image_opportunity_df['due_seconds_diff'] <= 24*3600 ]

        assured_tasking_duedatebased_df.sort_values(by = 'due_seconds_diff', ascending = True, inplace= True)
        assured_tasking_duedatebased_df.drop_duplicates(subset =['encoded_stripId'],keep = 'last',inplace=True)
        assured_tasking_basedOnDueDateEmergency_list = list(assured_tasking_duedatebased_df['encoded_stripId'])#[0:3]

        assured_tasking_based_on_input_df  = self.image_opportunity_df[self.image_opportunity_df['normalized_GlobalPriority'] >1000 ]
        assured_tasking_duedatebased_df.drop_duplicates(subset =['encoded_stripId'],inplace=True)
        assured_tasking_based_on_input_list = list(assured_tasking_based_on_input_df['encoded_stripId'])


        return {
                "assured_tasking_basedOnDueDateEmergency_list":assured_tasking_basedOnDueDateEmergency_list,\
                "assured_tasking_based_on_input_list":assured_tasking_based_on_input_list,\
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
        selected_image_opportunity_df = self.image_opportunity_df[['SatID','encoded_stripId','OpportunityStartOffset','OpportunityEndOffset','Eclipse','TW_index']]
        gsPass_df = self.gsPass_df[['SatID','gsID','start_time','end_time','Eclipse','TW_index']]
        selected_image_opportunity_df.rename(columns = {'OpportunityStartOffset':'start_time','OpportunityEndOffset':'end_time'},inplace=True)
        imgGS_union_df = pd.concat([selected_image_opportunity_df,gsPass_df],join='outer')
        
        #logic for adding TW where no gs pass and imging can happen at all
        imgGS_union_df.sort_values(by='start_time',inplace=True)
        imgGS_union_df['till_now_max'] = imgGS_union_df.groupby('SatID')['end_time'].cummax()
        imgGS_union_df['prev_max'] = imgGS_union_df.groupby('SatID')['till_now_max'].shift(1)

        imgGS_union_noOperation_df = imgGS_union_df[imgGS_union_df['start_time'] > imgGS_union_df['prev_max'] + 1] 
        imgGS_union_noOperation_df['start_time1'] = imgGS_union_noOperation_df['prev_max'] + 1 #TODO1 +1 is okay ?
        imgGS_union_noOperation_df['end_time1'] = imgGS_union_noOperation_df['start_time'] - 1
        imgGS_union_noOperation_df['encoded_stripId'] = 'no_i_no_g'
        imgGS_union_noOperation_df['gsID'] = 'no_i_no_g'

        #adding First TW where no imaging and gs pass can happen at all

        '''
        First_TW_df = imgGS_union_df.groupby('SatID').agg(max_end_time =('start_time','min')).reset_index()
        First_TW_df['max_end_time'] = First_TW_df['max_end_time'] -  1
        First_TW_df['min_start_time'] = 0

        First_TW_df1 = pd.DataFrame({'SatID':list(First_TW_df['SatID']),\
                            'end_time1': list(last_TW_df['max_end_time']),\
                            })

        First_TW_df1['start_time1'] = 0
        First_TW_df1['encoded_stripId'] = 'no_i_no_g'
        '''

        #adding last TW where no imaging and gs pass can happen at all 
        '''
        # last_TW_df = imgGS_union_df.groupby('SatID').agg(min_start_time =('end_time','max')).reset_index()
       
        # last_TW_df['min_start_time'] = last_TW_df['min_start_time'] +  1
        # last_TW_df1 = pd.DataFrame({'SatID':list(last_TW_df['SatID']),\
        #                     'start_time1': list(last_TW_df['min_start_time']),\
        #                     })

        # last_TW_df1['end_time1'] = self.config['scheduled_Hrs']*3600 - 1 #40
        # last_TW_df1['encoded_stripId'] = 'no_i_no_g'
        # last_TW_df1['gsID'] = 'no_i_no_g'
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

        #Adding global TW index assuming all TW will be different for each opportunity of satellite. No Eclipse and sunlit bifurcation
        memory_based_df['global_TW'] = memory_based_df[['start_time','end_time']].apply(list,axis=1)
        
        #Assuming different TW for each satellite 
        memory_based_df['Memory_global_TW_index'] = memory_based_df.groupby('SatID').cumcount()+1
        memory_based_df['Memory_global_TW_index'] = memory_based_df['Memory_global_TW_index'].astype(float)

        memory_based_df['concat_sat_MGWI'] = memory_based_df['SatID'].astype('str') + '_' + memory_based_df['Memory_global_TW_index'].astype(str)  #'memory_global_TW'
        TW__csn = dict(zip(memory_based_df['concat_sat_MGWI'],memory_based_df['global_TW']))

        All_imgGS__group_union_withouEclipse_df = memory_based_df.groupby(['SatID']).agg(global_TWindex_list = ('Memory_global_TW_index',list)).reset_index()
        MemoryglobalTWindexSortedList__s = dict(zip(All_imgGS__group_union_withouEclipse_df['SatID'],All_imgGS__group_union_withouEclipse_df['global_TWindex_list']))

        #PREV TIME WINDOW INDEX DICTIONARY
        #creating dict for each sat_globalTWIndex as key with value list of globalTWIndex that has happended before sat_globalTWIndex.
        prev_tWList__s_TWI_dict__s = {}
        prev_ImagingTWList__s_TWI_dict__s = {}
        memory_based_df.sort_values("start_time",inplace=True) # not necessary by satellite as we are filtering satellite 
        for s in memory_based_df['SatID'].unique():
            to_get_prev_index_df =memory_based_df[memory_based_df['SatID']==s]
            #for global s_TWI
            prev_tWList__s_TWI_dict__s[s] = get_prev_TW_index(to_get_prev_index_df)

            # Separate for Imaging
            to_get_Imagingprev_index_df = to_get_prev_index_df[to_get_prev_index_df['encoded_stripId'] != 'no_i_no_g']
            to_get_Imagingprev_index_df = to_get_Imagingprev_index_df[~to_get_Imagingprev_index_df['encoded_stripId'].isnull()]
            prev_ImagingTWList__s_TWI_dict__s[s] = get_prev_TW_index(to_get_Imagingprev_index_df)
    
        #======= #======= #======= #======= #======= #======= #======= #======= #======= #======= #======= #======= #======= #======= 
        #======= #======= #======= #======= #======= #======= #======= #======= #======= #======= #======= #======= #======= #======= 

        #Creating memory format based(without eclipse sunlit bifurcation) list for cases-> noImgNoGs,onlyImg,onlyGsPass
        
        #1 = noImgNoGs = [st,et,s,n]
        NoImgGSWithoutEclipse_df = memory_based_df[memory_based_df['gsID']=='no_i_no_g']
        NoImgGSWithoutEclipse_df['list_foramt_memory'] = NoImgGSWithoutEclipse_df[['SatID','start_time','end_time','Memory_global_TW_index']].\
            apply(lambda a : [a['start_time'],a['end_time'],a['SatID'],a['Memory_global_TW_index']],axis=1)
        Memory_NoimageGs_TW_list = list(NoImgGSWithoutEclipse_df['list_foramt_memory'])

        #2 = onlyImg = [st,et,s,n,j,k]
        #onlyimgGs_union_df = imgGS_union_df2[imgGS_union_df2['gsID'] != 'no_i_no_g']
        onlyimgGs_unionWithout_eclipse_df = memory_based_df[memory_based_df['gsID']!='no_i_no_g']
        Final_Img_union_without_eclipse_df = onlyimgGs_unionWithout_eclipse_df[~onlyimgGs_unionWithout_eclipse_df['encoded_stripId'].isnull()]
        Final_Img_union_without_eclipse_df['list_foramt_memory'] = Final_Img_union_without_eclipse_df[['SatID','start_time','end_time','Memory_global_TW_index','encoded_stripId','TW_index']].\
            apply(lambda a : [a['start_time'],a['end_time'],a['SatID'],a['Memory_global_TW_index'],a['encoded_stripId'],a['TW_index']],axis=1)
        Memory_onlyImgTW_list = list(Final_Img_union_without_eclipse_df['list_foramt_memory'])
        #3 = onlygs =[st,et,s,n,g]
        Final_onlygs_union_without_eclipse_df = onlyimgGs_unionWithout_eclipse_df[~onlyimgGs_unionWithout_eclipse_df['gsID'].isnull()]
        Final_onlygs_union_without_eclipse_df['list_foramt_memory'] = Final_onlygs_union_without_eclipse_df[['SatID','start_time','end_time','Memory_global_TW_index','gsID']].\
            apply(lambda a : [a['start_time'],a['end_time'],a['SatID'],a['Memory_global_TW_index'],a['gsID']],axis=1)
        Memory_onlyGsTW_list = list(Final_onlygs_union_without_eclipse_df['list_foramt_memory'])
        
        #======= #======= #======= #======= #======= #======= #======= #======= #======= #======= #======= #======= #======= #======= 
        #======= #======= #======= #======= #======= #======= #======= #======= #======= #======= #======= #======= #======= #======= 

        #adding enclipse and sunlit based window bifurcation assuming only either one of eclipse/sunlit will happen during gs pass .
        memory_based_copy_df = memory_based_df.copy() 
        
        memory_based_copy_df['EcStEnd_list'] = memory_based_copy_df[['SatID','start_time','end_time']].\
                                          apply( lambda a : \
                                          get_EcStEnd_list(a['start_time'],a['end_time'],df = self.eclipse_df_dict[a['SatID']] ),axis= 1 )
        #imgGS_union_df1['EcStEnd_list'] ==> [[st,et,eclipse],[st,et,eclipse]]
        #print(imgGS_union_df1['EcStEnd_list'])
        memory_based_copy_df['len_EcStEnd_list'] = memory_based_copy_df['EcStEnd_list'].apply(lambda a : len(a))
        print('len_before_eclipse_transition_divide=',len(memory_based_copy_df))
        memory_based_copy_df = memory_based_copy_df.explode('EcStEnd_list')
        print('len_after(after explode)_eclipse_transition_divide=',len(memory_based_copy_df))
        memory_based_copy_df['new_eclipse'] = memory_based_copy_df['EcStEnd_list'].apply(lambda a : a[0])
        memory_based_copy_df['new_start_time'] = memory_based_copy_df['EcStEnd_list'].apply(lambda a : a[1])
        memory_based_copy_df['new_end_time'] = memory_based_copy_df['EcStEnd_list'].apply(lambda a : a[2])

        #Z1
        check_l1 = len(memory_based_copy_df)
        print('Before_Ambiguous_event_transition_divide =',len(memory_based_copy_df))
        print(memory_based_copy_df[memory_based_copy_df['new_start_time'] =='NA'])
        memory_based_copy_df = memory_based_copy_df[memory_based_copy_df['new_start_time']!='NA']
        print('After_len_of_ambiguous_event_transition_divide =',len(memory_based_copy_df))
        check_l2 = len(memory_based_copy_df)
        if check_l1!=check_l2:
            print("something is wrong in eclipse data or opprtunity start time or  end time")

        memory_based_copy_df.drop(['start_time','end_time','Eclipse','EcStEnd_list'],axis=1,inplace=True)
        memory_based_copy_df.rename(columns={'new_start_time':'start_time','new_end_time':'end_time','new_eclipse':'Eclipse'},inplace=True)
        
        memory_based_copy_df = memory_based_copy_df.drop(['till_now_max','prev_max'], axis=1)
        # imgGS_union_df1 : only having noImgNogs case
        # imgGS_union_df : either img or gs case

        memory_based_copy_df = memory_based_copy_df.sort_values( by='start_time')
        memory_based_copy_df['power_global_TW'] = memory_based_copy_df[['start_time','end_time']].apply(list,axis=1)

        #========++++++++++=====================+++++++++=============================+++++++++=============================================================
        #power based or after eclipse divide , new TIME window index addition
        print("list of unique lenths of eclipse_transition_divide(bifurcation) = ",memory_based_copy_df['len_EcStEnd_list'].unique())
        same_as_memory_global_TW_index_df = memory_based_copy_df[memory_based_copy_df['len_EcStEnd_list'] == 1]
        same_as_memory_global_TW_index_df['power_global_TW_index'] = same_as_memory_global_TW_index_df['Memory_global_TW_index']
        different_from_memory_global_TW_index_df = memory_based_copy_df[memory_based_copy_df['len_EcStEnd_list'] > 1]
        different_from_memory_global_TW_index_df['power_global_TW_index'] = different_from_memory_global_TW_index_df.groupby('SatID').cumcount()+1 # changed GTWI for those TW having both  sunlit and eclipse
        different_from_memory_global_TW_index_df['power_global_TW_index'] = different_from_memory_global_TW_index_df['power_global_TW_index'] + 0.5 # add decimal so that it does not match with any other GTW
        print('len_power_based_memory_based=',len(same_as_memory_global_TW_index_df),'len_power_based_memory_based=',len(different_from_memory_global_TW_index_df))
        power_based_df = pd.concat([same_as_memory_global_TW_index_df,different_from_memory_global_TW_index_df])
        print('final_len_power_based=',len(power_based_df))
        power_based_df.sort_values('start_time',inplace=True)

        # Dedicated TW for Readout 
        # Eclipse    Common_r
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
        
        dedicatedReadoutTWlist__concat_sat_memoryTWindex = dict(zip(dedicated_readout_df['concat_sat_memoryTWindex'],dedicated_readout_df['list_format']))
        DROPriority__concat_sat_memoryTWindex = dict(zip(dedicated_readout_df['concat_sat_memoryTWindex'],dedicated_readout_df['readout_priority']))

        dedicated_readout_grouped_df = dedicated_readout_df.groupby('SatID').agg(TW_list = ('Memory_global_TW_index',set)).reset_index()
        dedicatedReadoutTWIndex__sat = dict(zip(dedicated_readout_grouped_df['SatID'],dedicated_readout_grouped_df['TW_list']))

        prev_dedicatedReadoutIndex__s_TWI_dict__s = {}
        dedicated_readout_df.sort_values(by='start_time',inplace = True)
        for s in dedicated_readout_df['SatID'].unique():
            to_get_readout_prev_index_df =dedicated_readout_df[dedicated_readout_df['SatID']==s]
            prev_dedicatedReadoutIndex__s_TWI_dict__s[s] = get_prev_TW_index(to_get_readout_prev_index_df,'Memory_global_TW_index')

        #ADDING prev TW list for power based format TW table  
        prev_power_tWList__s_TWI_dict__s = {}
        for s in power_based_df['SatID'].unique():
            to_get_power_prev_index_df =power_based_df[power_based_df['SatID']==s]

            prev_power_tWList__s_TWI_dict__s[s] = get_prev_TW_index(to_get_power_prev_index_df,'power_global_TW_index')
    
        #ENtire dict of power based TW GTWI(global time window index) list
        imgGS_Grouped_union_df2 = power_based_df.groupby(['SatID']).agg(global_TWindex_list = ('power_global_TW_index',list)).reset_index()
        PowerglobalTWindexSortedList__s = dict(zip(imgGS_Grouped_union_df2['SatID'],imgGS_Grouped_union_df2['global_TWindex_list']))

        #power based TW list for cases : noImgNoGs,onlyImg,onlyGs
        FinalNoImgGS_union_df = power_based_df[power_based_df['gsID'] == 'no_i_no_g']
        FinalNoImgGS_union_df['list_foramt_memoryPower'] = FinalNoImgGS_union_df[['encoded_stripId','SatID','start_time','end_time','TW_index','Eclipse','power_global_TW_index']].\
            apply(lambda a : [a['start_time'],a['end_time'],a['SatID'],a['encoded_stripId'],a['TW_index'],a['Eclipse'],a['power_global_TW_index']],axis=1)
        Power_NoimageGs_TW_list = list(FinalNoImgGS_union_df['list_foramt_memoryPower'])

        #1 = noImgNoGs = [st,et,s,j,k,eclipse,n]
        onlyimgGs_union_df = power_based_df[power_based_df['gsID'] != 'no_i_no_g']
        Final_Img_union_df = onlyimgGs_union_df[~onlyimgGs_union_df['encoded_stripId'].isnull()] # i<->g # i <-> !g
        Final_Img_union_df['list_foramt_memoryPower'] = Final_Img_union_df[['encoded_stripId','SatID','start_time','end_time','TW_index','Eclipse','power_global_TW_index']].\
            apply(lambda a : [a['start_time'],a['end_time'],a['SatID'],a['encoded_stripId'],a['TW_index'],a['Eclipse'],a['power_global_TW_index']],axis=1)
        
        #2 = OnlyImg = [st,et,s,j,k,eclipse,n]
        Power_image_TW_list = list(Final_Img_union_df['list_foramt_memoryPower'])
        Final_GS_union_df = onlyimgGs_union_df[~onlyimgGs_union_df['gsID'].isnull()]
        Final_GS_union_df['list_foramt_memoryPower'] = Final_GS_union_df[['gsID','SatID','start_time','end_time','TW_index','Eclipse','power_global_TW_index']].\
            apply(lambda a : [a['start_time'],a['end_time'],a['SatID'],a['gsID'],a['TW_index'],a['Eclipse'],a['power_global_TW_index']],axis=1)
        #3 = OnlyGS = [st,et,s,g,k,eclipse,n]
        Power_GS_TW_list = list(Final_GS_union_df['list_foramt_memoryPower'])

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
                "DROPriority__concat_sat_memoryTWindex":DROPriority__concat_sat_memoryTWindex}

        
    def create_latest_dict(self):
        
        # original_gs_pass_df
        self.gsPass_df['SatID'] = self.gsPass_df['SatID'].astype(str)
        self.gsPass_df['gsID'] = self.gsPass_df['gsID'].astype(str)
        self.image_opportunity_df['SatID'] = self.image_opportunity_df['SatID'].astype(str)
        self.image_opportunity_df['StripID'] = self.image_opportunity_df['StripID'].astype(str)
        self.image_opportunity_df['AoiID'] = self.image_opportunity_df['AoiID'].astype(str)
        self.image_opportunity_df['encoded_stripId'] =   self.image_opportunity_df['StripID'] + '_' + self.image_opportunity_df['AoiID']
        original_gs_pass_df = self.gsPass_df
        # conflict_gs_img_pass
        self.gsPass_df = self.gsPass_df[['SatID','gsID','start_time','end_time','Eclipse']].drop_duplicates()
        self.gsPass_df['TW_gs'] = self.gsPass_df[['start_time','end_time']].apply(list,axis=1)
        self.gsPass_df['TW_index'] = self.gsPass_df.groupby(['gsID','SatID'])['TW_gs'].rank(method='dense')

        original_gs_pass_grouped_df = self.gsPass_df.groupby('SatID').agg(TW_list_gspass = ('TW_gs',list),\
                                                             GS_list = ('gsID',list)).reset_index()
        
        self.image_opportunity_df['TW'] = self.image_opportunity_df[['OpportunityStartOffset','OpportunityEndOffset']].apply(list,axis=1)
        
        
        self.image_opportunity_df = pd.merge(self.image_opportunity_df,original_gs_pass_grouped_df,on='SatID',how='left')
        self.image_opportunity_df[['TW_list_gspass', 'gs_list']] = self.image_opportunity_df[['TW_list_gspass','GS_list']].fillna(value='NA')
        self.image_opportunity_df['flag_gs_pass_conflict'] = self.image_opportunity_df[['TW','TW_list_gspass']].apply( lambda a : remove_opportunities_conflict_GSpass(a['TW'],a['TW_list_gspass'],setup_time=self.setup_time), axis = 1)
        
        conflict_image_opportunity_df = self.image_opportunity_df[self.image_opportunity_df['flag_gs_pass_conflict']==1] #TODO1
        self.success_metric_before = {'conflict_images':list(conflict_image_opportunity_df['encoded_stripId'].unique()) }
        self.image_opportunity_df = self.image_opportunity_df[self.image_opportunity_df['flag_gs_pass_conflict']==0]

        #imgery_sat_id_list,
    
        self.image_opportunity_df['TW_index'] = self.image_opportunity_df.groupby(['encoded_stripId','SatID'])['TW'].rank(method='dense')
        self.image_opportunity_df['concat_SatID_encodedStripId_TWindex'] = self.image_opportunity_df['SatID'] +'_' + \
                                                                           self.image_opportunity_df['encoded_stripId'] +\
                                                                           '_' + self.image_opportunity_df['TW_index'].astype(str)
        
        #
        imgery_sat_id_list = list(self.image_opportunity_df['SatID'].unique())
        only_gs_sat_id_list = list(original_gs_pass_df[~original_gs_pass_df['SatID'].isin(imgery_sat_id_list)]['SatID'].unique())
        encoded_stripId_list = self.image_opportunity_df['encoded_stripId'].unique()
        unique_img_opportunities_list = list(self.image_opportunity_df['concat_SatID_encodedStripId_TWindex'].unique())

        image_opportunity_grouped_df = self.image_opportunity_df.groupby('encoded_stripId').agg(opportunity_list = ('concat_SatID_encodedStripId_TWindex',list)).reset_index()
        self.image_opportunity_grouped_df1 = self.image_opportunity_df.groupby('SatID').agg(opportunity_list = ('concat_SatID_encodedStripId_TWindex',set)).reset_index()

        cs1j2k2Domainlist__cs1j1k1 = {}
        #sj1k1j2k2_dict['domain_of_csjk'] ={}
        cs1j2k2Domainlist__cs1j1k1 = get_conflicting_dict(self.image_opportunity_df,cs1j2k2Domainlist__cs1j1k1,self.setup_time,'SatID',\
                                                          'concat_SatID_encodedStripId_TWindex','OpportunityEndOffset',\
                                                            'OpportunityStartOffset')
    

        csjkSet__s = dict(zip(self.image_opportunity_grouped_df1['SatID'],self.image_opportunity_grouped_df1['opportunity_list']))

        csjkList__j = dict(zip(image_opportunity_grouped_df['encoded_stripId'],\
                                                        image_opportunity_grouped_df['opportunity_list']))
       
        TW__csjk = dict(zip(self.image_opportunity_df['concat_SatID_encodedStripId_TWindex'],\
                                                  self.image_opportunity_df['TW']))
        
        stripid__encodedstripID = dict(zip(self.image_opportunity_df['encoded_stripId'],\
                                                        self.image_opportunity_df['StripID']))
        
        AOIid__encodedstripID = dict(zip(self.image_opportunity_df['encoded_stripId'],\
                                                        self.image_opportunity_df['AoiID']))
        
        
        camera_memory_capacity__s = {s: 1000 for s in imgery_sat_id_list+only_gs_sat_id_list}
        initial_camera_memory_value__s = {s: 20 for s in imgery_sat_id_list+only_gs_sat_id_list}

        readout_memory_capacity__s = {s: 980 for s in imgery_sat_id_list+only_gs_sat_id_list}
        initial_readout_camera_memory_value__s = {s: 350 for s in imgery_sat_id_list+only_gs_sat_id_list}

        power_capacity__s = {s: 720000 for s in imgery_sat_id_list+only_gs_sat_id_list}
        initial_power_value__s = {s: 65000 for s in imgery_sat_id_list+only_gs_sat_id_list}

        camera_thermal_capacity__s = {s: 45 for s in imgery_sat_id_list+only_gs_sat_id_list}
        camera_initial_thermal_value__s = {s: 20 for s in imgery_sat_id_list+only_gs_sat_id_list}

        readout_thermal_capacity__s = {s: 45 for s in imgery_sat_id_list+only_gs_sat_id_list}
        readout_initial_thermal_value__s = {s: 20 for s in imgery_sat_id_list+only_gs_sat_id_list}

        imaging_rate =  7 #gbps
        downlinking_rate = 0.5
        Readout_rate = 0.8


        print("=====IMAGING THEMAL======")
        if self.config['constraints']['thermal_constraint_imaging']:
            heatCameraTimeBucket_SCT_dict__s = {s : get_thermal_bucket(camera_initial_thermal_value__s[s], \
                                            self.config['thermal_parameters']['cameraDetector_heat_eqn'] ,\
                                            self.config['thermal_parameters']['cameraDetector_cool_eqn'],\
                                            camera_thermal_capacity__s[s]) \
                                            for s in imgery_sat_id_list+only_gs_sat_id_list}
            
            max_camera_heat_dict = {s : heatCameraTimeBucket_SCT_dict__s[s]['max_time_heat'] for s in imgery_sat_id_list+only_gs_sat_id_list }

            for s in imgery_sat_id_list+only_gs_sat_id_list :
                del heatCameraTimeBucket_SCT_dict__s[s]['max_time_heat']

        else:
            heatCameraTimeBucket_SCT_dict__s = {}
            max_camera_heat_dict = {}

        print("=====READOUT THERMAL======")
        if self.config['constraints']['thermal_constraint_readout']:
            heatTimeBucket_SCT_dict__s = {
                                        s : get_thermal_bucket(readout_initial_thermal_value__s[s], \
                                        self.config['thermal_parameters']['NCCM_heat_eqn'] ,\
                                        self.config['thermal_parameters']['NCCM_cool_eqn'],\
                                        readout_thermal_capacity__s[s]) \
                                        for s in imgery_sat_id_list+only_gs_sat_id_list\
                                        }
        
            max_readout_heat_dict = {s : heatTimeBucket_SCT_dict__s[s]['max_time_heat'] for s in imgery_sat_id_list+only_gs_sat_id_list }
        
            for s in imgery_sat_id_list+only_gs_sat_id_list :
                del heatTimeBucket_SCT_dict__s[s]['max_time_heat']
        else:

            heatTimeBucket_SCT_dict__s ={}
            max_readout_heat_dict = {}

        return {"encoded_stripId_list":encoded_stripId_list,\
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
                "initial_power_value__s":initial_power_value__s,\
                "initial_readout_camera_memory_value__s":initial_readout_camera_memory_value__s,\
                "imaging_rate":imaging_rate,\
                "Readout_rate":Readout_rate,\
                "heatCameraTimeBucket_SCT_dict__s":heatCameraTimeBucket_SCT_dict__s,\
                "heatTimeBucket_SCT_dict__s":heatTimeBucket_SCT_dict__s,\
                "max_camera_heat_dict":max_camera_heat_dict,\
                "max_readout_heat_dict":max_readout_heat_dict,\
                "stripid__encodedstripID":stripid__encodedstripID,\
                "AOIid__encodedstripID":AOIid__encodedstripID }
    #max_camera_heat_dict,max_readout_heat_dict,unique_img_opportunities_list,TW__csjk,imgery_sat_id_list,csjkSet__s,cs1j2k2Domainlist__cs1j1k1,camera_memory_capacity__s,only_gs_sat_id_list
    #MemoryglobalTWindexSortedList__s,readout_memory_capacity__s,dedicatedReadoutTWIndex__sat,TW__csn,power_capacity__s,PowerglobalTWindexSortedList__s,Memory_onlyGsTW_list
    #heatTimeBucket_SCT_dict__s,Power_GS_TW_list,encoded_stripId_list,csjkList__j,Memory_NoimageGs_TW_list
    #dedicatedReadoutTWlist__concat_sat_memoryTWindex,Readout_rate,prev_dedicatedReadoutIndex__s_TWI_dict__s,initial_readout_camera_memory_value__s
    #prev_tWList__s_TWI_dict__s,initial_camera_memory_value__s,Memory_onlyImgTW_list,imaging_rate,prev_ImagingTWList__s_TWI_dict__s,heatCameraTimeBucket_SCT_dict__s,
    #Power_NoimageGs_TW_list,prev_power_tWList__s_TWI_dict__s,initial_power_value__s,Power_image_TW_list,TotalPriority__csjk,DROPriority__concat_sat_memoryTWindex,
    #GlobalPriority__csjk,Local_Priority__csjk,GlobalPriority__csj,Local_Priority__csj,stripid__encodedstripID,AOIid__encodedstripID
    
    #active_assured_strip_id_list
    def preprocess(self):
        model_input_data_dict = self.create_latest_dict()
        #elf.get_temporal_data()
        #print(self.image_opportunity_df.isnull().sum(),"CHECK 1")
        priority_dict = self.get_priorities()
        model_input_data_dict.update(priority_dict)

        # assured_strip_id_list = get_active_assured_task(self.image_opportunity_df,model_input_data_dict)
        # model_input_data_dict['active_assured_strip_id_list'] = assured_strip_id_list

        #assured_strip_id_list = get_active_assured_task(self.image_opportunity_df,model_input_data_dict)
        model_input_data_dict['active_assured_strip_id_list'] = []#assured_strip_id_list


        MTW_PTW_dict = self.create_MTW_PTW()
        model_input_data_dict.update(MTW_PTW_dict)

        return model_input_data_dict
