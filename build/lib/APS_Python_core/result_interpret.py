import pandas as pd
import warnings
warnings.filterwarnings('ignore')
import json 
import datetime
from APS_Python_core.utils import remove_opportunities_conflict_GSpass
#========================================================================================================================================================================================================
#========================================================================================================================================================================================================
def get_conflict_dict_for_image(image_input_df):
    cs1j2k2Domainlist__cs1j1k1 = {}
        #sj1k1j2k2_dict['domain_of_csjk'] ={}
    for s in image_input_df['SatID'].unique():
        this_df = image_input_df[image_input_df['SatID'] == s ]
        for sjk in this_df['concat_SatID_encodedStripId_TWindex'].unique():
            that_df = this_df[this_df['concat_SatID_encodedStripId_TWindex'] == sjk]
            this_LOS = list(that_df['OpportunityEndOffset'].unique())[0]
            this_AOS = list(that_df['OpportunityStartOffset'].unique())[0]

            that_df1 = this_df[this_df['OpportunityStartOffset'] >= this_LOS + 120]#8994.072077,9002.072077  #9096.064325,9104.064325
            #that_df1 = that_df1[that_df1['OpportunityStartOffset'] >= this_AOS -  self.setup_time ]
            that_df2 = this_df[this_df['OpportunityEndOffset'] <= this_AOS -  120]
            that_df3 = pd.concat([that_df1,that_df2])
            #that_df1 = that_df1[that_df1['OpportunityEndOffset'] <= this_LOS + self.setup_time]

            
            notNeeded_oppor_forOverlap_const = list(that_df3['concat_SatID_encodedStripId_TWindex'].unique())
            
            that_df1 = this_df[~this_df['concat_SatID_encodedStripId_TWindex'].isin(notNeeded_oppor_forOverlap_const)]
            
            # that_df1 = this_df[this_df['OpportunityStartOffset'] <= this_LOS + self.setup_time]
            # that_df1 = that_df1[that_df1['OpportunityStartOffset'] >= this_AOS -  self.setup_time ]
            # that_df1 = that_df1[that_df1['OpportunityEndOffset'] >= this_AOS -  self.setup_time ]
            # that_df1 = that_df1[that_df1['OpportunityEndOffset'] <= this_LOS + self.setup_time]
            
            that_df1 = that_df1[that_df1['concat_SatID_encodedStripId_TWindex'] != sjk ]
            
            cs1j2k2Domainlist__cs1j1k1[sjk] = list(that_df1['concat_SatID_encodedStripId_TWindex'].unique())
    return cs1j2k2Domainlist__cs1j1k1
#========================================================================================================================================================================================================
#========================================================================================================================================================================================================
def interpret_result(image_input_df,gsPass_output_df,image_result_df,config):
    # image_result_df = pd.read_csv('5_output_data/img_capture_schedule.csv') #Imaging_new_output ##Imaging_new_output (1)
    # image_input_df = pd.read_csv('1_input_data/Imaging_new (1).csv') ##Imaging_new #Imaging_new (1)
    # gsPass_output_df = pd.read_csv('5_output_data/gs_pass_result_df.csv')#GS_Passes_new #GS_Passes_new (1)
    # gsPass_input_df = pd.read_csv('1_input_data/GS_Passes_new (1).csv')#GS_Passes_new #GS_Passes_new (1)

    # with open('1_input_data/config.json', 'r') as file:
    #     config = json.load(file)

    image_input_df['StripID'] = image_input_df['StripID'].astype(str)
    image_result_df['StripID'] = image_result_df['StripID'].astype(str)

    setup_time = 120

    C_priority = 0.4
    C_cc = 0.1
    C_dd = 0.2
    C_offN = 0.3

    #========================================================================================================================================================================================================
    #========================================================================================================================================================================================================
    gsPass_output_df = gsPass_output_df[['SatID','gsID','start_time','end_time','Eclipse']].drop_duplicates()
    gsPass_output_df['TW_gs'] = gsPass_output_df[['start_time','end_time']].apply(list,axis=1)
    gsPass_output_df['TW_index'] = gsPass_output_df.groupby(['gsID','SatID'])['TW_gs'].rank(method='dense')

    original_gs_pass_grouped_df = gsPass_output_df.groupby('SatID').agg(TW_list_gspass = ('TW_gs',list),\
                                                            GS_list = ('gsID',list)).reset_index()


    image_input_df['TW'] = image_input_df[['OpportunityStartOffset','OpportunityEndOffset']].apply(list,axis=1)


    image_input_df = pd.merge(image_input_df,original_gs_pass_grouped_df,on='SatID',how='left')
    image_input_df[['TW_list_gspass', 'gs_list']] = image_input_df[['TW_list_gspass','GS_list']].fillna(value='NA')
    image_input_df['flag_gs_pass_conflict'] = image_input_df[['TW','TW_list_gspass']].apply( lambda a : remove_opportunities_conflict_GSpass(a['TW'],a['TW_list_gspass'],setup_time=120), axis = 1)

    conflict_image_opportunity_df = image_input_df[image_input_df['flag_gs_pass_conflict']==1] #TODO1
    #conflict_image_opportunity_df['encoded_stripId'] =   conflict_image_opportunity_df['StripID'].astype(str) + '_' + conflict_image_opportunity_df['AoiID'].astype(str)

    #success_metric_before = {'conflict_images':list(conflict_image_opportunity_df['encoded_stripId'].unique()) }

    #========================================================================================================================================================================================================
    image_input_df['X'] = image_input_df[['OpportunityStartTime','OpportunityStartOffset']].apply(lambda a: pd.to_datetime(a['OpportunityStartTime']) - pd.DateOffset(seconds=a['OpportunityStartOffset']),axis=1)
    image_input_df['Y'] = image_input_df[['OpportunityEndTime','OpportunityEndOffset']].apply(lambda a: pd.to_datetime(a['OpportunityEndTime']) - pd.DateOffset(seconds=a['OpportunityEndOffset']),axis=1)
    base_time_stamp = image_input_df["X"].to_list()[0]
    #base_time_stamp = base_time_stamp #self.base_time_stamp
    image_input_df['base_timestamp'] = base_time_stamp
    image_input_df['due_date_end_offset'] = pd.to_datetime(image_input_df['OrderValidityEnd']) - pd.to_datetime(image_input_df['base_timestamp'])#image_input_df['base_timestamp']
    image_input_df['due_date_end_offset'] = image_input_df[['due_date_end_offset']].apply(lambda a : a['due_date_end_offset'].total_seconds(),axis=1)
    image_input_df['due_seconds_diff'] =  image_input_df['due_date_end_offset'] #- image_input_df['base_timestamp']
    #==============================================================================================================
    image_input_df['local_priority_due_date'] = image_input_df['due_seconds_diff'].apply(lambda a : abs(1/(a+0.0001)) )# 48 becuause if due date is less than 2 days from now it will exponentially increase for that denominator should be less than 1 .
    image_input_df['local_priority_CC_based'] = abs(1 / (image_input_df['CloudCover']+0.0001)/(10  ))
    image_input_df['local_priority_offNadir'] = abs(1 / (image_input_df['OffNadir']+0.0001)/(8  ))

    #==============================================================================================================
    image_input_df['normalized_local_priority_due_date'] = (image_input_df['local_priority_due_date'] / image_input_df['local_priority_due_date'].max())*1000#/ (image_input_df['local_priority_due_date'].max() - image_input_df['local_priority_due_date'].min())
    image_input_df['normalized_local_priority_CC_based'] = (image_input_df['local_priority_CC_based'] / image_input_df['local_priority_CC_based'].max())*1000#/ (image_input_df['local_priority_CC_based'].max() - image_input_df['local_priority_CC_based'].min())
    image_input_df['normalized_local_priority_offNadir'] = (image_input_df['local_priority_offNadir'] / image_input_df['local_priority_offNadir'].max())*1000#/ (image_input_df['local_priority_offNadir'].max() - image_input_df['local_priority_offNadir'].min())
    max_GP_= image_input_df[image_input_df['Priority']<1000]['Priority'].max()
    image_input_df['normalized_GlobalPriority'] = (image_input_df['Priority'] / max_GP_)*1000#/ (image_input_df['Priority'].max() - image_input_df['Priority'].min()-0.00001)
    image_input_df['normalized_GlobalPriority'] = image_input_df[['normalized_GlobalPriority','Priority']].apply(lambda a: a['Priority']*1000 if  a['Priority'] >=1000 else a['normalized_GlobalPriority'],axis = 1)
    image_input_df['normalized_local_priority_due_date'] = image_input_df[['normalized_local_priority_due_date','due_seconds_diff']].apply(lambda a: abs(1/(a['due_seconds_diff']+0.001))*24*60*1000*1000 if  a['due_seconds_diff'] <= 24*60 else a['normalized_local_priority_due_date'],axis = 1)

    #==============================================================================================================

    image_input_df['normalized_Total_Priority'] =  config['GP_weight']*image_input_df['normalized_GlobalPriority'] + \
                                                config['DDLP_weight']*image_input_df['normalized_local_priority_due_date']+\
                                                config['CCLP_weight']*image_input_df['normalized_local_priority_CC_based'] +\
                                                config['ONLP_weight']*image_input_df['normalized_local_priority_offNadir']

    image_input_df['normalized_Total_Priority'] = image_input_df['normalized_Total_Priority'].astype(int)

    image_input_df['TW'] = image_input_df[['OpportunityStartOffset','OpportunityEndOffset']].apply(list,axis=1)
    image_input_df['encoded_stripId'] =   image_input_df['StripID'] + '_' + image_input_df['AoiID']
    image_input_df['TW_index'] = image_input_df.groupby(['encoded_stripId','SatID'])['TW'].rank(method='dense')
    image_input_df['concat_SatID_encodedStripId_TWindex'] = image_input_df['SatID'] +'_' + \
                                                                            image_input_df['encoded_stripId'] +\
                                                                            '_' + image_input_df['TW_index'].astype(str)

    image_conflict_dict = get_conflict_dict_for_image(image_input_df)
    image_input_df['conflicting_strip_oppr'] = image_input_df['concat_SatID_encodedStripId_TWindex'].map(image_conflict_dict)

    #========================================================================================================================================================================================================#========================================================================================================================================================================================================
    #========================================================================================================================================================================================================


    image_result_df = image_result_df[image_result_df['operation']=='Imaging']
    image_result_df['flag'] = 1
    image_result_df['base_timestamp'] = base_time_stamp
    image_result_df['base_timestamp'] = image_result_df['base_timestamp'].astype(str)
    image_result_df['base_timestamp'] = image_result_df['base_timestamp'].apply(lambda x: datetime.datetime.strptime(x,'%Y-%m-%d %H:%M:%S'))

    image_result_df['OpportunityStartTime'] = image_result_df[['base_timestamp','start_time']].apply(lambda a: pd.to_datetime(a['base_timestamp']) + pd.DateOffset(seconds=a['start_time']),axis=1)
    image_result_df['OpportunityEndTime'] = image_result_df[['base_timestamp','end_time']].apply(lambda a: pd.to_datetime(a['base_timestamp']) + pd.DateOffset(seconds=a['end_time']),axis=1)

    image_result_df['OpportunityEndTime'] = image_result_df['OpportunityEndTime'].astype(str)
    image_result_df['OpportunityStartTime'] = image_result_df['OpportunityStartTime'].astype(str)
    image_input_df['OpportunityEndTime'] = image_input_df['OpportunityEndTime'].astype(str)
    image_input_df['OpportunityStartTime'] = image_input_df['OpportunityStartTime'].astype(str)
    #image_result_df['base_timestamp'] = image_result_df['base_timestamp'].dt.round('1s')
    #image_result_df['OpportunityStartTime'] = image_result_df[['start_time','base_timestamp']].apply(lambda a: a['base_timestamp']+ datetime.timedelta(a['start_time']),axis=1)
    #image_result_df['OpportunityStartTime'] = pd.to_datetime(image_result_df['base_timestamp']) + pd.to_timedelta(image_result_df['start_time'], unit='s')

    #image_result_df['time_delta_start'] = pd.to_timedelta(image_result_df['start_time'])
    #image_result_df['OpportunityStartTime'] = image_result_df[['start_time','base_timestamp']].apply(lambda a: a['base_timestamp'] + datetime.timedelta(a['start_time']),axis =1)
    #image_result_df['OpportunityStartTime'] = image_result_df['OpportunityStartTime'].dt.round('1s')



    image_input_df_1 = pd.merge(image_input_df,image_result_df,on=['SatID', 'StripID', 'OpportunityStartTime', 'OpportunityEndTime','AoiID','base_timestamp'],how='left')

    image_input_df_1['flag'] = image_input_df_1['flag'].fillna(0)
    #========================================================================================================================================================================================================
    #========================================================================================================================================================================================================
    extracted_raw_file_df= image_input_df_1[['SatID', 'OpportunityStartTime',
        'OpportunityEndTime','StripID', 'OffNadir','OrderValidityStart', 'OrderValidityEnd','AoiID','Priority','OpportunityStartOffset',
        'OpportunityEndOffset','normalized_local_priority_due_date',
        'normalized_local_priority_CC_based',
        'normalized_local_priority_offNadir', 'normalized_GlobalPriority',
        'normalized_Total_Priority','camera_memory_value_endofTW',
        'delta_camera_memory_value_in_this_TW','flag','flag_gs_pass_conflict','conflicting_strip_oppr','concat_SatID_encodedStripId_TWindex','CloudCoverLimit','CloudCover' ]]


    extracted_raw_file_df['encoded_strip_id'] = extracted_raw_file_df['StripID'].astype(str)+ '_' + extracted_raw_file_df['AoiID'].astype(str)
    #========================================================================================================================================================================================================
    #========================================================================================================================================================================================================
    extracted_raw_file_df.sort_values(by='normalized_local_priority_CC_based',ascending=False)
    extracted_raw_imaging_filtered_df = extracted_raw_file_df[extracted_raw_file_df['flag'] == 1]

    conflict_strip_oppr_df = pd.DataFrame()
    for csjk in extracted_raw_imaging_filtered_df['concat_SatID_encodedStripId_TWindex'].unique():
        df1 = extracted_raw_imaging_filtered_df[extracted_raw_imaging_filtered_df['concat_SatID_encodedStripId_TWindex']==csjk]
        conflict_strip_list = df1['conflicting_strip_oppr'].to_list()[0]
        conflict_strip_list = conflict_strip_list + [csjk]
        filtered_for_this_csjk_df = extracted_raw_file_df[extracted_raw_file_df['concat_SatID_encodedStripId_TWindex'].isin(conflict_strip_list)]
        filtered_for_this_csjk_df['conflic_strip_flag_named'] = csjk  
        filtered_for_this_csjk_df = filtered_for_this_csjk_df[['concat_SatID_encodedStripId_TWindex','conflicting_strip_oppr','OpportunityStartTime','OpportunityEndTime','StripID','normalized_GlobalPriority','normalized_local_priority_due_date','normalized_Total_Priority','conflic_strip_flag_named']]
        conflict_strip_oppr_df = pd.concat([conflict_strip_oppr_df,filtered_for_this_csjk_df])

    selected_for_merged_df = extracted_raw_file_df[['concat_SatID_encodedStripId_TWindex','normalized_GlobalPriority','normalized_local_priority_due_date','normalized_Total_Priority']].drop_duplicates()
    selected_for_merged_df.rename(columns={'normalized_GlobalPriority':'this_flag_norm_GP',\
                                        "normalized_local_priority_due_date":'this_flag_norm_LLDD',\
                                            "normalized_Total_Priority":"this_flag_norm_TP",\
                                            "concat_SatID_encodedStripId_TWindex":"conflic_strip_flag_named"},inplace=True)
    #========================================================================================================================================================================================================
    #========================================================================================================================================================================================================
    conflict_strip_oppr_df['max_Norm_GP'] = conflict_strip_oppr_df.groupby('conflic_strip_flag_named')['normalized_GlobalPriority'].transform('max')
    conflict_strip_oppr_df['max_Norm_LPDD'] = conflict_strip_oppr_df.groupby('conflic_strip_flag_named')['normalized_local_priority_due_date'].transform('max')
    conflict_strip_oppr_df['max_Norm_TP'] = conflict_strip_oppr_df.groupby('conflic_strip_flag_named')['normalized_Total_Priority'].transform('max')
    conflict_strip_oppr_df_ = pd.merge(conflict_strip_oppr_df,selected_for_merged_df,on='conflic_strip_flag_named',how='left')
    conflict_strip_oppr_df_ = conflict_strip_oppr_df_[['conflic_strip_flag_named','max_Norm_TP','this_flag_norm_TP','max_Norm_GP','this_flag_norm_GP','max_Norm_LPDD','this_flag_norm_LLDD']].drop_duplicates()
    #========================================================================================================================================================================================================
    #========================================================================================================================================================================================================
    

    criteria_list = []
    before_APS = []
    APS_result = []
    flag_list =[]

    APS_output_raw_file_df = extracted_raw_file_df[extracted_raw_file_df['flag']==1]

    criteria_list.append('Number_of_Oppr')
    before_APS.append(len(extracted_raw_file_df))
    APS_result.append(len(APS_output_raw_file_df))
    flag_list.append("including_oppr_gs_pass_conflicts")

    criteria_list.append('Number_of_Strips')
    before_APS.append(extracted_raw_file_df['encoded_strip_id'].nunique())
    APS_result.append(APS_output_raw_file_df['encoded_strip_id'].nunique())
    flag_list.append("including_oppr_gs_pass_conflicts")

    criteria_list.append('Number_of_AoiID')
    before_APS.append(extracted_raw_file_df['AoiID'].nunique())
    APS_result.append(APS_output_raw_file_df['AoiID'].nunique())
    flag_list.append("including_oppr_gs_pass_conflicts")
    #================================================================================================================================================
    conflictsFree_raw_file_df = extracted_raw_file_df[extracted_raw_file_df['flag_gs_pass_conflict']==0]
    APS_conflictsFree_raw_file_df = conflictsFree_raw_file_df[conflictsFree_raw_file_df['flag']==1]

    criteria_list.append('Number_of_Oppr')
    before_APS.append(len(conflictsFree_raw_file_df))
    APS_result.append(len(APS_conflictsFree_raw_file_df))
    flag_list.append("Excluding_oppr_gs_pass_conflicts")

    criteria_list.append('Number_of_Strips')
    before_APS.append(conflictsFree_raw_file_df['StripID'].nunique())
    APS_result.append(APS_conflictsFree_raw_file_df['StripID'].nunique())
    flag_list.append("Excluding_oppr_gs_pass_conflicts")

    criteria_list.append('Number_of_AoiID')
    before_APS.append(conflictsFree_raw_file_df['AoiID'].nunique())
    APS_result.append(APS_conflictsFree_raw_file_df['AoiID'].nunique())
    flag_list.append("Excluding_oppr_gs_pass_conflicts")
    #================================================================================================================================================
    conflictsFree_raw_file_dd_df = conflictsFree_raw_file_df[['StripID','AoiID',\
                                                            'normalized_GlobalPriority',\
                                                                'normalized_local_priority_due_date']].drop_duplicates()
    APS_conflictsFree_raw_file_dd_df = APS_conflictsFree_raw_file_df[['StripID','AoiID',\
                                                            'normalized_GlobalPriority',\
                                                                'normalized_local_priority_due_date']].drop_duplicates()
    criteria_list.append('normalized_GlobalPriority')
    before_APS.append(conflictsFree_raw_file_dd_df['normalized_GlobalPriority'].sum())
    APS_result.append(APS_conflictsFree_raw_file_dd_df['normalized_GlobalPriority'].sum())
    flag_list.append("Excluding_oppr_gs_pass_conflicts")

    criteria_list.append('normalized_local_priority_due_date')
    before_APS.append(conflictsFree_raw_file_dd_df['normalized_local_priority_due_date'].sum())
    APS_result.append(APS_conflictsFree_raw_file_dd_df['normalized_local_priority_due_date'].sum())
    flag_list.append("Excluding_oppr_gs_pass_conflicts")

    selected_NTP_conflictsFree_raw_file_df = conflictsFree_raw_file_df[['StripID','AoiID',\
                                                            'normalized_Total_Priority']]
    selected_NTP_conflictsFree_raw_grouped_file_df = selected_NTP_conflictsFree_raw_file_df.groupby(['StripID','AoiID']).agg(mean_normalized_Total_Priority = ('normalized_Total_Priority','mean')).reset_index()

    selected_NTP_APSconflictsFree_raw_file_df = APS_conflictsFree_raw_file_df[['StripID','AoiID',\
                                                            'normalized_Total_Priority']]
    selected_NTP_APSconflictsFree_raw_grouped_file_df = selected_NTP_APSconflictsFree_raw_file_df.groupby(['StripID','AoiID']).agg(mean_normalized_Total_Priority = ('normalized_Total_Priority','mean')).reset_index()

    criteria_list.append('Weighted_normalized_Total_priority')
    before_APS.append(selected_NTP_conflictsFree_raw_grouped_file_df['mean_normalized_Total_Priority'].sum())
    APS_result.append(selected_NTP_APSconflictsFree_raw_grouped_file_df['mean_normalized_Total_Priority'].sum())
    flag_list.append("Excluding_oppr_gs_pass_conflicts")

    #==========================================================================================================================================================================================================================================
    priority_based_result_list = ['normalized_GlobalPriority','normalized_local_priority_due_date']

    df1 = conflictsFree_raw_file_df[['encoded_strip_id','normalized_GlobalPriority','normalized_local_priority_due_date','flag']]
    df1.sort_values(by='flag',inplace = True, ascending =False)
    df1.drop_duplicates(subset = ['encoded_strip_id','normalized_GlobalPriority','normalized_local_priority_due_date'],keep = 'first',inplace =True)

    d1 = {}
    per_list = [25,50,75]
    for priority_basis in priority_based_result_list: 
        for i in per_list:
            d1[priority_basis+'_'+str(i)] = {}
        
    for priority_basis in priority_based_result_list:

        df1.sort_values(by = priority_basis,ascending=False,inplace=True)
        df1['cum_sum'] = df1[priority_basis].cumsum()
        max_cum_GP = df1['cum_sum'].max()
        df1['percentile'] = df1['cum_sum']/max_cum_GP *100

        df1['percentile_25'] = df1[priority_basis].quantile(0.25)
        df1['percentile_50'] = df1[priority_basis].quantile(0.50)
        df1['percentile_75'] = df1[priority_basis].quantile(0.75)
        
        top_25_percentile_df = df1[df1[priority_basis]>=df1['percentile_75']]
        d1[priority_basis+'_'+str(25)]['stripID_set_before'] = set(top_25_percentile_df['encoded_strip_id'])
        top_50_percentile_df = df1[df1[priority_basis]>=df1['percentile_50']]
        d1[priority_basis+'_'+str(50)]['stripID_set_before'] = set(top_50_percentile_df['encoded_strip_id'])
        top_75_percentile_df = df1[df1[priority_basis]>=df1['percentile_25']]
        d1[priority_basis+'_'+str(75)]['stripID_set_before'] = set(top_75_percentile_df['encoded_strip_id'])

        APS_selected_top_25_percentile_df = top_25_percentile_df[top_75_percentile_df['flag']==1]
        APS_selected_top_50_percentile_df = top_50_percentile_df[top_50_percentile_df['flag']==1]
        APS_selected_top_75_percentile_df = top_75_percentile_df[top_75_percentile_df['flag']==1]
        d1[priority_basis+'_'+str(25)]['stripID_set_after'] = set(APS_selected_top_25_percentile_df['encoded_strip_id'])
        d1[priority_basis+'_'+str(50)]['stripID_set_after'] = set(APS_selected_top_50_percentile_df['encoded_strip_id'])
        d1[priority_basis+'_'+str(75)]['stripID_set_after'] = set(APS_selected_top_75_percentile_df['encoded_strip_id'])


        criteria_list.append('top_25%_'+priority_basis+'_Strips')
        before_APS.append(top_25_percentile_df['encoded_strip_id'].nunique())
        APS_result.append(APS_selected_top_25_percentile_df['encoded_strip_id'].nunique())
        flag_list.append("Excluding_oppr_gs_pass_conflicts")

        criteria_list.append('top_50%_'+priority_basis+'_Strips')
        before_APS.append(top_50_percentile_df['encoded_strip_id'].nunique())
        APS_result.append(APS_selected_top_50_percentile_df['encoded_strip_id'].nunique())
        flag_list.append("Excluding_oppr_gs_pass_conflicts")

        criteria_list.append('top_75%_'+priority_basis+'_Strips')
        before_APS.append(top_75_percentile_df['encoded_strip_id'].nunique())
        APS_result.append(APS_selected_top_75_percentile_df['encoded_strip_id'].nunique())
        flag_list.append("Excluding_oppr_gs_pass_conflicts")

    per_list = [25,50,75]
    #for priority_basis in priority_based_result_list: 
    for i in per_list:
        criteria_list.append('common_strips_in_top_'+str(i))
        x = len(d1['normalized_GlobalPriority'+'_'+str(i)]['stripID_set_before'].intersection(d1['normalized_local_priority_due_date'+'_'+str(i)]['stripID_set_before']))
        before_APS.append(x)
        y = len(d1['normalized_GlobalPriority'+'_'+str(i)]['stripID_set_after'].intersection(d1['normalized_local_priority_due_date'+'_'+str(i)]['stripID_set_after']))
        APS_result.append(y)

        flag_list.append("Excluding_oppr_gs_pass_conflicts")


    KPI_df = pd.DataFrame({'criteria':criteria_list,'before_APS':before_APS,'APS_result':APS_result,'remarks':flag_list})
    KPI_df['percentage'] = KPI_df['APS_result']/KPI_df['before_APS']*100
    #========================================================================================================================================================================================================
    #========================================================================================================================================================================================================

    return {"interpret_extracted_raw_file_df":extracted_raw_file_df,\
            "interpret_selected_oppr_conflict_comparision_df":conflict_strip_oppr_df_,\
            "interpret_KPI_df":KPI_df}
