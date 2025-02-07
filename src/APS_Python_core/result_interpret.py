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
    for s in image_input_df['sat_id'].unique():
        this_df = image_input_df[image_input_df['sat_id'] == s ]
        for sjk in this_df['concat_sat_id_encoded_strip_id_tw_index'].unique():
            that_df = this_df[this_df['concat_sat_id_encoded_strip_id_tw_index'] == sjk]
            this_LOS = list(that_df['opportunity_end_offset'].unique())[0]
            this_AOS = list(that_df['opportunity_start_offset'].unique())[0]

            that_df1 = this_df[this_df['opportunity_start_offset'] >= this_LOS + 120]#8994.072077,9002.072077  #9096.064325,9104.064325
            that_df2 = this_df[this_df['opportunity_end_offset'] <= this_AOS -  120]
            that_df3 = pd.concat([that_df1,that_df2])
            notNeeded_oppor_forOverlap_const = list(that_df3['concat_sat_id_encoded_strip_id_tw_index'].unique())
            
            that_df1 = this_df[~this_df['concat_sat_id_encoded_strip_id_tw_index'].isin(notNeeded_oppor_forOverlap_const)]
           
            that_df1 = that_df1[that_df1['concat_sat_id_encoded_strip_id_tw_index'] != sjk ]
            
            cs1j2k2Domainlist__cs1j1k1[sjk] = list(that_df1['concat_sat_id_encoded_strip_id_tw_index'].unique())
    return cs1j2k2Domainlist__cs1j1k1
#========================================================================================================================================================================================================
#========================================================================================================================================================================================================
def interpret_result(image_input_df,gsPass_output_df,image_result_df,config):
   
    image_input_df['strip_id'] = image_input_df['strip_id'].astype(str)
    image_result_df['strip_id'] = image_result_df['strip_id'].astype(str)

    setup_time = 120
    #========================================================================================================================================================================================================
    #========================================================================================================================================================================================================
    gsPass_output_df = gsPass_output_df[['sat_id','gs_id','start_time','end_time']].drop_duplicates()
    gsPass_output_df['tw_gs'] = gsPass_output_df[['start_time','end_time']].apply(list,axis=1)
    gsPass_output_df['tw_index'] = gsPass_output_df.groupby(['gs_id','sat_id'])['tw_gs'].rank(method='dense')

    original_gs_pass_grouped_df = gsPass_output_df.groupby('sat_id').agg(tw_list_gspass = ('tw_gs',list),\
                                                            gs_list = ('gs_id',list)).reset_index()


    image_input_df['TW'] = image_input_df[['opportunity_start_offset','opportunity_end_offset']].apply(list,axis=1)


    image_input_df = pd.merge(image_input_df,original_gs_pass_grouped_df,on='sat_id',how='left')
    image_input_df[['tw_list_gspass', 'gs_list']] = image_input_df[['tw_list_gspass','gs_list']].fillna(value='NA')
    image_input_df['flag_gs_pass_conflict'] = image_input_df[['TW','tw_list_gspass']].apply( lambda a : remove_opportunities_conflict_GSpass(a['TW'],a['tw_list_gspass'],setup_time=120), axis = 1)

    conflict_image_opportunity_df = image_input_df[image_input_df['flag_gs_pass_conflict']==1] #TODO1
   
    #========================================================================================================================================================================================================
    image_input_df['X'] = image_input_df[['opportunity_start_time','opportunity_start_offset']].apply(lambda a: pd.to_datetime(a['opportunity_start_time']) - pd.DateOffset(seconds=a['opportunity_start_offset']),axis=1)
    image_input_df['Y'] = image_input_df[['opportunity_end_time','opportunity_end_offset']].apply(lambda a: pd.to_datetime(a['opportunity_end_time']) - pd.DateOffset(seconds=a['opportunity_end_offset']),axis=1)
    base_time_stamp = image_input_df["X"].to_list()[0]
    
    image_input_df['base_timestamp'] = base_time_stamp

    image_input_df['due_date_end_offset'] = pd.to_datetime(image_input_df['order_validity_end']) - pd.to_datetime(image_input_df['base_timestamp'])#image_input_df['base_timestamp']
    image_input_df['due_date_end_offset'] = image_input_df[['due_date_end_offset']].apply(lambda a : a['due_date_end_offset'].total_seconds(),axis=1)
    image_input_df['due_seconds_diff'] =  image_input_df['due_date_end_offset'] #- image_input_df['base_timestamp']
    #==============================================================================================================
    image_input_df['local_priority_due_date'] = image_input_df['due_seconds_diff'].apply(lambda a : abs(1/(a+0.0001)) )# 48 becuause if due date is less than 2 days from now it will exponentially increase for that denominator should be less than 1 .
    image_input_df['local_priority_cc_based'] = abs(1 / (image_input_df['cloud_cover']+0.0001)/(10  ))
    image_input_df['local_priority_off_nadir'] = abs(1 / (image_input_df['off_nadir']+0.0001)/(8  ))

    #==============================================================================================================
    image_input_df['normalized_local_priority_due_date'] = (image_input_df['local_priority_due_date'] / image_input_df['local_priority_due_date'].max())*1000#/ (image_input_df['local_priority_due_date'].max() - image_input_df['local_priority_due_date'].min())
    image_input_df['normalized_local_priority_cc_based'] = (image_input_df['local_priority_cc_based'] / image_input_df['local_priority_cc_based'].max())*1000#/ (image_input_df['local_priority_cc_based'].max() - image_input_df['local_priority_cc_based'].min())
    image_input_df['normalized_local_priority_off_nadir'] = (image_input_df['local_priority_off_nadir'] / image_input_df['local_priority_off_nadir'].max())*1000#/ (image_input_df['local_priority_off_nadir'].max() - image_input_df['local_priority_off_nadir'].min())
    max_GP_= image_input_df[image_input_df['global_priority']<1000]['global_priority'].max()
    image_input_df['normalized_global_priority'] = (image_input_df['global_priority'] / max_GP_)*1000#/ (image_input_df['global_priority'].max() - image_input_df['global_priority'].min()-0.00001)
    image_input_df['normalized_global_priority'] = image_input_df[['normalized_global_priority','global_priority']].apply(lambda a: a['global_priority']*1000 if  a['global_priority'] >=1000 else a['normalized_global_priority'],axis = 1)
    image_input_df['normalized_local_priority_due_date'] = image_input_df[['normalized_local_priority_due_date','due_seconds_diff']].apply(lambda a: abs(1/(a['due_seconds_diff']+0.001))*24*60*1000*1000 if  a['due_seconds_diff'] <= 24*60 else a['normalized_local_priority_due_date'],axis = 1)

    #==============================================================================================================

    image_input_df['normalized_total_priority'] =  config['GP_weight']*image_input_df['normalized_global_priority'] + \
                                                config['DDLP_weight']*image_input_df['normalized_local_priority_due_date']+\
                                                config['CCLP_weight']*image_input_df['normalized_local_priority_cc_based'] +\
                                                config['ONLP_weight']*image_input_df['normalized_local_priority_off_nadir']

    image_input_df['normalized_total_priority'] = image_input_df['normalized_total_priority'].astype(int)

    image_input_df['TW'] = image_input_df[['opportunity_start_offset','opportunity_end_offset']].apply(list,axis=1)
    image_input_df['encoded_strip_id'] =   image_input_df['strip_id'] + '_' + image_input_df['aoi_id']
    image_input_df['tw_index'] = image_input_df.groupby(['encoded_strip_id','sat_id'])['TW'].rank(method='dense')
    image_input_df['concat_sat_id_encoded_strip_id_tw_index'] = image_input_df['sat_id'] +'_' + \
                                                                            image_input_df['encoded_strip_id'] +\
                                                                            '_' + image_input_df['tw_index'].astype(str)

    image_conflict_dict = get_conflict_dict_for_image(image_input_df)
    image_input_df['conflicting_strip_oppr'] = image_input_df['concat_sat_id_encoded_strip_id_tw_index'].map(image_conflict_dict)

    #========================================================================================================================================================================================================#========================================================================================================================================================================================================
    #========================================================================================================================================================================================================


    image_result_df = image_result_df[image_result_df['operation']=='Imaging']
    image_result_df['flag'] = 1
    image_result_df['base_timestamp'] = base_time_stamp
    image_result_df['base_timestamp'] = image_result_df['base_timestamp'].astype(str)
    image_result_df['base_timestamp'] = image_result_df['base_timestamp'].apply(lambda x: datetime.datetime.strptime(x,'%Y-%m-%d %H:%M:%S'))
    
    
    image_result_df['opportunity_start_time'] = image_result_df[['base_timestamp','start_time']].apply(lambda a: pd.to_datetime(a['base_timestamp']) + pd.DateOffset(seconds = a['start_time']),axis=1)
    image_result_df['opportunity_end_time'] = image_result_df[['base_timestamp','end_time']].apply(lambda a: pd.to_datetime(a['base_timestamp']) + pd.DateOffset(seconds = a['end_time']),axis=1)

    image_result_df['opportunity_end_time'] = image_result_df['opportunity_end_time'].astype(str)
    image_result_df['opportunity_start_time'] = image_result_df['opportunity_start_time'].astype(str)

    image_input_df['opportunity_end_time'] = image_input_df['opportunity_end_time'].astype(str)
    image_input_df['opportunity_start_time'] = image_input_df['opportunity_start_time'].astype(str)

    image_input_df_1 = pd.merge(image_input_df,image_result_df,on=['sat_id', 'strip_id', 'opportunity_start_time', 'opportunity_end_time','aoi_id','base_timestamp'],how='left')
    image_input_df_1['flag'] = image_input_df_1['flag'].fillna(0)
    
    #========================================================================================================================================================================================================
    #========================================================================================================================================================================================================
    extracted_raw_file_df= image_input_df_1[['sat_id', 'opportunity_start_time',
        'opportunity_end_time','strip_id', 'off_nadir','order_validity_start', 'order_validity_end','aoi_id','opportunity_start_offset',
        'opportunity_end_offset','normalized_local_priority_due_date',
        'normalized_local_priority_cc_based',
        'normalized_local_priority_off_nadir', 'normalized_global_priority',
        'normalized_total_priority','camera_memory_value_end_of_tw',
        'delta_camera_memory_value_in_this_tw','flag','flag_gs_pass_conflict','conflicting_strip_oppr','concat_sat_id_encoded_strip_id_tw_index','cloud_cover_limit','cloud_cover' ]]


    extracted_raw_file_df['encoded_strip_id'] = extracted_raw_file_df['strip_id'].astype(str)+ '_' + extracted_raw_file_df['aoi_id'].astype(str)
  
    #========================================================================================================================================================================================================
    #========================================================================================================================================================================================================
    extracted_raw_file_df_copy = extracted_raw_file_df.copy()
    extracted_raw_imaging_filtered_df = extracted_raw_file_df[extracted_raw_file_df['flag'] == 1]
    #========================================================================================================================================================================================================
    extracted_raw_imaging_filtered_df_copy = extracted_raw_imaging_filtered_df.copy()
    filtered_for_this_csjk_df = extracted_raw_imaging_filtered_df_copy[['concat_sat_id_encoded_strip_id_tw_index','conflicting_strip_oppr','opportunity_start_time','opportunity_end_time','strip_id',\
                                                           'normalized_global_priority','normalized_local_priority_due_date','normalized_total_priority']]
    filtered_for_this_csjk_df['conflicts_number'] =  filtered_for_this_csjk_df[['conflicting_strip_oppr']].apply(lambda a : len(a['conflicting_strip_oppr']),axis=1)

    filtered_for_this_csjk_df['entire_conflict_list'] =  filtered_for_this_csjk_df[['conflicting_strip_oppr','concat_sat_id_encoded_strip_id_tw_index']].\
        apply(lambda a : a['conflicting_strip_oppr']+[a['concat_sat_id_encoded_strip_id_tw_index']],axis=1)


    filtered_for_this_csjk_df = filtered_for_this_csjk_df.explode('entire_conflict_list')
    extracted_raw_file_df_copy.rename(columns = {"concat_sat_id_encoded_strip_id_tw_index":"entire_conflict_list"},inplace = True)
    filtered_for_this_csjk_df = filtered_for_this_csjk_df[['concat_sat_id_encoded_strip_id_tw_index','entire_conflict_list','conflicts_number']]

    conflict_strip_oppr_df = pd.merge(filtered_for_this_csjk_df,extracted_raw_file_df_copy,on='entire_conflict_list',how='left')

    conflict_strip_oppr_df.rename(columns = {"concat_sat_id_encoded_strip_id_tw_index":"conflic_strip_flag_named"},inplace = True)
   
    #========================================================================================================================================================================================================
    
    selected_for_merged_df = extracted_raw_file_df[['concat_sat_id_encoded_strip_id_tw_index','normalized_global_priority','normalized_local_priority_due_date','normalized_total_priority']].drop_duplicates()
    selected_for_merged_df.rename(columns={'normalized_global_priority':'this_flag_norm_GP',\
                                        "normalized_local_priority_due_date":'this_flag_norm_LLDD',\
                                            "normalized_total_priority":"this_flag_norm_TP",\
                                            "concat_sat_id_encoded_strip_id_tw_index":"conflic_strip_flag_named"},inplace=True)
    #========================================================================================================================================================================================================
    #========================================================================================================================================================================================================
    conflict_strip_oppr_df['max_Norm_GP'] = conflict_strip_oppr_df.groupby('conflic_strip_flag_named')['normalized_global_priority'].transform('max')
    conflict_strip_oppr_df['max_Norm_LPDD'] = conflict_strip_oppr_df.groupby('conflic_strip_flag_named')['normalized_local_priority_due_date'].transform('max')
    conflict_strip_oppr_df['max_Norm_TP'] = conflict_strip_oppr_df.groupby('conflic_strip_flag_named')['normalized_total_priority'].transform('max')
    conflict_strip_oppr_df_ = pd.merge(conflict_strip_oppr_df,selected_for_merged_df,on='conflic_strip_flag_named',how='left')
    conflict_strip_oppr_df_ = conflict_strip_oppr_df_[['conflic_strip_flag_named','max_Norm_TP','this_flag_norm_TP','max_Norm_GP','this_flag_norm_GP','max_Norm_LPDD','this_flag_norm_LLDD','conflicts_number']].drop_duplicates()
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

    criteria_list.append('Number_of_aoi_id')
    before_APS.append(extracted_raw_file_df['aoi_id'].nunique())
    APS_result.append(APS_output_raw_file_df['aoi_id'].nunique())
    flag_list.append("including_oppr_gs_pass_conflicts")
    #================================================================================================================================================
    conflictsFree_raw_file_df = extracted_raw_file_df[extracted_raw_file_df['flag_gs_pass_conflict']==0]
    APS_conflictsFree_raw_file_df = conflictsFree_raw_file_df[conflictsFree_raw_file_df['flag']==1]

    criteria_list.append('Number_of_Oppr')
    before_APS.append(len(conflictsFree_raw_file_df))
    APS_result.append(len(APS_conflictsFree_raw_file_df))
    flag_list.append("Excluding_oppr_gs_pass_conflicts")

    criteria_list.append('Number_of_Strips')
    before_APS.append(conflictsFree_raw_file_df['strip_id'].nunique())
    APS_result.append(APS_conflictsFree_raw_file_df['strip_id'].nunique())
    flag_list.append("Excluding_oppr_gs_pass_conflicts")

    criteria_list.append('Number_of_aoi_id')
    before_APS.append(conflictsFree_raw_file_df['aoi_id'].nunique())
    APS_result.append(APS_conflictsFree_raw_file_df['aoi_id'].nunique())
    flag_list.append("Excluding_oppr_gs_pass_conflicts")
    #================================================================================================================================================
    conflictsFree_raw_file_dd_df = conflictsFree_raw_file_df[['strip_id','aoi_id',\
                                                            'normalized_global_priority',\
                                                                'normalized_local_priority_due_date']].drop_duplicates()
    APS_conflictsFree_raw_file_dd_df = APS_conflictsFree_raw_file_df[['strip_id','aoi_id',\
                                                            'normalized_global_priority',\
                                                                'normalized_local_priority_due_date']].drop_duplicates()
    criteria_list.append('normalized_global_priority')
    before_APS.append(conflictsFree_raw_file_dd_df['normalized_global_priority'].sum())
    APS_result.append(APS_conflictsFree_raw_file_dd_df['normalized_global_priority'].sum())
    flag_list.append("Excluding_oppr_gs_pass_conflicts")

    criteria_list.append('normalized_local_priority_due_date')
    before_APS.append(conflictsFree_raw_file_dd_df['normalized_local_priority_due_date'].sum())
    APS_result.append(APS_conflictsFree_raw_file_dd_df['normalized_local_priority_due_date'].sum())
    flag_list.append("Excluding_oppr_gs_pass_conflicts")

    selected_NTP_conflictsFree_raw_file_df = conflictsFree_raw_file_df[['strip_id','aoi_id',\
                                                            'normalized_total_priority']]
    selected_NTP_conflictsFree_raw_grouped_file_df = selected_NTP_conflictsFree_raw_file_df.groupby(['strip_id','aoi_id']).agg(mean_normalized_total_priority = ('normalized_total_priority','mean')).reset_index()

    selected_NTP_APSconflictsFree_raw_file_df = APS_conflictsFree_raw_file_df[['strip_id','aoi_id',\
                                                            'normalized_total_priority']]
    selected_NTP_APSconflictsFree_raw_grouped_file_df = selected_NTP_APSconflictsFree_raw_file_df.groupby(['strip_id','aoi_id']).agg(mean_normalized_total_priority = ('normalized_total_priority','mean')).reset_index()

    criteria_list.append('Weighted_normalized_Total_priority')
    before_APS.append(selected_NTP_conflictsFree_raw_grouped_file_df['mean_normalized_total_priority'].sum())
    APS_result.append(selected_NTP_APSconflictsFree_raw_grouped_file_df['mean_normalized_total_priority'].sum())
    flag_list.append("Excluding_oppr_gs_pass_conflicts")

    #==========================================================================================================================================================================================================================================
    priority_based_result_list = ['normalized_global_priority','normalized_local_priority_due_date']

    df1 = conflictsFree_raw_file_df[['encoded_strip_id','normalized_global_priority','normalized_local_priority_due_date','flag']]
    df1.sort_values(by='flag',inplace = True, ascending =False)
    df1.drop_duplicates(subset = ['encoded_strip_id','normalized_global_priority','normalized_local_priority_due_date'],keep = 'first',inplace =True)

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
        x = len(d1['normalized_global_priority'+'_'+str(i)]['stripID_set_before'].intersection(d1['normalized_local_priority_due_date'+'_'+str(i)]['stripID_set_before']))
        before_APS.append(x)
        y = len(d1['normalized_global_priority'+'_'+str(i)]['stripID_set_after'].intersection(d1['normalized_local_priority_due_date'+'_'+str(i)]['stripID_set_after']))
        APS_result.append(y)

        flag_list.append("Excluding_oppr_gs_pass_conflicts")


    KPI_df = pd.DataFrame({'criteria':criteria_list,'before_APS':before_APS,'APS_result':APS_result,'remarks':flag_list})
    KPI_df['percentage'] = KPI_df['APS_result']/KPI_df['before_APS']*100
    #========================================================================================================================================================================================================
    #========================================================================================================================================================================================================

    return {"interpret_extracted_raw_file_df":extracted_raw_file_df,\
            "interpret_selected_oppr_conflict_comparision_df":conflict_strip_oppr_df_,\
            "interpret_KPI_df":KPI_df}
