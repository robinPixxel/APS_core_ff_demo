import warnings
warnings.filterwarnings('ignore')

import pandas as pd
import json
import math

from datetime import datetime as dt
import os
# from APS_Python_core.preprocess_1.preprocess_GSpassSelecion import GSPassPreprocess
# from model_3.MILP_GSpassSelection_v3 import GSpassSelection
# from postprocess_4.postprocess_GSpassSelection import GSpasspostprocess

# from preprocess_1.preprocess_imageAqusuition_test import ImageAquisitionProcess #preprocess_imageAqusuition_test,preprocess_imageAquisition_v3_18112024
# from model_3.MILP_imageCapture_v3_17112024_copy import ImageCapturePlan # MILP_imageCapture_v2_16102024,MILP_imageCapture_v2_25102024 # MILP_imageCapture_v2_07112024 #MILP_imageCapture_v3_17112024
# from postprocess_4.image_capture_postprocess_V3_17112024 import ImagecapturePostProcess # image_capture_postprocess_V3_17112024# image_capture_postprocess_v2_18102024

# from preprocess_1.preprocess_downlink_WIP import DownlinkingPreProcess
# from model_3.MILP_downlink import ImageDownlinkPlan
# from postprocess_4.postprocess_downlink import ImageDownlinkPostProcess

# from result_interpret import interpret_result
# from utils import *

from APS_Python_core.preprocess_1.preprocess_GSpassSelecion import GSPassPreprocess
from APS_Python_core.model_3.MILP_GSpassSelection_v3 import GSpassSelection
from APS_Python_core.postprocess_4.postprocess_GSpassSelection import GSpasspostprocess

from APS_Python_core.preprocess_1.preprocess_imageAqusuition_test import ImageAquisitionProcess #preprocess_imageAqusuition_test,preprocess_imageAquisition_v3_18112024
from APS_Python_core.model_3.MILP_imageCapture_v3_17112024 import ImageCapturePlan # MILP_imageCapture_v3_17112024_copy#MILP_imageCapture_v2_16102024,MILP_imageCapture_v2_25102024 # MILP_imageCapture_v2_07112024 #MILP_imageCapture_v3_17112024# 
from APS_Python_core.postprocess_4.image_capture_postprocess_V3_17112024 import ImagecapturePostProcess # image_capture_postprocess_V3_17112024# image_capture_postprocess_v2_18102024

from APS_Python_core.preprocess_1.preprocess_downlink_WIP import DownlinkingPreProcess
from APS_Python_core.model_3.MILP_downlink import ImageDownlinkPlan
from APS_Python_core.postprocess_4.postprocess_downlink import ImageDownlinkPostProcess

from APS_Python_core.result_interpret import interpret_result
from APS_Python_core.utils import *

# script_dir = os.path.abspath( os.path.dirname( __file__ ) )
# print("script directory: ",script_dir)

def select_gs_pass_oppr(GS_pass_df,config):
    ''' 
    this function selects gs pass based on basic constraints.
    Input->Gs_pass_df['GsID', 'AOS', 'LOS', 'SatID', 'AOSOffset', 'LOSOffset']
           config->{"constraints":{"Thermal_constraints_GS_pass":false}
    Output->df[SatID,gsID,concat_sat_gs_k,start_time,end_time,TW_index,AOSoffset,LOSoffset,duration]

    '''

    obj_preprocess = GSPassPreprocess(GS_pass_df,config)
    data = obj_preprocess.preprocess()
    #print(data['SG1K1G2K2_pair']['domain_of_csgk'])
    print("optimization_model_starts")
    obj_model = GSpassSelection(data,config)
    result,thermal_profile_gsPass = GSpasspostprocess(obj_model,data,config).get_gsPasses()# 21 seconds

    try :
        result['duration'] = result['end_time'] - result['start_time']
        result = result[result['duration']> 0]
    except:
        print("model is not converged or infeasible or not solved")

    return result
    

def select_img_opprtunity(image_opportunity_df,gs_pass_result_df,eclipse_df_dict,config):
    ''' 
    this function selects strip orpprtunity based on memory power thermal and varous priorities..
    Input->gs_pass_result_df-->df[SatID,gsID,concat_sat_gs_k,start_time,end_time,TW_index,AOSoffset,LOSoffset,duration]
           image_opportunity_df-->df[SatID','StripID','AoiID','OpportunityStartTime','OpportunityEndTime','OpportunityStartOffset','OpportunityEndOffset','OrderValidityEnd','CloudCover','OffNadir']
           config->dict({"constraints":{...},
                    "objective":{"GS_Pass_and_Imaging":true,"total_readout_memory":false},
                    "downlink_schedule_OnlyJustsortImgID":true,
                    "GP_weight":0.4,
                    "DDLP_weight":0.2,
                    "CCLP_weight":0.1,
                    "ONLP_weight":0.3,
                    "min_readout_time_seconds":50})

           eclipse_df_dict-->dict{'sat':df['time_index','SatID','eclipse']}

    Output->img_capture_result--> df[SatID,StripID,AoiID,encoded_strip_id,start_time,end_time,gsID,operation,\
                               camera_memory_value_endofTW,delta_camera_memory_value_in_this_TW,SSD_memory_value_endofTW,\
                                delta_SSD_memory_value_in_this_TW,global_priority,local_priority,mean_global_priority,mean_local_priority]

            data--> dict(['encoded_stripId_list', 'imgery_sat_id_list', 'only_gs_sat_id_list', 'unique_img_opportunities_list', 'TW__csjk', \
                          'csjkList__j', 'csjkSet__s', 'cs1j2k2Domainlist__cs1j1k1', 'camera_memory_capacity__s', 'initial_camera_memory_value__s',\
                          'readout_memory_capacity__s', 'power_capacity__s', 'initial_power_value__s', 'initial_readout_camera_memory_value__s', \
                        'imaging_rate', 'Readout_rate', 'heatCameraTimeBucket_SCT_dict__s', 'heatTimeBucket_SCT_dict__s',\
                        'max_camera_heat_dict', 'max_readout_heat_dict', 'stripid__encodedstripID', 'AOIid__encodedstripID', \
                        'assured_tasking_basedOnDueDateEmergency_list', 'assured_tasking_based_on_input_list', \
                        'GlobalPriority__csjk', 'TotalPriority__csjk', 'Local_Priority__csjk', 'TotalPriority__csj', \
                        'GlobalPriority__csj', 'Local_Priority__csj', 'active_assured_strip_id_list', \
                        'PowerglobalTWindexSortedList__s', 'MemoryglobalTWindexSortedList__s', 'Power_GS_TW_list',\
                        'Memory_onlyGsTW_list', 'Power_image_TW_list', 'Memory_onlyImgTW_list', 'Power_NoimageGs_TW_list', \
                        'Memory_NoimageGs_TW_list', 'prev_tWList__s_TWI_dict__s', 'prev_ImagingTWList__s_TWI_dict__s', \
                        'prev_power_tWList__s_TWI_dict__s', 'TW_df_withoutEclipseDivide_df', 'TW_df_withEclipseDivide_df',\
                        'dedicatedReadoutTWlist__concat_sat_memoryTWindex', 'power_based_df', 'success_metric_before', \
                        'TW__csn', 'memory_based_df', 'dedicated_readout_df', 'dedicatedReadoutTWIndex__sat', \
                        'prev_dedicatedReadoutIndex__s_TWI_dict__s', 'DROPriority__concat_sat_memoryTWindex', 'GS_Pass_time_objective']))

    '''

    #basic flters
    #image_opportunity_df = image_opportunity_df[image_opportunity_df['OpportunityEndOffset']<config['scheduled_Hrs']*3600]
    #image_opportunity_df = image_opportunity_df[image_opportunity_df['CloudCoverLimit']>image_opportunity_df['CloudCover']]
    #image_opportunity_df = image_opportunity_df[image_opportunity_df['OffNadirLimit']>image_opportunity_df['OffNadir']]

    obj_preprocess = ImageAquisitionProcess(image_opportunity_df,gs_pass_result_df,eclipse_df_dict,config)
    data = obj_preprocess.preprocess()
    #print(data['cs1j2k2Domainlist__cs1j1k1'])

    #++++++++++++++++++++++++++  STEP 0  +++++++++++++++++++++++++++++++++++++++++++++++
    '''
    hard code some data
    '''
    data['camera_memory_capacity__s'] = {s:v for s,v in data['camera_memory_capacity__s'].items() }
    data['readout_memory_capacity__s'] = {s:v for s,v in data['readout_memory_capacity__s'].items() }
    data['power_capacity__s']  = {s:720000000 for s,v in data['power_capacity__s'].items() }
    data['initial_power_value__s']  = {s:v*0.3 for s,v in data['power_capacity__s'].items() }
    #++++++++++++++++++++++++++  STEP 1  +++++++++++++++++++++++++++++++++++++++++++++++
    config['objective']['GS_Pass_and_Imaging'] = True
    config['objective']['total_readout_memory'] = False
    obj_model = ImageCapturePlan(data,config)

    #Readout Schedule 
    data['GS_Pass_time_objective'] = obj_model.prob.objective.value()
    config['objective']['GS_Pass_and_Imaging'] = False
    config['objective']['total_readout_memory'] = True
    obj_model = ImageCapturePlan(data,config)

    #++++++++++++++++++++++++++  PostProcess  +++++++++++++++++++++++++++++++++++++++++++++++
    post_obj = ImagecapturePostProcess(obj_model,data)
    img_capture_result= post_obj.get_schedule()
    #.isnull().sum()
    return img_capture_result,data
    #======================================================================================================================================================================================================

    pass

def get_aps_success_metric(img_capture_result,data):
    after_aps_plan_df = img_capture_result[img_capture_result['operation']=='Imaging']
    criteria_list = ['total_opprtunities_ratio','GP_ratio','LP_ratio','conflictImg_gsPass']

    total_conflict_images_list= [data['success_metric_before']['conflict_images']]
    GP_before = data['success_metric_before']['original_Total_GP']
    LP_before = data['success_metric_before']['original_Total_LP']
    TOppr_before = data['success_metric_before']['total_opportunities'] 
    before_list = [TOppr_before,GP_before,LP_before,total_conflict_images_list]

    GP_after= after_aps_plan_df['mean_global_priority'].sum()
    LP_after = after_aps_plan_df['mean_local_priority'].sum()
    TOppr_after= after_aps_plan_df['encoded_strip_id'].nunique()
    fraction_conflict_images_list = [list(after_aps_plan_df[after_aps_plan_df['encoded_strip_id'].isin(total_conflict_images_list)]['encoded_strip_id'].unique())]
    after_list = [TOppr_after,GP_after,LP_after,fraction_conflict_images_list]

    APS_success_metric_df = pd.DataFrame({'criteria':criteria_list,'potential_input':before_list,'APS_selected':after_list})
    #APS_success_metric_df['percentage'] = APS_success_metric_df['APS_selected'] / APS_success_metric_df['potential_input']
    APS_success_metric_df1 = APS_success_metric_df[:-1]
    APS_success_metric_df1['percentage'] = APS_success_metric_df1['APS_selected'] / APS_success_metric_df1['potential_input'] * 100
    APS_success_metric_df2 = APS_success_metric_df[-1:]

    APS_success_metric_df = pd.concat([APS_success_metric_df1,APS_success_metric_df2])

    return APS_success_metric_df
    

def get_downlink_schedule(image_downlink_df,img_capture_result,config):

    downlink_operation_list  = ['downlinking_from_camera','downlinking_from_Readout']
    img_capture_result_downlink = img_capture_result[img_capture_result['operation'].isin(downlink_operation_list)]
    DownlinkingPreProcessObj = DownlinkingPreProcess(image_downlink_df,img_capture_result_downlink,config)
    data_downlink = DownlinkingPreProcessObj.preprocess()
    
    if config['downlink_schedule_OnlyJustsortImgID']:
        downlink_result = pd.DataFrame(data_downlink['LP_DD_Priority_imgID'].items(),columns=['ImageID','computed_priority']).\
            sort_values(by='computed_priority',ascending=False)
    else:
        obj_downlink_model = ImageDownlinkPlan(data_downlink,config)
        downlink_result = ImageDownlinkPostProcess(obj_downlink_model,data_downlink).get_schedule()
        downlink_result = downlink_result[downlink_result['TileStripNo_downLoad']!=0]

    return downlink_result
    


def get_input_files(config,GS_pass_df,image_opportunity_df,image_downlink_df,eclipse_event_df):
    ''' 
    to read input files and preliminary preprocessing.
    Input-->
            GS_pass_df-->df['GsID', 'AOS', 'LOS', 'Eclipse', 'AOSOffset', 'LOSOffset','SatID']
            image_opportunity_df-->df['SatID', 'OpportunityStartTime',
                'OpportunityEndTime', 'StripID', 'OffNadir', 'SunInView', 'EarthInView',
                'MoonInView','OrderValidityStart', 'OrderValidityEnd', 'AoiID','CloudCoverLimit', 'CloudCover',
                'OffNadirLimit', 'Priority','OpportunityStartOffset',
                'OpportunityEndOffset']
            image_downlink_df-->df['ImageID', 'SatID', 'DueDate', 'Priority', 'Tilestrips', 'Sensors',
                                   'Bands', 'EmergencyFlag', 'CaptureDate',delivery_type,assured_downlink_flag]
    Output-->dict('GS_pass_df':df[..],\
                'image_opportunity_df':df[..],\
                'image_downlink_df':df[..],\
                "eclipse_df_dict": {'sat':df ,..},
                "config":{..})
    '''
    # GS PASS
    #GS_pass_df = pd.read_csv(config["csv_file_path"]["gs_pass_opportunity"])#APS_gsPasses_TV1#GS_Passes_mock1#GS_Passes_live1#GS_Passes_new (1)
    
    #GS_pass_df = pd.read_csv("1_input_data/GS_Passes_new (1).csv")
    GS_pass_df_original = GS_pass_df.copy()
    GS_pass_df['SatID'] = GS_pass_df['SatID'].astype(str)
    GS_pass_df['AOSOffset'] = GS_pass_df['AOSOffset'].astype(int)
    GS_pass_df['LOSOffset'] = GS_pass_df['LOSOffset'].astype(int)

    # image Opprtunity
    #image_opportunity_df = pd.read_csv(config["csv_file_path"]["image_capture_opportunity"])#Imaging_mock1#APS_imagingOpportunities_TV1#Imaging_live#Imaging_new (1)
    # change made priority ulta
    #image_opportunity_df['Priority'] = 1/image_opportunity_df['Priority']
    
    image_opportunity_df['SatID'] = image_opportunity_df['SatID'].astype(str)
    image_opportunity_df['OpportunityStartOffset'] = image_opportunity_df['OpportunityStartOffset'].astype(int)
    image_opportunity_df['OpportunityEndOffset'] = image_opportunity_df['OpportunityEndOffset'].astype(int)
    image_opportunity_df_copy = image_opportunity_df.copy()
    image_opportunity_df_copy['X'] = image_opportunity_df_copy[['OpportunityStartTime','OpportunityStartOffset']].apply(lambda a: pd.to_datetime(a['OpportunityStartTime']) - pd.DateOffset(seconds=a['OpportunityStartOffset']),axis=1)
    image_opportunity_df_copy['Y'] = image_opportunity_df_copy[['OpportunityEndTime','OpportunityEndOffset']].apply(lambda a: pd.to_datetime(a['OpportunityEndTime']) - pd.DateOffset(seconds=a['OpportunityEndOffset']),axis=1)
    base_time_stamp = image_opportunity_df_copy["X"].to_list()[0]
    config['base_time_stamp_downlink'] = base_time_stamp

    #image Downlink
    #image_downlink_df = pd.read_csv(config["csv_file_path"]["image_downlink_file"])
    image_downlink_df['assured_downlink_flag'] = [0,0] +[0]*(len(image_downlink_df)-2)
    image_downlink_df['delivery_type'] = 'standard_delivery' # expedited_delivery,super_expedited_delivery
    union_list_of_sat = list(set(image_opportunity_df['SatID']).union(set(GS_pass_df['SatID'])).union(set(image_downlink_df['SatID'])))
    hrs = config['scheduled_Hrs']

    # get dummy eclipse data close to reality
    # satellite_list = eclipse_event_df['SatID'].unique()
    # eclipse_df = pd.DataFrame()
    # for sat in satellite_list:
    #     this_eclipse_df = eclipse_event_df[eclipse_event_df['SatID']==sat]
    #     that_eclipse_df = get_eclipse_data(this_eclipse_df,config)
    #     eclipse_df = pd.concat([that_eclipse_df,eclipse_df])
        
    # min_time_index= min([image_opportunity_df['OpportunityStartOffset'].min(),image_opportunity_df['OpportunityEndOffset'].max(),GS_pass_df['AOSOffset'].min(),GS_pass_df['LOSOffset'].max()])
    # max_time_index= max([image_opportunity_df['OpportunityStartOffset'].min(),image_opportunity_df['OpportunityEndOffset'].max(),GS_pass_df['AOSOffset'].min(),GS_pass_df['LOSOffset'].max()])

    # hrs = (max_time_index - min_time_index)/3600
    # hrs = math.ceil(hrs)
    # while True:
    #     hrs += 1
    #     if hrs % 1.5==0:
    #         break


    # in_orbit_eclipse_event = [1 for i in range(int(1.5*3600*0.4))] + [0 for i in range(int(1.5*3600*0.6))] #
    # eclipse_df  = pd.DataFrame({'time_index': [i for i in range(min_time_index,min_time_index+hrs*3600)] ,"eclipse" : in_orbit_eclipse_event*int(hrs/1.5)})
    # eclipse_df['SatID']= [union_list_of_sat] *len(eclipse_df)
    # eclipse_df = eclipse_df.explode('SatID')
    # eclipse_df_dict = {s: eclipse_df[eclipse_df['SatID']==s] for s in eclipse_df['SatID'].unique()}
    try :
        eclipse_df = pd.DataFrame()
        satellite_list = eclipse_event_df['SatID'].unique()
        for sat in satellite_list:
            this_eclipse_df = eclipse_event_df[eclipse_event_df['SatID']==sat]
            that_eclipse_df = get_eclipse_data(this_eclipse_df,config)
            eclipse_df = pd.concat([that_eclipse_df,eclipse_df])
        eclipse_df_dict = {s: eclipse_df[eclipse_df['SatID']==s] for s in eclipse_df['SatID'].unique()}
    except:
        print("some_error_in_eclipse_data_So_hard_coded_eclipse_data")
        min_time_index= min([image_opportunity_df['OpportunityStartOffset'].min(),image_opportunity_df['OpportunityEndOffset'].max(),GS_pass_df['AOSOffset'].min(),GS_pass_df['LOSOffset'].max()])
        max_time_index= max([image_opportunity_df['OpportunityStartOffset'].min(),image_opportunity_df['OpportunityEndOffset'].max(),GS_pass_df['AOSOffset'].min(),GS_pass_df['LOSOffset'].max()])

        hrs = (max_time_index - min_time_index)/3600
        hrs = math.ceil(hrs)
        while True:
            hrs += 1
            if hrs % 1.5==0:
                break


        in_orbit_eclipse_event = [1 for i in range(int(1.5*3600*0.4))] + [0 for i in range(int(1.5*3600*0.6))] #
        eclipse_df  = pd.DataFrame({'time_index': [i for i in range(min_time_index,min_time_index+hrs*3600)] ,"eclipse" : in_orbit_eclipse_event*int(hrs/1.5)})
        eclipse_df['SatID']= [union_list_of_sat] *len(eclipse_df)
        eclipse_df = eclipse_df.explode('SatID')
        eclipse_df_dict = {s: eclipse_df[eclipse_df['SatID']==s] for s in eclipse_df['SatID'].unique()}


    # get dummy data for assured tasking
    image_opportunity_df['encoded_stripId'] =   image_opportunity_df['StripID'].astype(str)+ '_' + image_opportunity_df['AoiID'].astype(str)
    total_capture_list = list(image_opportunity_df['encoded_stripId'].unique())
    no_of_list = len(total_capture_list)
    assured_capture_df = pd.DataFrame({'encoded_stripId':total_capture_list,'assured_task':[0,0]+[0]*(no_of_list-2)})
    image_opportunity_df = pd.merge(image_opportunity_df,assured_capture_df,on='encoded_stripId',how='left')
    image_opportunity_df = image_opportunity_df.drop(columns=['encoded_stripId'])

    # further processing eclipse data to align with gs pass where entire gs pass is assumed to be in eclipse region
    gsPassInput_df_copy = GS_pass_df_original
    gsPassInput_df_copy['SatID'] = gsPassInput_df_copy['SatID'].astype(str)
    gsPassInput_df_copy['AOSOffset'] = gsPassInput_df_copy['AOSOffset'].astype(int)
    gsPassInput_df_copy['LOSOffset'] = gsPassInput_df_copy['LOSOffset'].astype(int)
    gsPassInput_df_copy['list'] =  gsPassInput_df_copy[['AOSOffset','LOSOffset']].apply(lambda a : [i for i in range(a['AOSOffset'],a['LOSOffset']+1)],axis =1 )

    gsPassInput_df_copy1 = gsPassInput_df_copy[['SatID','list']]
    gsPassInput_df_copy1 = gsPassInput_df_copy1.explode('list')
    gsPassInput_df_grouped_copy1 = gsPassInput_df_copy1.groupby('SatID').agg(time_index_list = ('list',list)).reset_index()
    gsPasstimeIndexList__s = dict(zip(gsPassInput_df_grouped_copy1['SatID'],gsPassInput_df_grouped_copy1['time_index_list']))
    for k,v in eclipse_df_dict.items():
        if k in gsPasstimeIndexList__s.keys():
            this_time_index_list = gsPasstimeIndexList__s[k]
            v.loc[v["time_index"].isin(this_time_index_list), "eclipse"] = 1
            eclipse_df_dict[k] = v

    return {
            'GS_pass_df':GS_pass_df,\
            'image_opportunity_df':image_opportunity_df,\
            'image_downlink_df':image_downlink_df,\
            "eclipse_df_dict": eclipse_df_dict,
            "config":config
            }
    
def get_schedule(config,GS_pass_df,image_opportunity_df,image_downlink_df,eclipse_df):
    ''' 
    to get imaging , readout schedule and stats for schedule.

    Input-->
            GS_pass_df-->df['GsID', 'AOS', 'LOS', 'Eclipse', 'AOSOffset', 'LOSOffset','SatID']
            image_opportunity_df-->df['SatID', 'OpportunityStartTime',
                'OpportunityEndTime', 'StripID', 'OffNadir', 'SunInView', 'EarthInView',
                'MoonInView','OrderValidityStart', 'OrderValidityEnd', 'AoiID','CloudCoverLimit', 'CloudCover',
                'OffNadirLimit', 'Priority','OpportunityStartOffset',
                'OpportunityEndOffset']
            image_downlink_df-->df['ImageID', 'SatID', 'DueDate', 'Priority', 'Tilestrips', 'Sensors',
                                   'Bands', 'EmergencyFlag', 'CaptureDate',delivery_type,assured_downlink_flag]

    Output-->dict{"only_readout_result":df['SatID','start_time','end_time','base_time'],\
        "only_img_capture_result":df['SatID','start_time','end_time','AoiID','StripID','base_time'],\
            "only_gsPass_result":df['SatID','start_time','end_time','gsID','base_time'],\
            "combined_result":df['SatID', 'start_time', 'end_time', 'encoded_strip_id', 'gsID','operation', 'camera_memory_value_endofTW',
                                'delta_camera_memory_value_in_this_TW', 'SSD_memory_value_endofTW','delta_SSD_memory_value_in_this_TW', 'global_priority',
                                'local_priority', 'mean_global_priority', 'mean_local_priority','StripID', 'AoiID', 'base_time']}

            "interpret_extracted_raw_file_df":df['SatID', 'OpportunityStartTime', 'OpportunityEndTime', 'StripID','OffNadir', 'OrderValidityStart', 'OrderValidityEnd', 'AoiID','Priority', 'OpportunityStartOffset', 'OpportunityEndOffset',
                                                'normalized_local_priority_due_date','normalized_local_priority_CC_based','normalized_local_priority_offNadir', 'normalized_GlobalPriority','normalized_Total_Priority', 'camera_memory_value_endofTW',
                                                'delta_camera_memory_value_in_this_TW', 'flag', 'flag_gs_pass_conflict','conflicting_strip_oppr', 'concat_SatID_encodedStripId_TWindex','CloudCoverLimit', 'CloudCover', 'encoded_strip_id', 'base_time']
            "interpret_selected_oppr_conflict_comparision_df"df['conflic_strip_flag_named', 'max_Norm_TP', 'this_flag_norm_TP','max_Norm_GP', 'this_flag_norm_GP', 'max_Norm_LPDD','this_flag_norm_LLDD', 'base_time']
            "interpret_KPI_df: df['criteria', 'before_APS', 'APS_result', 'remarks', 'percentage','base_time']
            
                                                   
    '''
    
    # Open and read the JSON file
    #APS_Python_core/src/APS_Python_core/1_input_data/config.json
    #with open('APS_Python_core/src/APS_Python_core/1_input_data/config.json', 'r') as file:
    #with open('../1_input_data/config.json', 'r') as file:
        #config = json.load(file)
    original_image_opportunity_df = image_opportunity_df.copy()
    # if memory constraint False then thermal_constraint is also False
    #config['constraints']['thermal_constraint_readout'] = config['constraints']['memory_constrant'] and config['constraints']['thermal_constraint_readout']
    #config['constraints']['thermal_constraint_imaging'] = config['constraints']['memory_constrant'] and config['constraints']['thermal_constraint_imaging']

    #======================================================================================================================================================================================================
    # read_input
    input_dict = get_input_files(config,GS_pass_df,image_opportunity_df,image_downlink_df,eclipse_df)
    config = input_dict['config']
    #======================================================================================================================================================================================================
    #gs pass_selection
    gs_pass_result_df = select_gs_pass_oppr(input_dict['GS_pass_df'],config)
    gs_pass_result_df['Eclipse'] = 1 ## dummy
    gs_pass_result_df['duration'] = gs_pass_result_df['end_time'] - gs_pass_result_df['start_time']
    gs_pass_result_df = gs_pass_result_df[gs_pass_result_df['duration']> 0]
    interpret_gs_pass_result_df_copy = gs_pass_result_df.copy()# this not the gsPass result as it is to be get filtered after due to other factors in image capture plan.It is just to get require info in interpret result.

    print("image_capture_plan_starting")
    #======================================================================================================================================================================================================
    #image_selection
    img_capture_result,capture_plan_data_input= select_img_opprtunity(input_dict['image_opportunity_df'],gs_pass_result_df,input_dict['eclipse_df_dict'],config)
    img_capture_result['base_time'] = config['base_time_stamp_downlink']
    #img_capture_result = img_capture_result[img_capture_result['operation']=='Imaging']
    #readout_result = img_capture_result[img_capture_result['operation']=='Readout']
    interpret_img_capture_resul_copy = img_capture_result.copy()
    #======================================================================================================================================================================================================
    # get APS success metrics 
    APS_success_metric_df = get_aps_success_metric(img_capture_result,capture_plan_data_input)
    #======================================================================================================================================================================================================
    print("Downlink_plan_starting")
    try:
        downlink_result = get_downlink_schedule(input_dict['image_downlink_df'],img_capture_result,config)
        downlink_result['base_time'] = config['base_time_stamp_downlink']
    except:
        print("downlink_schedule_error")
    #======================================================================================================================================================================================================
    #img_capture_result[img_capture_result['download_from_']]
    # gs_pass_result_df.to_csv("APS_Python_core/src/APS_Python_core/5_output_data/gs_pass_result_df.csv",index=None)
    # img_capture_result.to_csv("APS_Python_core/src/APS_Python_core/5_output_data/img_capture_schedule.csv",index=None)
    # APS_success_metric_df.to_csv("APS_Python_core/src/APS_Python_core/5_output_data/APS_success_metric.csv",index = None)
    # downlink_result.to_csv("APS_Python_core/src/APS_Python_core/5_output_data/downlink_result.csv",index = None)

    interpret_image_opportunity_df = original_image_opportunity_df
    interpret_result_dict = interpret_result(interpret_image_opportunity_df,interpret_gs_pass_result_df_copy,interpret_img_capture_resul_copy,config)
    for k,v in interpret_result_dict.items():
        v['base_time'] = config['base_time_stamp_downlink']
        #v.to_csv("APS_Python_core/src/APS_Python_core/5_output_data/"+k+".csv",index = None)
    
    only_img_capture_result = img_capture_result[img_capture_result['operation']=='Imaging'][['SatID','start_time','end_time','AoiID','StripID','base_time']]
    only_img_capture_result['start_time'] = only_img_capture_result[['start_time','base_time']].apply(lambda a: pd.to_datetime(a['base_time']) + pd.DateOffset(seconds=a['start_time']),axis=1)
    only_img_capture_result['end_time'] = only_img_capture_result[['end_time','base_time']].apply(lambda a: pd.to_datetime(a['base_time']) + pd.DateOffset(seconds=a['end_time']),axis=1)

    only_readout_result = img_capture_result[img_capture_result['operation']=='Readout'][['SatID','start_time','end_time','base_time']]
    
    if len(only_readout_result):
        only_readout_result['start_time'] = only_readout_result[['start_time','base_time']].apply(lambda a: pd.to_datetime(a['base_time']) + pd.DateOffset(seconds=a['start_time']),axis=1)
        only_readout_result['end_time'] = only_readout_result[['end_time','base_time']].apply(lambda a: pd.to_datetime(a['base_time']) + pd.DateOffset(seconds=a['end_time']),axis=1)

    only_gsPass_result = img_capture_result[img_capture_result['operation']=='downlinking_from_Readout'][['SatID','start_time','end_time','gsID','base_time']]
    only_gsPass_result['start_time'] = only_gsPass_result[['start_time','base_time']].apply(lambda a: pd.to_datetime(a['base_time']) + pd.DateOffset(seconds=a['start_time']),axis=1)
    only_gsPass_result['end_time'] = only_gsPass_result[['end_time','base_time']].apply(lambda a: pd.to_datetime(a['base_time']) + pd.DateOffset(seconds=a['end_time']),axis=1)


    result_dict = {"only_readout_result":only_readout_result,\
                  "only_img_capture_result":only_img_capture_result,\
                  "only_gsPass_result":only_gsPass_result,\
                  "combined_result":img_capture_result}
    result_dict.update(interpret_result_dict)
    #return result_dict
    return result_dict
