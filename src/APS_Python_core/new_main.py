import warnings
import logging
from time import time
warnings.filterwarnings('ignore')

import pandas as pd
import plotly.graph_objects as go
import math

from APS_Python_core.preprocess_1.preprocess_system_parameters import systemReqPreprocess

from APS_Python_core.preprocess_1.preprocess_GSpassSelecion import GSPassPreprocess
from APS_Python_core.model_3.MILP_GSpassSelection_v3 import GSpassSelection
from APS_Python_core.postprocess_4.postprocess_GSpassSelection import GSpasspostprocess

from APS_Python_core.preprocess_1.preprocess_imageAqusuition_test import ImageAquisitionProcess #preprocess_imageAqusuition_test,preprocess_imageAquisition_v3_18112024
from APS_Python_core.model_3.MILP_imageCapture_v3_29012025_WIP import ImageCapturePlan # MILP_imageCapture_v3_17112024_copy#MILP_imageCapture_v2_16102024,MILP_imageCapture_v2_25102024 # MILP_imageCapture_v2_07112024 #MILP_imageCapture_v3_17112024# 
#from APS_Python_core.model_3.MILP_imageCapture_v3_17112024 import ImageCapturePlan # MILP_imageCapture_v3_17112024_copy#MILP_imageCapture_v2_16102024,MILP_imageCapture_v2_25102024 # MILP_imageCapture_v2_07112024 #MILP_imageCapture_v3_17112024# 
from APS_Python_core.postprocess_4.image_capture_postprocess_V3_17112024 import ImagecapturePostProcess # image_capture_postprocess_V3_17112024# image_capture_postprocess_v2_18102024

from APS_Python_core.preprocess_1.preprocess_downlink_WIP import DownlinkingPreProcess
from APS_Python_core.model_3.MILP_downlink import ImageDownlinkPlan
from APS_Python_core.postprocess_4.postprocess_downlink import ImageDownlinkPostProcess

from APS_Python_core.result_interpret import interpret_result
from APS_Python_core.utils import *

from APS_Python_core.plots import *

# script_dir = os.path.abspath( os.path.dirname( __file__ ) )
# print("script directory: ",script_dir)




def select_gs_pass_oppr(GS_pass_df,config,system_requirment_input_dict,logger):
    ''' 
    this function selects gs pass based on basic constraints.
    Input->Gs_pass_df['GsID', 'AOS', 'LOS', 'sat_id', 'aos_offset', 'los_offset']
           config->{"constraints":{"Thermal_constraints_GS_pass":false}
    Output->df[sat_id,gsID,concat_sat_gs_k,start_time,end_time,TW_index,aos_offset,los_offset,duration]

    '''

    obj_preprocess = GSPassPreprocess(GS_pass_df,config,system_requirment_input_dict,logger)
    logger.info("gs_Pass_Preprocess_start")
    st_time = time()
    data = obj_preprocess.preprocess()
    logger.info("gs_Pass_Preprocessing_time_taken="+str(time()-st_time))
    st_time = time()
    logger.info("gs_Pass_optimization_model_starts")
    obj_model = GSpassSelection(data,config,logger)
    logger.info("gs_Pass_optimization_model_time_taken="+str(time()-st_time))
    result = GSpasspostprocess(obj_model,data,config).get_gsPasses()# 21 seconds

    try :
        result['duration'] = result['end_time'] - result['start_time']
        result = result[result['duration']> 0]
    except:
        logger.error("model is not converged or infeasible or not solved")
        #print("model is not converged or infeasible or not solved")

    return result
    

def select_img_opprtunity(image_opportunity_df,gs_pass_result_df,eclipse_df_dict,config,system_requirment_input_dict,logger):
    ''' 
    this function selects strip orpprtunity based on memory power thermal and varous priorities..
    Input->gs_pass_result_df-->df[sat_id,gsID,concat_sat_gs_k,start_time,end_time,TW_index,aos_offset,los_offset,duration]
           image_opportunity_df-->df[sat_id','StripID','AoiID','opportunity_start_time','opportunity_end_time','opportunity_start_offset','opportunity_end_offset','OrderValidityEnd','CloudCover','OffNadir']
           config->dict({"constraints":{...},
                    "objective":{"GS_Pass_and_Imaging":true,"total_readout_memory":false},
                    "downlink_schedule_OnlyJustsortImgID":true,
                    "GP_weight":0.4,
                    "DDLP_weight":0.2,
                    "CCLP_weight":0.1,
                    "ONLP_weight":0.3,
                    "min_readout_time_seconds":50})

           eclipse_df_dict-->dict{'sat':df['time_index','sat_id','eclipse']}

    Output->img_capture_result--> df[sat_id,StripID,AoiID,encoded_strip_id,start_time,end_time,gsID,operation,\
                               camera_memory_value_endofTW,delta_camera_memory_value_in_this_TW,SSD_memory_value_endofTW,\
                                delta_SSD_memory_value_in_this_TW,global_priority,local_priority,mean_global_priority,mean_local_priority]
            data -->dict(....)
           
    '''

    #basic flters
    #image_opportunity_df = image_opportunity_df[image_opportunity_df['opportunity_end_offset']<config['scheduled_Hrs']*3600]
    #image_opportunity_df = image_opportunity_df[image_opportunity_df['CloudCoverLimit']>image_opportunity_df['CloudCover']]
    #image_opportunity_df = image_opportunity_df[image_opportunity_df['OffNadirLimit']>image_opportunity_df['OffNadir']]

    obj_preprocess = ImageAquisitionProcess(image_opportunity_df,gs_pass_result_df,eclipse_df_dict,config,system_requirment_input_dict,logger)
    logger.info("select_img_opprtunity_Preprocess_start")
    data = obj_preprocess.preprocess()
    #print(data['cs1j2k2Domainlist__cs1j1k1'])

    #++++++++++++++++++++++++++  STEP 0  +++++++++++++++++++++++++++++++++++++++++++++++
    '''
    hard code some data
    '''
    # data['camera_memory_capacity__s'] = {s:v for s,v in data['camera_memory_capacity__s'].items() }
    # data['readout_memory_capacity__s'] = {s:v for s,v in data['readout_memory_capacity__s'].items() }
    # data['power_capacity__s']  = {s:720000000 for s,v in data['power_capacity__s'].items() }
    # data['initial_power_value__s']  = {s:v*0.9 for s,v in data['power_capacity__s'].items() }
    #++++++++++++++++++++++++++  STEP 1  +++++++++++++++++++++++++++++++++++++++++++++++
    config['objective']['GS_Pass_and_Imaging'] = True
    config['objective']['total_readout_memory'] = False

    logger.info("select_img_opprtunity_optimization_model_GS_Pass_and_Imaging_start")
    obj_model = ImageCapturePlan(data,config,logger)

    #Readout Schedule
    if config['readout_schedule']:
        data['GS_Pass_time_objective'] = obj_model.prob.objective.value()
        config['objective']['GS_Pass_and_Imaging'] = False
        config['objective']['total_readout_memory'] = True
        config["prev_model_obj"] = obj_model
        logger.info("select_img_opprtunity_optimization_model_total_readout_memory_start")
        obj_model = ImageCapturePlan(data,config,logger)

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
    

def get_downlink_schedule(image_downlink_df,img_capture_result,config,logger):

    downlink_operation_list  = ['downlinking_from_camera','downlinking_from_Readout']
    img_capture_result_downlink = img_capture_result[img_capture_result['operation'].isin(downlink_operation_list)]
    DownlinkingPreProcessObj = DownlinkingPreProcess(image_downlink_df,img_capture_result_downlink,config)
    logger.info("get_downlink_schedule_preprocess_starts")
    data_downlink = DownlinkingPreProcessObj.preprocess()
    
    if config['downlink_schedule_OnlyJustsortImgID']:
        logger.info("get_downlink_schedule_OnlyJustsortImgID")
        downlink_result = pd.DataFrame(data_downlink['LP_DD_Priority_imgID'].items(),columns=['ImageID','computed_priority']).\
            sort_values(by='computed_priority',ascending=False)
      
    else:
        logger.info("downlink_schedule_more_optimized_at_tilestrip_level_starts")
        obj_downlink_model = ImageDownlinkPlan(data_downlink,config,logger)
        downlink_result = ImageDownlinkPostProcess(obj_downlink_model,data_downlink).get_schedule()
    return downlink_result
    


def get_input_files(config,GS_pass_df,image_opportunity_df,image_downlink_df,eclipse_event_df,logger):
    ''' 
    to read input files and preliminary preprocessing.
    Input-->
            GS_pass_df-->df['GsID', 'AOS', 'LOS', 'Eclipse', 'aos_offset', 'los_offset','sat_id']
            image_opportunity_df-->df['sat_id', 'opportunity_start_time',
                'opportunity_end_time', 'StripID', 'OffNadir', 'SunInView', 'EarthInView',
                'MoonInView','OrderValidityStart', 'OrderValidityEnd', 'AoiID','CloudCoverLimit', 'CloudCover',
                'OffNadirLimit', 'Priority','opportunity_start_offset',
                'opportunity_end_offset']
            image_downlink_df-->df['ImageID', 'sat_id', 'DueDate', 'Priority', 'Tilestrips', 'Sensors',
                                   'Bands', 'EmergencyFlag', 'CaptureDate',delivery_type,assured_downlink_flag]

            eclipse_event_df ---> sat_id,base_time,start_time(str dd-mm-yyyy hh:mm:ss),end_time(str dd-mm-yyyy hh:mm:ss),eclipse(value = 1) 
    Output-->dict('GS_pass_df':df[..],\
                'image_opportunity_df':df[..],\
                'image_downlink_df':df[..],\
                "eclipse_df_dict": {'sat':df ,..},
                "config":{..})
    '''
    # GS PASS
    GS_pass_df_original = GS_pass_df.copy()
    GS_pass_df['sat_id'] = GS_pass_df['sat_id'].astype(str)
    GS_pass_df['aos_offset'] = GS_pass_df['aos_offset'].astype(int)
    GS_pass_df['los_offset'] = GS_pass_df['los_offset'].astype(int)

    # image Opprtunity 
    image_opportunity_df['sat_id'] = image_opportunity_df['sat_id'].astype(str)
    image_opportunity_df['opportunity_start_offset'] = image_opportunity_df['opportunity_start_offset'].astype(int)
    image_opportunity_df['opportunity_end_offset'] = image_opportunity_df['opportunity_end_offset'].astype(int)
    image_opportunity_df_copy = image_opportunity_df.copy()
    image_opportunity_df_copy['X'] = image_opportunity_df_copy[['opportunity_start_time','opportunity_start_offset']].apply(lambda a: pd.to_datetime(a['opportunity_start_time']) - pd.DateOffset(seconds=a['opportunity_start_offset']),axis=1)
    image_opportunity_df_copy['Y'] = image_opportunity_df_copy[['opportunity_end_time','opportunity_end_offset']].apply(lambda a: pd.to_datetime(a['opportunity_end_time']) - pd.DateOffset(seconds=a['opportunity_end_offset']),axis=1)
    base_time_stamp = image_opportunity_df_copy["X"].to_list()[0]
    config['base_time_stamp_downlink'] = base_time_stamp

    #get eclipse

    union_list_of_sat = list(set(image_opportunity_df['sat_id']).union(set(GS_pass_df['sat_id'])).union(set(image_downlink_df['sat_id'])))
    
    try :
        eclipse_df = pd.DataFrame()
        satellite_list = eclipse_event_df['sat_id'].unique()
        for sat in satellite_list:
            this_eclipse_df = eclipse_event_df[eclipse_event_df['sat_id']==sat]
            that_eclipse_df = get_eclipse_data(this_eclipse_df,config)
            eclipse_df = pd.concat([that_eclipse_df,eclipse_df])
        eclipse_df_dict = {s: eclipse_df[eclipse_df['sat_id']==s] for s in eclipse_df['sat_id'].unique()}
    except:
        logger.info("some_error_in_eclipse_data_So_hard_coded_eclipse_data")
        min_time_index= min([image_opportunity_df['opportunity_start_offset'].min(),image_opportunity_df['opportunity_end_offset'].max(),GS_pass_df['aos_offset'].min(),GS_pass_df['los_offset'].max()])
        max_time_index= max([image_opportunity_df['opportunity_start_offset'].min(),image_opportunity_df['opportunity_end_offset'].max(),GS_pass_df['aos_offset'].min(),GS_pass_df['los_offset'].max()])

        hrs = (max_time_index - min_time_index)/3600
        hrs = math.ceil(hrs)
        while True:
            hrs += 1
            if hrs % 1.5==0:
                break


        in_orbit_eclipse_event = [1 for i in range(int(1.5*3600*0.4))] + [0 for i in range(int(1.5*3600*0.6))] #
        eclipse_df  = pd.DataFrame({'time_index': [i for i in range(min_time_index,min_time_index+hrs*3600)] ,"eclipse" : in_orbit_eclipse_event*int(hrs/1.5)})
        eclipse_df['sat_id']= [union_list_of_sat] *len(eclipse_df)
        eclipse_df = eclipse_df.explode('sat_id')
        eclipse_df_dict = {s: eclipse_df[eclipse_df['sat_id']==s] for s in eclipse_df['sat_id'].unique()}

    # further processing eclipse data to align with gs pass where entire gs pass is assumed to be in eclipse region
    gsPassInput_df_copy = GS_pass_df_original
    gsPassInput_df_copy['sat_id'] = gsPassInput_df_copy['sat_id'].astype(str)
    gsPassInput_df_copy['aos_offset'] = gsPassInput_df_copy['aos_offset'].astype(int)
    gsPassInput_df_copy['los_offset'] = gsPassInput_df_copy['los_offset'].astype(int)
    gsPassInput_df_copy['list'] =  gsPassInput_df_copy[['aos_offset','los_offset']].apply(lambda a : [i for i in range(a['aos_offset'],a['los_offset']+1)],axis =1 )

    gsPassInput_df_copy1 = gsPassInput_df_copy[['sat_id','list']]
    gsPassInput_df_copy1 = gsPassInput_df_copy1.explode('list')
    gsPassInput_df_grouped_copy1 = gsPassInput_df_copy1.groupby('sat_id').agg(time_index_list = ('list',list)).reset_index()
    gsPasstimeIndexList__s = dict(zip(gsPassInput_df_grouped_copy1['sat_id'],gsPassInput_df_grouped_copy1['time_index_list']))
    for k,v in eclipse_df_dict.items():
        if k in gsPasstimeIndexList__s.keys():
            this_time_index_list = gsPasstimeIndexList__s[k]
            this_v = v[v["time_index"].isin(this_time_index_list)]
            eclipse_list = list(this_v['eclipse'].unique())
            if len(eclipse_list)==2:
                v.loc[v["time_index"].isin(this_time_index_list), "eclipse"] = 1
            elif eclipse_list[0] == 1:
                v.loc[v["time_index"].isin(this_time_index_list), "eclipse"] = 1
            else:
                v.loc[v["time_index"].isin(this_time_index_list), "eclipse"] = 0
            #v.loc[v["time_index"].isin(this_time_index_list), "eclipse"] = 0
            eclipse_df_dict[k] = v

    return {
            'GS_pass_df':GS_pass_df,\
            'image_opportunity_df':image_opportunity_df,\
            'image_downlink_df':image_downlink_df,\
            "eclipse_df_dict": eclipse_df_dict,
            "config":config
            }
    
def get_schedule(config,GS_pass_df,image_opportunity_df,image_downlink_df,eclipse_df,\
                 thermal_data_df,memory_data_df,mem_transfer_SatLvlData_df,\
                    mem_transfer_gsLvlData_df,power_data_df,power_transfer_data,setup_time_df,logger):

    ''' 
    to get imaging , readout schedule and stats for schedule.

    Input-->
            GS_pass_df-->df['GsID', 'AOS', 'LOS', 'Eclipse', 'aos_offset', 'los_offset','sat_id']
            image_opportunity_df-->df['sat_id', 'opportunity_start_time',
                'opportunity_end_time', 'StripID', 'OffNadir', 'SunInView', 'EarthInView',
                'MoonInView','OrderValidityStart', 'OrderValidityEnd', 'AoiID','CloudCoverLimit', 'CloudCover',
                'OffNadirLimit', 'Priority','opportunity_start_offset',
                'opportunity_end_offset']
            image_downlink_df-->df['ImageID', 'sat_id', 'DueDate', 'Priority', 'Tilestrips', 'Sensors',
                                   'Bands', 'EmergencyFlag', 'CaptureDate',delivery_type,assured_downlink_flag]

    Output-->dict{"only_readout_result":df['sat_id','start_time','end_time','base_time'],\
        "only_img_capture_result":df['sat_id','start_time','end_time','AoiID','StripID','base_time'],\
            "only_gsPass_result":df['sat_id','start_time','end_time','gsID','base_time'],\
            "combined_result":df['sat_id', 'start_time', 'end_time', 'encoded_strip_id', 'gsID','operation', 'camera_memory_value_endofTW',
                                'delta_camera_memory_value_in_this_TW', 'SSD_memory_value_endofTW','delta_SSD_memory_value_in_this_TW', 'global_priority',
                                'local_priority', 'mean_global_priority', 'mean_local_priority','StripID', 'AoiID', 'base_time']}

            "interpret_extracted_raw_file_df":df['sat_id', 'opportunity_start_time', 'opportunity_end_time', 'StripID','OffNadir', 'OrderValidityStart', 'OrderValidityEnd', 'AoiID','Priority', 'opportunity_start_offset', 'opportunity_end_offset',
                                                'normalized_local_priority_due_date','normalized_local_priority_CC_based','normalized_local_priority_offNadir', 'normalized_GlobalPriority','normalized_Total_Priority', 'camera_memory_value_endofTW',
                                                'delta_camera_memory_value_in_this_TW', 'flag', 'flag_gs_pass_conflict','conflicting_strip_oppr', 'concat_sat_id_encodedStripId_TWindex','CloudCoverLimit', 'CloudCover', 'encoded_strip_id', 'base_time']
            "interpret_selected_oppr_conflict_comparision_df"df['conflic_strip_flag_named', 'max_Norm_TP', 'this_flag_norm_TP','max_Norm_GP', 'this_flag_norm_GP', 'max_Norm_LPDD','this_flag_norm_LLDD', 'base_time']
            "interpret_KPI_df: df['criteria', 'before_APS', 'APS_result', 'remarks', 'percentage','base_time']
            
                                                   
    '''
    
    original_image_opportunity_df = image_opportunity_df.copy()
   
    #======================================================================================================================================================================================================
    # read_input
 
    logger.info("get_input_function_call")
    input_dict = get_input_files(config,GS_pass_df,image_opportunity_df,image_downlink_df,eclipse_df,logger)
    system_requirement_input_dict = systemReqPreprocess(thermal_data_df,memory_data_df,\
                          mem_transfer_SatLvlData_df,\
                          mem_transfer_gsLvlData_df,power_data_df,power_transfer_data,setup_time_df)
   
    config = input_dict['config']
    #======================================================================================================================================================================================================
    #gs pass_selection
    logger.info("gs pass_selection_module_start")
    gs_pass_result_df = select_gs_pass_oppr(input_dict['GS_pass_df'],config,system_requirement_input_dict,logger)
    gs_pass_result_df['Eclipse'] = 1 ## dummy
    gs_pass_result_df['duration'] = gs_pass_result_df['end_time'] - gs_pass_result_df['start_time']
    gs_pass_result_df = gs_pass_result_df[gs_pass_result_df['duration']> 0]
    
    interpret_gs_pass_result_df_copy = gs_pass_result_df.copy()# this not the gsPass result as it is to be get filtered after due to other factors in image capture plan.It is just to get require info in interpret result.

    logger.info("image_capture_plan_module_start")
    #======================================================================================================================================================================================================
    #image_selection
    img_capture_result,capture_plan_data_input= select_img_opprtunity(input_dict['image_opportunity_df'],gs_pass_result_df,input_dict['eclipse_df_dict'],config,system_requirement_input_dict,logger)
    img_capture_result['base_time'] = config['base_time_stamp_downlink']
    #img_capture_result = img_capture_result[img_capture_result['operation']=='Imaging']
    #readout_result = img_capture_result[img_capture_result['operation']=='Readout']
    
    interpret_img_capture_resul_copy = img_capture_result.copy()

    #======================================================================================================================================================================================================
    #======================================================================================================================================================================================================
    logger.info("Downlink_plan_module_start")
    try:
        downlink_result = get_downlink_schedule(input_dict['image_downlink_df'],img_capture_result,config,logger)
        downlink_result['base_time'] = config['base_time_stamp_downlink']
        downlink_result['start_time'] = downlink_result[['start_time','base_time']].apply(lambda a: pd.to_datetime(a['base_time']) + pd.DateOffset(seconds=a['start_time']),axis=1)
        downlink_result['end_time'] = downlink_result[['end_time','base_time']].apply(lambda a: pd.to_datetime(a['base_time']) + pd.DateOffset(seconds=a['end_time']),axis=1)

    except Exception as Argument:
        logger.error(Argument, stack_info=True, exc_info=True)
        downlink_result = pd.DataFrame()
        logger.error("========Downlink_plan_error=========")
    #======================================================================================================================================================================================================
    logger.info("interpret_results_starts")
    interpret_image_opportunity_df = original_image_opportunity_df.copy()
    interpret_result_dict = interpret_result(interpret_image_opportunity_df,interpret_gs_pass_result_df_copy,interpret_img_capture_resul_copy,config)
    for k,v in interpret_result_dict.items():
        v['base_time'] = config['base_time_stamp_downlink']

    logger.info("ALL RESULTS relavant columns Selection")
    only_img_capture_result = img_capture_result[img_capture_result['operation']=='Imaging'][['sat_id','start_time','end_time','aoi_id','strip_id','base_time']]
    only_img_capture_result['start_time'] = only_img_capture_result[['start_time','base_time']].apply(lambda a: pd.to_datetime(a['base_time']) + pd.DateOffset(seconds=a['start_time']),axis=1)
    only_img_capture_result['end_time'] = only_img_capture_result[['end_time','base_time']].apply(lambda a: pd.to_datetime(a['base_time']) + pd.DateOffset(seconds=a['end_time']),axis=1)

    only_readout_result = img_capture_result[img_capture_result['operation']=='Readout'][['sat_id','start_time','end_time','base_time']]
    
    if len(only_readout_result):
        only_readout_result['start_time'] = only_readout_result[['start_time','base_time']].apply(lambda a: pd.to_datetime(a['base_time']) + pd.DateOffset(seconds=a['start_time']),axis=1)
        only_readout_result['end_time'] = only_readout_result[['end_time','base_time']].apply(lambda a: pd.to_datetime(a['base_time']) + pd.DateOffset(seconds=a['end_time']),axis=1)

    only_gsPass_result = img_capture_result[img_capture_result['operation']=='downlinking_from_Readout'][['sat_id','start_time','end_time','gs_id','base_time']]
    only_gsPass_result.sort_values(by="start_time",inplace=True)
    only_gsPass_result['start_time'] = only_gsPass_result[['start_time','base_time']].apply(lambda a: pd.to_datetime(a['base_time']) + pd.DateOffset(seconds=a['start_time']),axis=1)
    only_gsPass_result['end_time'] = only_gsPass_result[['end_time','base_time']].apply(lambda a: pd.to_datetime(a['base_time']) + pd.DateOffset(seconds=a['end_time']),axis=1)

    result_dict = {"only_readout_result":only_readout_result,\
                  "only_img_capture_result":only_img_capture_result,\
                  "only_gsPass_result":only_gsPass_result,\
                  "combined_result":img_capture_result,\
                  "downlink_result":downlink_result}
    result_dict.update(interpret_result_dict)
    #======================================================================================================================================================================================================
    #return result_dict
    camera_memory_plots_fig_obj , ssd_memory_plots_fig_obj , power_plots_fig_obj ,\
        camera_detector_thermal_plots_fig_obj , XBT_thermal_plots_fig_obj , \
            NCCM_thermal_plots_fig_obj = plot_memory_power_thermal(result_dict["combined_result"],memory_data_df,\
                                                mem_transfer_SatLvlData_df,power_data_df,\
                                            power_transfer_data,input_dict['eclipse_df_dict'],thermal_data_df)
    #======================================================================================================================================================================================================
    
    plot_strip_status_df = result_dict["interpret_extracted_raw_file_df"].copy()
    #fig,this_schedule_df,first_filter_col='sat_id',title_name="Strip to Strip Status",legend_Group = "All_Strips",second_col_filter="StripID",y_axis = "Strips"):
    fig1 = go.Figure()
    strip_status_fig_obj = plot_strip_status(fig1,plot_strip_status_df,first_filter_col='sat_id',title_name="Strip to Strip Status",legend_Group = "All_Strips",second_col_filter="strip_id",y_axis = "Strips")
    plot_strip_status_df = plot_strip_status_df[plot_strip_status_df['flag']==1]
    strip_status_fig_obj = plot_strip_status(fig1,plot_strip_status_df,first_filter_col='sat_id',title_name="Strip to Strip Status",legend_Group = "Selected_strips",second_col_filter="strip_id",y_axis = "Strips")
    #======================================================================================================================================================================================================
    fig2 = go.Figure()
    # 'GsID', 'AOS', 'LOS', 'Eclipse', 'aos_offset', 'los_offset','sat_id'
    GS_pass_plot_df = GS_pass_df[["sat_id","gs_id","AOS","LOS"]]
    GS_pass_plot_df.sort_values(by="AOS",inplace=True)
    GS_pass_plot_df.rename(columns={"AOS":"opportunity_start_time",\
                                    "LOS":"opportunity_end_time"},inplace = True)
    S2G_status_fig_obj = plot_strip_status(fig2,GS_pass_plot_df,first_filter_col='sat_id',title_name="Sat to GS Status",legend_Group = "S2G_status",second_col_filter="gs_id",y_axis = "GsID")

    only_gsPass_plot_result_df = only_gsPass_result[['sat_id','start_time','end_time','gs_id']]
    only_gsPass_plot_result_df["start_time"] = only_gsPass_plot_result_df["start_time"].astype(str)
    only_gsPass_plot_result_df["end_time"] = only_gsPass_plot_result_df["end_time"].astype(str)
    only_gsPass_plot_result_df.rename(columns={"start_time":"opportunity_start_time",\
                                    "end_time":"opportunity_end_time"},inplace = True)
    
    S2G_status_fig_obj = plot_strip_status(S2G_status_fig_obj,only_gsPass_plot_result_df,first_filter_col='sat_id',title_name="Sat to GS Status",legend_Group = "S2G_selcted_status",second_col_filter="gs_id",y_axis = "GsID")
    #======================================================================================================================================================================================================
    fig3 = go.Figure()    
    G2S_status_fig_obj = plot_strip_status(fig3,GS_pass_plot_df,first_filter_col='gs_id',title_name="GS to Sat Status",legend_Group = "G2S_status",second_col_filter="sat_id",y_axis = "sat_id")
    
    G2S_status_fig_obj = plot_strip_status(fig3,only_gsPass_plot_result_df,first_filter_col='gs_id',title_name="GS to Sat Status",legend_Group = "Selected_G2S_status",second_col_filter="sat_id",y_axis = "sat_id")
    #======================================================================================================================================================================================================
    
    return result_dict
