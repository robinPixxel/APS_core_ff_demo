from APS_Python_core.new_main import *
from APS_Python_core.propogator import *
import pandas as pd
from test_utils import *
import json
import math
# importing module
import logging

GS_pass_df = pd.read_csv("1_input_data/new_data/gs_pass_clean.csv") #gs_PASS_CHECK_input#GS_Passes_new (1)
image_opportunity_df = pd.read_csv("1_input_data/new_data/image_oppr_clean.csv")
image_downlink_df = pd.read_csv("1_input_data/new_data/image_table_clean.csv")
eclipse_df = pd.DataFrame({"sat_id":[],"start_time":[],"end_time":[],"eclipse":[]}) 
with open('1_input_data/new_data/config.json', 'r') as file: #config_develop
    config = json.load(file)


thermal_data_df = get_thermal_data(GS_pass_df,image_opportunity_df) 
memory_data_df = get_memory_data(GS_pass_df,image_opportunity_df)
mem_transfer_SatLvlData_df = get_memory_transfer_rate_sat_level(GS_pass_df,image_opportunity_df)
mem_transfer_gsLvlData_df = get_MemoryTransferRateGSLevel(GS_pass_df)
power_data_df = get_PowerData(GS_pass_df,image_opportunity_df)
power_transfer_data = get_PowerTransferRate(GS_pass_df,image_opportunity_df)
nccm_cool_data_df = get_NCCM_thermal_data(GS_pass_df,image_opportunity_df)
nccm_cool_data_df = get_NCCM_thermal_data(GS_pass_df,image_opportunity_df)
setup_time_df = setup_time_config()

#=================#=================#=================#=================#=================#=================
image_opportunity_df_copy = image_opportunity_df.copy()
image_opportunity_df_copy['X'] = image_opportunity_df_copy[['opportunity_start_time','opportunity_start_offset']].apply(lambda a: pd.to_datetime(a['opportunity_start_time']) - pd.DateOffset(seconds=a['opportunity_start_offset']),axis=1)
image_opportunity_df_copy['Y'] = image_opportunity_df_copy[['opportunity_end_time','opportunity_end_offset']].apply(lambda a: pd.to_datetime(a['opportunity_end_time']) - pd.DateOffset(seconds=a['opportunity_end_offset']),axis=1)
base_time_stamp = image_opportunity_df_copy["X"].to_list()[0]
#=================#=================#=================#=================#=================#=================

try :
    eclipse_df = pd.DataFrame()
    satellite_list = eclipse_event_df['sat_id'].unique()
    
    config['base_time_stamp_downlink'] = base_time_stamp
    for sat in satellite_list:
        this_eclipse_df = eclipse_event_df[eclipse_event_df['sat_id']==sat]
        that_eclipse_df = get_eclipse_data(this_eclipse_df,config)
        eclipse_df = pd.concat([that_eclipse_df,eclipse_df])
    eclipse_df_dict = {s: eclipse_df[eclipse_df['sat_id']==s] for s in eclipse_df['sat_id'].unique()}
except:
    #logger.info("some_error_in_eclipse_data_So_hard_coded_eclipse_data")
    union_list_of_sat = list(set(image_opportunity_df['sat_id']).union(set(GS_pass_df['sat_id'])).union(set(image_downlink_df['sat_id'])))

    min_time_index= min([image_opportunity_df['opportunity_start_offset'].min(),image_opportunity_df['opportunity_end_offset'].max(),GS_pass_df['aos_offset'].min(),GS_pass_df['los_offset'].max()])
    max_time_index= max([image_opportunity_df['opportunity_start_offset'].min(),image_opportunity_df['opportunity_end_offset'].max(),GS_pass_df['aos_offset'].min(),GS_pass_df['los_offset'].max()])

    #========

    next_offset_time_first_oppr = max_time_index + 3600
    max_time_index = next_offset_time_first_oppr
    #========
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


#=================#=================#=================#=================#=================#=================

combined_result_df = pd.read_csv("5_output_data/combined_result.csv")
combined_result_df = combined_result_df[['sat_id', 'start_time', 'end_time' , 'operation']]

camera_memory_estimator_dict = {}
readout_memory_estimator_dict = {}
power_estimator_dict = {}
thermal_estimator_dict = {}
for s in combined_result_df["sat_id"].unique():
    eclipse_this_df = eclipse_df_dict[s]
    camera_memory_estimator_dict[s] = camera_memory_estimator(s,combined_result_df,10,mem_transfer_SatLvlData_df)
    readout_memory_estimator_dict[s] = readout_memory_estimator(s,combined_result_df,10,mem_transfer_SatLvlData_df)
    power_estimator_dict[s] = power_estimator(s,combined_result_df,eclipse_this_df,power_transfer_data,power_data_df,\
                                              next_offset_time_first_oppr)
    thermal_estimator_dict[s] = thermal_estimator(s,combined_result_df,thermal_data_df,next_offset_time_first_oppr)
    


print(thermal_estimator_dict)

