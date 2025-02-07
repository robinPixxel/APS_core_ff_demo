from APS_Python_core.new_main import *
from test_utils import *
import json
# importing module
import logging

# Create and configure logger
logging.basicConfig(filename="newfile.log",
                    format='%(asctime)s %(message)s',
                    filemode='w')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

GS_pass_df = pd.read_csv("1_input_data/new_data/gs_pass_clean.csv") #gs_PASS_CHECK_input#GS_Passes_new (1)
image_opportunity_df = pd.read_csv("1_input_data/new_data/image_oppr_clean.csv")
image_downlink_df = pd.read_csv("1_input_data/new_data/image_table_clean.csv")
eclipse_df = pd.DataFrame({"sat_id":[],"start_time":[],"end_time":[],"eclipse":[]}) 

##### mission system and sub system parameters data generate

thermal_data_df = get_thermal_data(GS_pass_df,image_opportunity_df) 
memory_data_df = get_memory_data(GS_pass_df,image_opportunity_df)
mem_transfer_SatLvlData_df = get_memory_transfer_rate_sat_level(GS_pass_df,image_opportunity_df)
mem_transfer_gsLvlData_df = get_MemoryTransferRateGSLevel(GS_pass_df)
power_data_df = get_PowerData(GS_pass_df,image_opportunity_df)
power_transfer_data = get_PowerTransferRate(GS_pass_df,image_opportunity_df)
nccm_cool_data_df = get_NCCM_thermal_data(GS_pass_df,image_opportunity_df)
nccm_cool_data_df = get_NCCM_thermal_data(GS_pass_df,image_opportunity_df)
setup_time_df = setup_time_config()

# setup time is constant for s2g and g2s : Needs also to be in data ?

#================================================================================================================================================
with open('1_input_data/new_data/config.json', 'r') as file: #config_develop
    config = json.load(file)
result_dict = get_schedule(config,GS_pass_df,image_opportunity_df,image_downlink_df,eclipse_df,\
                 thermal_data_df,memory_data_df,mem_transfer_SatLvlData_df,\
                    mem_transfer_gsLvlData_df,power_data_df,power_transfer_data,setup_time_df,logger)
for k,v in result_dict.items():
    v.to_csv('5_output_data/'+k+'.csv',index=None)
#================================================================================================================================================


