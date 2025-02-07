import pandas as pd



def get_thermal_data(GS_pass_df,image_opportunity_df):
    sat_id_set1 = set(GS_pass_df['sat_id'])
    sat_id_set2 = set(image_opportunity_df['sat_id'])
    sat_id_set = sat_id_set1.union(sat_id_set2)

    device_list = ['camera_detector','XBT','NCCM']
    initial_temp = [20,20,20]
    temp_cap = [70,45,70]
    sufficient_cooldown_temp = [21,21,21]
    sure_cooltime = [1000,1800,1800]
    alllowed_heat_time = [100,600,600]
    a_cool_parameter = [-0.01, -0.02 , "(Teh-20)/(54.9-20)"]
    b_cool_parameter = [.0001, .0001 , 0.00169]

    d = {}
    d['sat_id'] = list(sat_id_set)
    d['device'] = [device_list] * len( d['sat_id'])
  
    d['initial_temp'] = [initial_temp]* len( d['sat_id'])
    d['temp_cap'] = [temp_cap]* len( d['sat_id'])
    d['heat_eqn'] = [["0.0000039805 * t**3 - 0.0020476 * t**2 + 0.4473 *t + 0.5265","-0.0000001075462963 * t**3 + 0.000061 * t**2  + 0.04186666667*t -1.964","0.0000000728690314 * t**3 - 0.000138692964 * t**2 + 0.103057817 * t  + 1.88504399 "]]* len( d['sat_id'])

    d['cool_eqn'] = [[ "c * math.exp(a * t + b * (T_c - Ti) ) + d ","c * math.exp(a * t + b * (T_c - Ti) ) + d ","(a*math.exp(-b*t)*34.9+20)* -1 " ]]* len( d['sat_id'])
    d['sufficient_cooldown_temp'] = [sufficient_cooldown_temp]* len( d['sat_id'])
    d['sure_cooltime'] = [sure_cooltime]* len( d['sat_id'])
    d['alllowed_heat_time'] = [alllowed_heat_time]* len( d['sat_id'])
    d['a_cool_parameter'] = [a_cool_parameter]* len( d['sat_id'])
    d['b_cool_parameter'] = [b_cool_parameter]* len( d['sat_id'])
    
    thermal_data_df = pd.DataFrame(d)
    l1 = [ 'device', 'initial_temp', 'temp_cap', 'heat_eqn', 'cool_eqn','sufficient_cooldown_temp','sure_cooltime','alllowed_heat_time','a_cool_parameter','b_cool_parameter']
    #l1 = ['device','initial_temp','temp_cap','Heat_Eqn','cool_Eqn']
    #print(thermal_data_df)
    thermal_data_df = thermal_data_df.explode(l1).reset_index(drop=True)
    return thermal_data_df

def get_memory_data(GS_pass_df,image_opportunity_df):
    sat_id_set1 = set(GS_pass_df['sat_id'])
    sat_id_set2 = set(image_opportunity_df['sat_id'])
    sat_id_set = sat_id_set1.union(sat_id_set2)
    d = {}
    d['sat_id'] = list(sat_id_set)
    memory_device = ["NCCM","SSD"]
    initial_memory = [20,350]
    memory_cap = [1000,980]
    d['memory_device'] = [memory_device]* len( d['sat_id'])
    d['initial_memory'] = [initial_memory]* len( d['sat_id'])
    d['memory_cap'] = [memory_cap]* len( d['sat_id'])

    memory_data_df  = pd.DataFrame(d)
    l1 = ['memory_device','initial_memory','memory_cap']
    memory_data_df = memory_data_df.explode(l1).reset_index(drop=True)
    return memory_data_df

def get_memory_transfer_rate_sat_level(GS_pass_df,image_opportunity_df):
    sat_id_set1 = set(GS_pass_df['sat_id'])
    sat_id_set2 = set(image_opportunity_df['sat_id'])
    sat_id_set = sat_id_set1.union(sat_id_set2)
    d = {}
    d['sat_id'] = list(sat_id_set)
    d['imaging_rate'] = [7]*len(d['sat_id'])
    d['readout_rate'] = [0.8]*len(d['sat_id'])
    df = pd.DataFrame(d)
    return df

def get_MemoryTransferRateGSLevel(GS_pass_df):
    
    downlink_rate_df = GS_pass_df[['gs_id','sat_id']].drop_duplicates()#,GS_pass_df['GsID']
    downlink_rate_df['downlink_rate'] = 1
    downlink_rate_df['setup_time'] = 120
    return downlink_rate_df

def get_PowerData(GS_pass_df,image_opportunity_df):
    sat_id_set1 = set(GS_pass_df['sat_id'])
    sat_id_set2 = set(image_opportunity_df['sat_id'])
    sat_id_set = sat_id_set1.union(sat_id_set2)
    d = {}
    d['sat_id'] = list(sat_id_set)
    d['initial_power'] = [720000000*0.9]*len(d['sat_id'])
    d['power_cap'] = [720000000]*len(d['sat_id'])
    d['power_lower_cap'] = [720000000*0.8]*len(d['sat_id'])

    return pd.DataFrame(d)

def get_PowerTransferRate(GS_pass_df,image_opportunity_df):
    sat_id_set1 = set(GS_pass_df['sat_id'])
    sat_id_set2 = set(image_opportunity_df['sat_id'])
    sat_id_set = sat_id_set1.union(sat_id_set2)
    d = {}
    d['sat_id'] = list(sat_id_set)
    d['operation'] = [['imaging','downlinking','readout','idle']]* len(d['sat_id'])
    d['sunlit_power_generate_rate'] = [[46,60,55,115]]* len(d['sat_id'])
    d['eclipse_power_consumption_rate'] = [[0,122.9,69,40.6]]* len(d['sat_id'])
    d['sunlit_power_consume_rate'] = [[86,110,69,28.1]]* len(d['sat_id'])

    power_trasfer_df  = pd.DataFrame(d)
    l1 = ['operation','sunlit_power_generate_rate','eclipse_power_consumption_rate','sunlit_power_consume_rate']
    power_trasfer_df = power_trasfer_df.explode(l1).reset_index(drop=True)
    return power_trasfer_df

def get_NCCM_thermal_data(GS_pass_df,image_opportunity_df):

    sat_id_set1 = set(GS_pass_df['sat_id'])
    sat_id_set2 = set(image_opportunity_df['sat_id'])
    sat_id_set = sat_id_set1.union(sat_id_set2)
    d = {}
    d['sat_id'] = list(sat_id_set)
    d['interface_temp'] = [[20,30,40]]* len( d['sat_id'])
    d['cool_eqn'] = [['-2*t','-4*t','-6*t']]* len( d['sat_id'])

    nccm_cool_data_df = pd.DataFrame(d)
    l1 = [ 'interface_temp', 'cool_eqn']
    #l1 = ['device','initial_temp','temp_cap','Heat_Eqn','cool_Eqn']
    #print(thermal_data_df)
    nccm_cool_data_df = nccm_cool_data_df.explode(l1).reset_index(drop=True)
    return nccm_cool_data_df

def setup_time_config():

    from1 = ["sat_id","gs_id","sat_id"]
    to1 = ["gs_id","sat_id","strip_id"]
    setup_time = [120,120,120]

    return pd.DataFrame({"from":from1,\
                  "to":to1,\
                    "setup_time":setup_time})