import pandas as pd

def systemReqPreprocess(thermal_data_df,memory_data_df,\
                          mem_transfer_SatLvlData_df,\
                          mem_transfer_gsLvlData_df,power_data_df,power_transfer_data,setup_time_df):
    
    """ 
    thermal_data_df : 'sat_id', 'device', 'initial_temp', 'temp_cap', 'Heat_Eqn', 'cool_Eqn'

    nccm_cool_data_df : may be requred or  not ?

    memory_data_df :'sat_id', 'memory_device', 'initial_memory', 'memory_cap'
    mem_transfer_SatLvlData_df :'sat_id', 'imaging_rate', 'readout_rate'
    mem_transfer_gsLvlData_df : 'GsID', 'sat_id', 'downlink_rate' ,'setup_time'
    power_data_df : sat_id , initial_power , power_cap
    power_transfer_data : sat_id , operation , 'sunlit_power_generate_rate','eclipse_power_consumption_rate','sunlit_power_consume_rate' 
    """
    

    data = {}
    # nccm_initial_temp_df = thermal_data_df[thermal_data_df['device']=='NCCM'][['sat_id','initial_temp']].drop_duplicates()

    # nccm_cool_data_df = pd.merge(nccm_cool_data_df,nccm_initial_temp_df,on='sat_id',how='left')
    # nccm_cool_data_df1 = nccm_cool_data_df[nccm_cool_data_df['interface_temp']>=nccm_initial_temp_df['initial_temp']].sort_values(by='interface_temp',ascending = True)
    # nccm_cool_data_df2 = nccm_cool_data_df[nccm_cool_data_df['interface_temp']<=nccm_initial_temp_df['initial_temp']].sort_values(by='interface_temp',ascending = False)
    
    # nccm_cool_data_df1 = nccm_cool_data_df1.drop_duplicates(subset = ['sat_id'],keep='first')
    # nccm_cool_data_df2 = nccm_cool_data_df2.drop_duplicates(subset = ['sat_id'],keep='first')
    

    
    thermal_data_df['con'] = list(zip(thermal_data_df['sat_id'],thermal_data_df['device']))
    data['thermal_data_'] = dict(zip(thermal_data_df['con'], thermal_data_df[['initial_temp', 'temp_cap', 'heat_eqn', 'cool_eqn',\
                                                                              'sufficient_cooldown_temp',"sure_cooltime",'alllowed_heat_time',"a_cool_parameter","b_cool_parameter"]].values.tolist()))

    memory_data_df['con'] = list(zip(memory_data_df['sat_id'],memory_data_df['memory_device']))
    data['memory_data_'] = dict(zip(memory_data_df['con'], memory_data_df[['initial_memory','memory_cap']].values.tolist()))

    data['mem_transfer_SatLvlData_'] = dict(zip(mem_transfer_SatLvlData_df['sat_id'], mem_transfer_SatLvlData_df[['imaging_rate','readout_rate']].values.tolist()))

    mem_transfer_gsLvlData_df['con'] = list(zip(mem_transfer_gsLvlData_df['sat_id'],mem_transfer_gsLvlData_df['gs_id']))
    data['mem_transfer_gsLvlData_'] = dict(zip(mem_transfer_gsLvlData_df['con'], mem_transfer_gsLvlData_df[['downlink_rate','setup_time']].values.tolist()))

    data['power_data_'] = dict(zip(power_data_df['sat_id'], power_data_df[['initial_power','power_cap','power_lower_cap']].values.tolist()))

    power_transfer_data['con'] = list(zip(power_transfer_data['sat_id'],power_transfer_data['operation']))
    data['power_transfer_'] = dict(zip(power_transfer_data['con'], power_transfer_data[['sunlit_power_generate_rate','eclipse_power_consumption_rate','sunlit_power_consume_rate']].values.tolist()))

    setup_time_df['con'] = list(zip(setup_time_df['from'],setup_time_df['to']))
    data['setup_time_'] = dict(zip(setup_time_df['con'], setup_time_df['setup_time']))

    return data

