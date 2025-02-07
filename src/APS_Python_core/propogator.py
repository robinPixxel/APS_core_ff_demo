from APS_Python_core.utils import get_EcStEnd_list
import pandas as pd
#from APS_Python_core.themal_buckets import evaluate_cool_eqn
from APS_Python_core.plot_propogator_utils import correct_delta,get_delta_power,get_thermal_delta_list,get_delta_memory,map_dict,get_df,generate_profile_plots,get_overlap_plots
#


def camera_memory_estimator(s,image_capture_planned_df,initial_memory,mem_transfer_SatLvlData_df):
    """ 
    s : sat_id
    image_capture_planned_df : sat_id(str),start_time(float),end_time(float),operation(Imaging,Readout)
    current_time: current time
    imaging_rate
    readout_rate
    initial memory : current memory
    mem_transfer_SatLvlData_df :sat_id	imaging_rate	readout_rate
    """

    thissat_id_df = image_capture_planned_df[image_capture_planned_df['operation'].isin(['Readout','Imaging'])]
    thissat_id_df = thissat_id_df[thissat_id_df['sat_id']==s]
    thissat_id_df.sort_values(by='start_time',inplace = True)
    thissat_id_df = pd.merge(thissat_id_df,mem_transfer_SatLvlData_df,on='sat_id',how = 'left')
    thissat_id_df['delta_memory_till_now'] = thissat_id_df[['start_time','end_time','operation',"imaging_rate","readout_rate"]]\
                .apply(lambda a: (a['end_time']-a['start_time'])*a["imaging_rate"] if a['operation']=='Imaging' \
                 else (a['end_time']-a['start_time'])*a["readout_rate"]*-1 , axis = 1)
    
    #thissat_id_df['end_time'] -  thissat_id_df['start_time']*imaging_rate
    return thissat_id_df['delta_memory_till_now'].sum() + initial_memory

def readout_memory_estimator(s,image_capture_planned_df,initial_memory,mem_transfer_SatLvlData_df):
    """ 
    s : sat_id
    image_capture_planned_df : sat_id(str),start_time(float),end_time(float),current_time(float),operation(Readout,downlink_from_camera)
    current_time: current time
    readout_rate
    initial memory : current memory
    mem_transfer_SatLvlData_df: sat_id	imaging_rate	readout_rate
    """
    thissat_id_df = image_capture_planned_df[image_capture_planned_df['sat_id']==s]
    thissat_id_df = thissat_id_df[thissat_id_df['operation'].isin(['Readout','downlink_from_camera'])]
    thissat_id_df.sort_values(by='start_time',inplace = True)
    thissat_id_df = pd.merge(thissat_id_df,mem_transfer_SatLvlData_df,on='sat_id',how = 'left')
    thissat_id_df['delta_memory_till_now'] = thissat_id_df[['start_time','end_time','operation','readout_rate']]\
                .apply(lambda a: (a['end_time']-a['start_time'])*a["readout_rate"] \
                if a['operation']=='Readout' \
                else 0, axis  = 1)
    
    #thissat_id_df['end_time'] -  thissat_id_df['start_time']*imaging_rate
    return thissat_id_df['delta_memory_till_now'].sum() + initial_memory
   

def power_estimator(s,image_capture_planned_df,eclipse_df,power_transfer_data,power_data_df,next_offset_time_first_oppr):
    """ 
    s: sat_id
    current_time : offset_time

    image_capture_planned_df:sat_id,start_time,end_time,operation(Imaging,Readout,downlink_from_camera)
    already filtered start_time s.t,. it is >= start_time of gs_pass from initial power is taken.

    power_transfer_data : sat_id	operation	sunlit_power_generate_rate	eclipse_power_consumption_rate	sunlit_power_consume_rate


    eclipse_df : should be available from start_time s.t,. it is >= start_time of gs_pass from initial power is taken 
                                      TO   next_offset_time_first_oppr
    next_offset_time_first_oppr : next horizon first opportunity
    
    power_data_df: sat_id	initial_power	power_cap	power_lower_cap

    """

    thissat_id_df = image_capture_planned_df[image_capture_planned_df['sat_id']==s]
    thissat_id_df.sort_values(by='start_time',inplace = True)

    thissat_id_df['till_now_max'] = thissat_id_df.groupby('sat_id')['end_time'].cummax()
    thissat_id_df['prev_max'] = thissat_id_df.groupby('sat_id')['till_now_max'].shift(1)

    imgGS_union_df1 = thissat_id_df[thissat_id_df['start_time'] > thissat_id_df['prev_max'] + 1] 
    imgGS_union_df1['start_time1'] = imgGS_union_df1['prev_max'] + 1 #TODO1 +1 is okay ?
    imgGS_union_df1['end_time1'] = imgGS_union_df1['start_time'] - 1
    imgGS_union_df1['operation'] = 'idle'

    last_TW_df = thissat_id_df.groupby('sat_id').agg(min_start_time =('end_time','max')).reset_index()
       
    last_TW_df['min_start_time'] = last_TW_df['min_start_time'] +  1
    last_TW_df1 = pd.DataFrame({'sat_id':list(last_TW_df['sat_id']),\
                        'start_time1': list(last_TW_df['min_start_time']),\
                        })

    last_TW_df1['end_time1'] = next_offset_time_first_oppr # TODO1 #self.config['scheduled_Hrs']*3600 - 1 #40
    last_TW_df1['operation'] = 'idle'

    imgGS_union_df1 = pd.concat([imgGS_union_df1,last_TW_df1])

    imgGS_union_df1 = imgGS_union_df1.drop(['start_time', 'end_time','till_now_max','prev_max'], axis=1)
    imgGS_union_df1.rename(columns={'start_time1':'start_time','end_time1':'end_time'},inplace=True)
    #imgGS_union_df1 ==> contains TW without img and without gs pass  table without eclipse divide
    thissat_id_df = pd.concat([thissat_id_df,imgGS_union_df1])
    thissat_id_df['duration'] = thissat_id_df['end_time']-thissat_id_df['start_time']

    thissat_id_df['EcStEnd_list'] = thissat_id_df[['sat_id','start_time','end_time']].\
                                          apply( lambda a : \
                                          get_EcStEnd_list(a['start_time'],a['end_time'],df = eclipse_df ),axis= 1 )
    
    thissat_id_df['len_EcStEnd_list'] = thissat_id_df['EcStEnd_list'].apply(lambda a : len(a))
    print('len_before_eclipse_transition_divide=',len(thissat_id_df))
    thissat_id_df = thissat_id_df.explode('EcStEnd_list')
    print('len_after_eclipse_transition_divide=',len(thissat_id_df))
    thissat_id_df['new_eclipse'] = thissat_id_df['EcStEnd_list'].apply(lambda a : a[0])
    thissat_id_df['new_start_time'] = thissat_id_df['EcStEnd_list'].apply(lambda a : a[1])
    thissat_id_df['new_end_time'] = thissat_id_df['EcStEnd_list'].apply(lambda a : a[2])
    thissat_id_df.drop(['start_time','end_time','EcStEnd_list'],axis=1,inplace=True)
    thissat_id_df.rename(columns={'new_start_time':'start_time','new_end_time':'end_time','new_eclipse':'eclipse'},inplace=True)    
    thissat_id_df = thissat_id_df.drop(['till_now_max','prev_max'], axis=1)

    power_transfer_map_dict = {'imaging':"Imaging",\
                                "downlinking":"downlinking_from_Readout",\
                                "readout":"Readout",\
                                "idle":"idle"} 
    
    power_transfer_data['operation'] = power_transfer_data['operation'].map(power_transfer_map_dict)
    

    thissat_id_df = pd.merge(thissat_id_df,power_transfer_data,on=["sat_id","operation"],how='left')
    thissat_id_df = pd.merge(thissat_id_df , power_data_df, on = "sat_id",how='left')

    thissat_id_df["delta_power"] = thissat_id_df[["operation","duration","eclipse",\
                                                          "sunlit_power_generate_rate",\
                                                            "eclipse_power_consumption_rate",\
                                                            "sunlit_power_consume_rate"]]\
                                                        .apply(lambda a: get_delta_power(a["operation"],\
                                                                                         a["duration"],\
                                                                                         a["eclipse"],\
                                                                                         a["sunlit_power_generate_rate"],\
                                                                                         a["eclipse_power_consumption_rate"],\
                                                                                         a["sunlit_power_consume_rate"]),axis=1)

 
    return list(thissat_id_df["initial_power"].unique())[0] + thissat_id_df["delta_power"].sum()


def thermal_estimator(s,image_capture_planned_df,thermal_data_df,next_offset_time_first_oppr):
    ''' 
    s: sat_id
    image_capture_planned_df:"sat_id","start_time","end_time","operation",
    next_offset_time_first_oppr : next horizon first opportunity offset
    offset will be from common base time , preferably start of current plabnning horizon
    thermal_data_df: sat_id	device	initial_temp	temp_cap	heat_eqn	cool_eqn	sufficient_cooldown_temp	sure_cooltime	alllowed_heat_time	a_cool_parameter	b_cool_parameter

    '''

    thissat_id_df = image_capture_planned_df[image_capture_planned_df['sat_id']==s]
    thissat_id_df.sort_values(by='start_time',inplace=True)
    thissat_id_df['till_now_max'] = thissat_id_df.groupby('sat_id')['end_time'].cummax()
    thissat_id_df['prev_max'] = thissat_id_df.groupby('sat_id')['till_now_max'].shift(1)

    thissat_id_df1 = thissat_id_df[thissat_id_df['start_time'] > thissat_id_df['prev_max'] + 1] 
    thissat_id_df1['start_time1'] = thissat_id_df1['prev_max'] + 1 #TODO1 +1 is okay ?
    thissat_id_df1['end_time1'] = thissat_id_df1['start_time'] - 1
    thissat_id_df1['operation'] = 'idle'

    last_TW_df = thissat_id_df.groupby('sat_id').agg(min_start_time =('end_time','max')).reset_index()
       
    last_TW_df['min_start_time'] = last_TW_df['min_start_time'] +  1
    last_TW_df1 = pd.DataFrame({'sat_id':list(last_TW_df['sat_id']),\
                        'start_time1': list(last_TW_df['min_start_time']),\
                        })

    last_TW_df1['end_time1'] = next_offset_time_first_oppr # TODO #self.config['scheduled_Hrs']*3600 - 1 #40
    last_TW_df1['operation'] = 'idle'
    thissat_id_df1 = pd.concat([thissat_id_df1,last_TW_df1])

    thissat_id_df1 = thissat_id_df1.drop(['start_time', 'end_time','till_now_max','prev_max'], axis=1)
    thissat_id_df1.rename(columns={'start_time1':'start_time','end_time1':'end_time'},inplace=True)
    #imgGS_union_df1 ==> contains TW without img and without gs pass  table without eclipse divide
    all_TW_df = pd.concat([thissat_id_df,thissat_id_df1])
    all_TW_df.sort_values(by ='start_time',inplace=True)

    thermal_camera_detector_df = thermal_data_df[thermal_data_df['device']=='camera_detector'][['sat_id','heat_eqn','cool_eqn','initial_temp','temp_cap','a_cool_parameter', 'b_cool_parameter']]
    thermal_XBT_df = thermal_data_df[thermal_data_df['device']=='XBT'][['sat_id','heat_eqn','cool_eqn','initial_temp','temp_cap','a_cool_parameter', 'b_cool_parameter']]
    thermal_NCCM_df = thermal_data_df[thermal_data_df['device']=='NCCM'][['sat_id','heat_eqn','cool_eqn','initial_temp','temp_cap','a_cool_parameter', 'b_cool_parameter']]

    thermal_camera_detector_df.rename(columns = {"heat_eqn":"camera_detector_heat_eqn",\
                                                 "cool_eqn":"camera_detector_cool_eqn",\
                                                "initial_temp":"initial_camera_detector_temp",\
                                                "temp_cap":"cap_camera_detector_temp",\
                                                "a_cool_parameter":"camera_detector_a_cool_parameter",\
                                                "b_cool_parameter":"camera_detector_b_cool_parameter"}, inplace = True)
    
    thermal_XBT_df.rename(columns = {"heat_eqn":"XBT_heat_eqn",\
                                    "cool_eqn":"XBT_cool_eqn",\
                                    "initial_temp":"initial_xbt_temp",\
                                    "temp_cap":"cap_xbt_temp",\
                                    "a_cool_parameter":"XBT_a_cool_parameter",\
                                    "b_cool_parameter":"XBT_b_cool_parameter"}, inplace = True)
    
    thermal_NCCM_df.rename(columns = {"heat_eqn":"NCCM_heat_eqn",\
                                    "cool_eqn":"NCCM_cool_eqn",\
                                    "initial_temp":"initial_nccm_temp",\
                                    "temp_cap":"cap_nccm_temp",\
                                    "a_cool_parameter":"NCCM_a_cool_parameter",\
                                    "b_cool_parameter":"NCCM_b_cool_parameter"}, inplace = True)
    


    all_TW_df = pd.merge(all_TW_df,thermal_camera_detector_df,on="sat_id",how="left")
    all_TW_df = pd.merge(all_TW_df,thermal_XBT_df,on="sat_id",how="left")
    all_TW_df = pd.merge(all_TW_df,thermal_NCCM_df,on="sat_id",how="left")

    all_TW_df.sort_values(by = 'start_time', inplace = True )
    all_TW_df['duration'] = all_TW_df["end_time"] - all_TW_df["start_time"]
 
    all_TW_df = get_thermal_delta_list(all_TW_df ,need_thermal_operation = "Imaging",heat_eqn_column = "camera_detector_heat_eqn", cool_eqn_col = "camera_detector_cool_eqn",initial_temp_col = "initial_camera_detector_temp" , a_cool_parameter_col = "camera_detector_a_cool_parameter" , b_cool_parameter_col = "camera_detector_b_cool_parameter",delta_col= "delta_camera_detector")
    all_TW_df= get_thermal_delta_list(all_TW_df ,need_thermal_operation = "downlinking_from_Readout",heat_eqn_column = "XBT_heat_eqn", cool_eqn_col = "XBT_cool_eqn",initial_temp_col = "initial_xbt_temp" , a_cool_parameter_col = "XBT_a_cool_parameter" , b_cool_parameter_col = "XBT_b_cool_parameter",delta_col= "delta_xbt" )

    all_TW_df= get_thermal_delta_list(all_TW_df ,need_thermal_operation = "Readout",heat_eqn_column = "NCCM_heat_eqn", cool_eqn_col = "NCCM_cool_eqn",initial_temp_col = "initial_nccm_temp" , a_cool_parameter_col = "NCCM_a_cool_parameter" , b_cool_parameter_col = "NCCM_b_cool_parameter",delta_col= "delta_nccm" )
    all_TW_df = correct_delta(all_TW_df,delta_col="delta_camera_detector", initial_col = "initial_camera_detector_temp" , cap_col = "cap_camera_detector_temp" ,lower_cap_col= "initial_camera_detector_temp"  )
    all_TW_df = correct_delta(all_TW_df,delta_col="delta_xbt", initial_col = "initial_xbt_temp" , cap_col = "cap_xbt_temp" , lower_cap_col = "initial_xbt_temp"  )
    
    all_TW_df = correct_delta(all_TW_df,delta_col="delta_nccm", initial_col = "initial_nccm_temp" , cap_col = "cap_nccm_temp" , lower_cap_col = "initial_nccm_temp"  )


    return [ 
             list(all_TW_df["initial_camera_detector_temp"].unique())[0] + all_TW_df["delta_camera_detector"].sum(),
             list(all_TW_df["initial_xbt_temp"].unique())[0] + all_TW_df["delta_xbt"].sum(),
             list(all_TW_df["initial_nccm_temp"].unique())[0] + all_TW_df["delta_nccm"].sum()   
           ]
            




    
    
    # else:
    #     duration = next_time_offset - all_TW_df_end_date_max  # TODO
    #     delta_temp = eval(cool_eqn,duration)
 
    #     if all_TW_df["delta_temp"].cumsum() + initial_temp + delta_temp <= interface_temp:
    #         return interface_temp
    #     else:
    #         return all_TW_df["delta_temp"].cumsum() + initial_temp + delta_temp 



