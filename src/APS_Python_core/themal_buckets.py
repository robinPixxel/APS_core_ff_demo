
import pandas as pd
import math

def evaluate_heat_eqn(t,temp_eqn):
    return eval(temp_eqn,{'t':t})

def evaluate_cool_eqn(initial_heat_temp , final_temp , interface_temp , duration , a_cool_parameter , b_cool_parameter , cool_eqn , operation = "Imaging" ):

    Ti = interface_temp
    Teh = initial_heat_temp
    T_c = final_temp
    t = duration

    if operation == "Imaging" :
        a = eval(str(a_cool_parameter))
        b = eval(str(b_cool_parameter))

        c = eval( "(Ti - Teh)/( math.exp(a* 1800) - math.exp( b *(Teh - Ti) ) )" )

        d = eval( "c * math.exp( b * (Teh - Ti) ) " ) * -1 

        delta_cool  = eval( cool_eqn )
    if operation == "Readout":
        a = eval(str(a_cool_parameter))
        b = eval(str(b_cool_parameter))
        delta_cool  = eval( cool_eqn )

    if operation == "downlinking_from_Readout":

        a = eval(str(a_cool_parameter))
        b = eval(str(b_cool_parameter))

        c = eval( "(Ti - Teh)/( math.exp(a* 1800) - math.exp( b *(Teh - Ti) ) )" )

        d = eval( "c * math.exp( b * (Teh - Ti) ) " ) * -1 

        delta_cool  = eval( cool_eqn )

    return delta_cool


def get_bucketwise_safe_cool_time(initial_heat_temp,interface_temp,max_cool_time,sufficient_cooldown_temp, cool_eqn , a_cool_parameter , b_cool_parameter, operation):
   
    final_temp = initial_heat_temp
    c = 0 
    while final_temp >= sufficient_cooldown_temp and c <= max_cool_time :
        delta_temp = evaluate_cool_eqn(initial_heat_temp , final_temp, interface_temp , c , a_cool_parameter , b_cool_parameter , cool_eqn , operation )
        final_temp = initial_heat_temp + delta_temp
        c += 1
    if c <= max_cool_time: 
        return c
    else:
        return max_cool_time
           
def get_thermal_bucket(initial_temp, heat_temp_eqn ,cool_eqn ,thermal_cap_temp,\
                       sufficient_cooldown_temp, sure_cooltime , allowed_heat_time , a_cool_parameter , b_cool_parameter , operation):
    '''
    tenp_eqn (for heating): str Expression example: "2*t**3 + 3*t**2 + 5*t + 10 " expression should have variable t and t  will be given as seconds input

    return dict
    '''
    _time_hrs = allowed_heat_time/60/60
    time_index_list = [i for i in range(0,int(_time_hrs*3600))]
    initial_temp_list = [initial_temp]+[0 for i in range(1,int(_time_hrs*3600))]
    bucket_flag = [ [i]*10 for i in range(0,int(_time_hrs*3600/10))]
    bucket_flag = sum(bucket_flag,[]) # flattening
    
    df = pd.DataFrame({"time_index":time_index_list,\
                "initial_temp": initial_temp_list,\
                    "bucket_flag":bucket_flag})
    
    save_dict = {}
    for t_ind in range(0,int(_time_hrs*3600)):
        delta_temp = evaluate_heat_eqn( t_ind, heat_temp_eqn )
        save_dict[t_ind] = delta_temp
        if delta_temp + initial_temp >= thermal_cap_temp:
            break
    df = df[df['time_index'] < t_ind]
   
    df['delta_temp_heat'] = df['time_index'].map(save_dict)

    df['proxy_heat'] = df['delta_temp_heat'] +   initial_temp
    df['cumm_heat'] = df['proxy_heat']#.cumsum()

    df = df[df['cumm_heat']<=thermal_cap_temp]

    max_temp_sec = df['time_index'].max()

    df_grouped_ = df.groupby(['bucket_flag']).agg(max_time=('time_index','max'),\
                                    min_time = ('time_index','min')).reset_index()

    df_heat = pd.merge(df,df_grouped_,on='bucket_flag',how='left')

    df_cool = df_heat[df_heat['max_time']==df_heat['time_index']]
   
    initial_heat_list = list(df_cool['cumm_heat'])
    safe_cool_time_list = []
    for initial_heat_temp in initial_heat_list:
        safe_cool_time_list.append(get_bucketwise_safe_cool_time(initial_heat_temp,initial_temp,sure_cooltime,sufficient_cooldown_temp, cool_eqn , a_cool_parameter , b_cool_parameter, operation))
    df_cool['safe_cool_time'] = safe_cool_time_list
    df_cool['safe_cool_time'] = df_cool['safe_cool_time'].fillna(1)

    df_cool = df_cool[['bucket_flag','max_time','min_time','safe_cool_time']]
    
    df_cool['list'] = df_cool[['bucket_flag','max_time','min_time','safe_cool_time']].\
        apply(lambda a : [a['min_time'],a['max_time'],a['safe_cool_time']],axis = 1)

    max_min_sct__bucket_flag = dict(zip(df_cool['bucket_flag'],df_cool['list']))

    max_min_sct__bucket_flag['max_time_heat'] = t_ind - 1

    return max_min_sct__bucket_flag 