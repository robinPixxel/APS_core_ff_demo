import pandas as pd
import datetime
from pulp import *

def get_flag_for_gs_pass(current_t,TW,TW_index,GS_list):
    flag = 0 
    l1 = []
    for i,v in enumerate(TW):
        if current_t >= v[0] and current_t <= v[1]:
            flag = 1
            l1.append([TW_index[i],GS_list[i]])

    if flag ==0 :
        return -1 
    else:
        return l1
    
def get_flag_for_image(current_t,TW_list,TW_index_list,job_list,TW_gs_Pass_list,gs_list):
            flag1 = 0
            flag2 = 0 
            l1 = []
            l2  = []

            # l1 = [ [TW_index[i],job_list[i]] for i,v in enumerate(TW) if current_t > v[0] and current_t <= v[1] ]
            # if TW_gs_Pass_list == 'NA':
            #     l2 = [] 
            # else:
            #     l2 = [ [ v,gs_list[i] ]  for i,v in enumerate(TW_gs_Pass_list) if current_t > v[0] and current_t <= v[1] ]
        
            #l1 = [i,v for i,v in enumerate(TW) if current_t > v[0] and current_t <= v[1] ]
            for i,v in enumerate(TW_list):
                if current_t > v[0] and current_t <= v[1]:
                    flag1= 1
                    l1.append([TW_index_list[i],job_list[i]])
                    

            if TW_gs_Pass_list == 'NA':
                flag2 = 0
            else:
                for i,v in enumerate(TW_gs_Pass_list):
                    if current_t > v[0] and current_t <= v[1]:
                        flag2 = 1
                        l2.append([v,gs_list[i]])                        

            if flag1 == 0 and flag2 ==0 :
                return [-1,-1]
            elif flag1 == 0 and flag2 != 0:
                return [-1,l2]
            elif flag1 !=0 and flag2 ==0:
                return [l1,-1]
            else:
                return [l1,l2]

            # if len(l1) == 0 and len(l2) ==0 :
            #     return [-1,-1]
            # elif len(l1) == 0 and len(l2) != 0:
            #     return [-1,l2]
            # elif len(l1) !=0 and len(l2) ==0:
            #     return [l1,-1]
            # else:
            #     return [l1,l2]
    

# def get_flag_for_image(current_t,TW,TW_index,job_list,TW_gs_Pass_list,gs_list):
#     flag1 = 0
#     flag2 = 0 
#     l1 = []
#     l2  = []
#     for i,v in enumerate(TW):
#         if current_t > v[0] and current_t <= v[1]:
#             flag1= 1
#             l1.append([TW_index[i],job_list[i]])

    
#     for i,v in enumerate(TW_gs_Pass_list):
#         if v == 'NA':
#             flag2 = 0
#         if current_t > v[0] and current_t <= v[1]:
#             flag2 = 1
#             l2.append([v,gs_list[i]])


#     if flag1 == 0 and flag2 ==0 :
#         return [-1,-1]
#     elif flag1 == 0 and flag2 != 0:
#         return [-1,l2]
#     elif flag1 !=0 and flag2 ==0:
#         return [l1,-1]
#     else:
#         return [l1,l2]
    
def filter_TW():
    pass


def heating_func(t1,t_now):

    heat_value = -0.02323*(((t_now-t1)/60)**3) + 0.2196 *(((t_now-t1)/60)**2) +	2.512*((t_now-t1)/60)  -1.964
    #-0.02323	0.2196	2.512	-1.964
    return heat_value

def cooling_func(t1,t_now):

    cool_value = -5.9386*(10**-4)*(((t_now-t1)/60)**3) + 5.1754*(10**-2)*(((t_now-t1)/60)**2) -1.6882*((t_now-t1)/60) + 1.4488

    # -5.9386*(10**-4)+ 5.1754*(10**-2)	-1.6882	+ 1.4488
    # -5.9386*(10^-4)	5.1754*(10^-2)	-1.6882	1.4488
    return cool_value

def get_time_list(a):
    l1 = [a[0]]
    l2= []

    for i in range(1,len(a)):
        
        if a[i-1] + 1 == a[i]  :
            l1.append(a[i])
        else:
            l2.append(l1)
            l1 =[]
            l1.append(a[i])
    if l1 :
        l2.append(l1)
    return l2

def heat_func_imaging(t1,t_now):
    heat_value = 3.9805*(((t_now-t1))**3) * (10**-6) -2.0476 *(((t_now-t1))**2)* (10**-3) +	4.473 *((t_now-t1)) * (10**-1) + 5.265 * (10**-1)
    return heat_value

def cool_func_imaging(t1,t_now):

    cool_value = -4.7625*(((t_now-t1))**3) * (10**-7) + 5.79 *(((t_now-t1))**2)* (10**-4) -2.4 * (t_now-t1) * (10**-1) - 3.43 
    return cool_value

def remove_opportunities_conflict_GSpass(a,b,setup_time):
    '''
    a:TW
    b: TW_gsPass_list

    '''
    #print(a,b,setup_time)
    if b == 'NA':
        return 0
    for TW_gspass in b:
        if  not ( (a[1] <= TW_gspass[0] - setup_time)  or \
                  (a[0] >= TW_gspass[1] + setup_time)  ) :
          
            return 1
    return 0


# def get_EcStEnd_list(b,d,df):
#     '''df : eclipse_df ['time_index']
#     '''
#     df1 = df[df['time_index']>=b]
#     df2 = df1[df1['time_index']<=d]
#     if len(df2)!=0:
#         df2.sort_values(by = 'time_index', inplace=True)

#         # a = list(df2['eclipse'].unique())

#         # if len(a)==1:
#         #     return a
#         # else:
#         # eclipse =1 ,sunlit =0
#         df2['m1'] = df2['eclipse'].cummax()
#         df2['m2'] = df2['eclipse'].cummin()
#         df2['m3'] = df2['m2'] + df2['m1'] + df2['eclipse'] 
#         # 3 ,2,1,0 ===> 3 ===> entire prev eclipse current eclipse
#         #               1 ===> m1 = 1, m2 =0, m3=0 , entire prev eclipse and current is sunlit
#         #               0 ====> entires prev sunlit and current is sunlit
#         #               2 ====> m1 = 0,m2 =1 ,m3 =1 ,entire prev sunlit and current is eclipse

#         df2['r'] = df2['m3'].rank(method='dense')
#         df3 = df2.groupby(['eclipse','r']).agg(min_time= ('time_index','min'),\
#                                             max_time = ('time_index','max')).reset_index()
#         df3.sort_values(by='min_time',inplace=True)
#         df3['TW'] = df3[['eclipse','min_time','max_time']].apply(lambda a : [a['eclipse'],a['min_time'],a['max_time']],axis=1)
            
#         return  df3['TW'].to_list()
#     else:
#         return ['NA','NA','NA']
    


def get_EcStEnd_list(b,d,df):
    '''
    df : eclipse_df ['time_index'],'eclipse'
    b: start_time,d: end_time
    '''
    df1 = df[df['time_index']>=b]
    df2 = df1[df1['time_index']<=d]

    if len(df2)!=0:
        df2.sort_values(by = 'time_index', inplace=True)

        # a = list(df2['eclipse'].unique())
        # if len(a)==1:
        #     return a
        # else:
        # eclipse =1 ,sunlit =0
        
        df2['r_sunlit'] = (df2['eclipse']==0).cumsum()
        df2['r_eclipse'] = (df2['eclipse']==1).cumsum() 

        Z1 = df2[df2['eclipse']==0]
        Z2 = df2[df2['eclipse']==1]

        Z1.rename(columns={'r_eclipse':'common_rank'},inplace=True)
        Z2.rename(columns={'r_sunlit':'common_rank'},inplace=True)

        df2 = pd.concat([Z1,Z2])
        df2.sort_values(by='time_index',inplace=True)

        #df2['r'] = df2['m3'].rank(method='dense')
        df3 = df2.groupby(['eclipse','common_rank']).agg(min_time= ('time_index','min'),\
                                            max_time = ('time_index','max')).reset_index()
        df3.sort_values(by='min_time',inplace=True)

        df3['TW'] = df3[['eclipse','min_time','max_time']].apply(lambda a : [a['eclipse'],a['min_time'],a['max_time']],axis=1)

        #print(df3)
            
        return  df3['TW'].to_list()
    else:
        return [['NA','NA','NA']]
    
def cal_cum_sum(sat_grp,eclipse_value,column_name):
        sat_grp[column_name] = (sat_grp['Eclipse'] == eclipse_value ).cumsum()
        return sat_grp
    
def get_readout_TW(power_based_df):
    dedicatedTW_readout_df = power_based_df[power_based_df['gsID'] == 'no_i_no_g']
    dedicatedTW_readout_df.sort_values(by=['SatID','start_time'],inplace=True)

    dedicatedTW_readout_df = dedicatedTW_readout_df.\
                                            groupby('SatID').apply(cal_cum_sum,eclipse_value = 0 ,column_name='r1')\
                                            .reset_index(drop=True)
    
    
    dedicatedTW_readout_df = dedicatedTW_readout_df.\
                                            groupby('SatID').apply(cal_cum_sum,eclipse_value = 1 ,column_name='r2')\
                                            .reset_index(drop=True)
    

    imgGS_union_Dedicated_TW_sunlit_readout_df = dedicatedTW_readout_df[dedicatedTW_readout_df['Eclipse'] == 0]
    imgGS_union_Dedicated_TW_eclipse_readout_df = dedicatedTW_readout_df[dedicatedTW_readout_df['Eclipse'] == 1]
    
    
    imgGS_union_Dedicated_TW_sunlit_readout_df.rename(columns = {'r2':'common_r'},inplace=True)
    imgGS_union_Dedicated_TW_eclipse_readout_df.rename(columns = {'r1':'common_r'},inplace=True)
    dedicatedTW_readout_df = pd.concat([imgGS_union_Dedicated_TW_sunlit_readout_df,\
                                                        imgGS_union_Dedicated_TW_eclipse_readout_df])
    

    imgGS_union_Dedicated_TW_grouped_readout_df = dedicatedTW_readout_df.groupby(['SatID','Eclipse','common_r'])\
                                                                                        .agg(st_time_list = ('start_time',list),\
                                                                                            et_time_list =('end_time',list),\
                                                                                                power_global_TW_index_list =('power_global_TW_index',list),\
                                                                                                memory_global_TW_index_list =('Memory_global_TW_index',list)).reset_index()
    #print("ABC",imgGS_union_Dedicated_TW_grouped_readout_df.columns,imgGS_union_Dedicated_TW_grouped_readout_df)
    
    imgGS_union_Dedicated_TW_grouped_readout_df[['start_time','end_time','power_global_TW_index','Memory_global_TW_index']] = imgGS_union_Dedicated_TW_grouped_readout_df[['st_time_list','et_time_list',\
                                                                                                                                'power_global_TW_index_list','memory_global_TW_index_list']].\
                                                                                                                                apply(lambda a: pd.Series({'start_time': a['st_time_list'][-1],\
                                                                                                                                                        'end_time':a['et_time_list'][-1],\
                                                                                                                                                        'power_global_TW_index':a['power_global_TW_index_list'][-1],\
                                                                                                                                            'Memory_global_TW_index':a['memory_global_TW_index_list'][-1]}),axis=1)
                                                                                            
    dedicated_readout_df = imgGS_union_Dedicated_TW_grouped_readout_df[imgGS_union_Dedicated_TW_grouped_readout_df['Eclipse'] == 0]
    dedicated_readout_df['concat_sat_memoryTWindex'] = dedicated_readout_df['SatID'] + '_' + dedicated_readout_df['Memory_global_TW_index'].astype(str)
    dedicated_readout_df['list_format'] = dedicated_readout_df[['SatID','start_time','end_time']]\
                                            .apply(lambda a: [a['SatID'],a['start_time'],a['end_time']],axis=1)
    
    dedicated_readout_df.sort_values(by='start_time',inplace=True)
    dedicated_readout_df['readout_priority'] = dedicated_readout_df.groupby(['SatID'])['start_time'].rank(method='dense')
    dedicated_readout_df['readout_priority'] = 1/dedicated_readout_df['readout_priority']

    return dedicated_readout_df
    
def get_delivery_time(delivery_type , capturedate):

    if delivery_type =='standard_delivery':
        return capturedate + datetime.timedelta(hours=36)
    if delivery_type =='expedited_delivery':
        return capturedate + datetime.timedelta(hours=24)
    if delivery_type =='super_expedited_delivery':
        return capturedate + datetime.timedelta(hours=12)
        
def evaluate_eqn(t,temp_eqn):

    return eval(temp_eqn,{'t':t})

def get_bucketwise_safe_cool_time(initial_heat_temp,cooling_temp_eqn,interface_temp):
    _time_hrs = 2
    time_index_list = [i for i in range(0,_time_hrs*3600)]
    initial_temp_list = [initial_heat_temp]+[0 for i in range(1,_time_hrs*3600)]

    df = pd.DataFrame({"time_index":time_index_list,\
            "initial_temp": initial_temp_list})
    
    save_dict = {}
    for t_ind in range(0,_time_hrs*3600,10):
        delta_temp = evaluate_eqn(t_ind,cooling_temp_eqn )
        save_dict[t_ind] = delta_temp
        if initial_heat_temp + initial_heat_temp < interface_temp :
            break

    df = df[ df['time_index'] < t_ind ]
    df['delta_temp_cool'] = df['time_index'].apply(lambda a : evaluate_eqn(a,temp_eqn = cooling_temp_eqn  ))
    #df['delta_temp_cool'] = df['time_index'].map(save_dict )


    # df['proxy_cool'] = df['delta_temp_cool'] +   df ['initial_temp'] 
    # df['cumm_cool'] = df['proxy_cool'].cumsum()

    df['proxy_cool'] = df['delta_temp_cool'] +   initial_heat_temp
    df['cumm_cool'] = df['proxy_cool']#.cumsum()

    df = df[df['cumm_cool'] >=interface_temp]

    max_temp_sec = df['time_index'].max()

    return max_temp_sec

def get_thermal_bucket(initial_temp, temp_eqn ,cooling_temp_eqn,thermal_cap_temp):
    '''
    tenp_eqn (for heating): str Expression example: "2*t**3 + 3*t**2 + 5*t + 10 " expression should have variable t and t  will be given as seconds input

    return dict
    '''
    _time_hrs = 2
    time_index_list = [i for i in range(0,_time_hrs*3600)]
    initial_temp_list = [initial_temp]+[0 for i in range(1,_time_hrs*3600)]
    bucket_flag = [ [i]*10 for i in range(0,int(_time_hrs*3600/10))]
    bucket_flag = sum(bucket_flag,[]) # flattening
    


    df = pd.DataFrame({"time_index":time_index_list,\
                "initial_temp": initial_temp_list,\
                    "bucket_flag":bucket_flag})
    save_dict = {}
    for t_ind in range(0,_time_hrs*3600):
        delta_temp = evaluate_eqn(t_ind,temp_eqn )
        save_dict[t_ind] = delta_temp
        if delta_temp + initial_temp >= thermal_cap_temp:
            break
    df = df[df['time_index'] < t_ind]
    
    #df['delta_temp_heat'] = df['time_index'].apply(lambda a : evaluate_eqn(a,temp_eqn = temp_eqn  ))
   
    df['delta_temp_heat'] = df['time_index'].map(save_dict)

    # df['proxy_heat'] = df['delta_temp_heat'] +   df ['initial_temp']
    # df['cumm_heat'] = df['proxy_heat'].cumsum()

    df['proxy_heat'] = df['delta_temp_heat'] +   initial_temp
    df['cumm_heat'] = df['proxy_heat']#.cumsum()


    #print(df,df_grouped_1,"===1=====")
    df = df[df['cumm_heat']<=thermal_cap_temp]

    

    max_temp_sec = df['time_index'].max()

    df_grouped_ = df.groupby(['bucket_flag']).agg(max_time=('time_index','max'),\
                                    min_time = ('time_index','min')).reset_index()
    #print(df.head(50),df_grouped_.head(60),"===2=====")
    
    
    df_heat = pd.merge(df,df_grouped_,on='bucket_flag',how='left')
    
    
    df_cool = df_heat[df_heat['max_time']==df_heat['time_index']]
   
    initial_heat_list = list(df_cool['cumm_heat'])

    safe_cool_time_list = []
    for initial_heat_temp in initial_heat_list:
        safe_cool_time_list.append(get_bucketwise_safe_cool_time(initial_heat_temp,cooling_temp_eqn,initial_temp))
    df_cool['safe_cool_time'] = safe_cool_time_list
    df_cool['safe_cool_time'] = df_cool['safe_cool_time'].fillna(1)
    
    df_cool = df_cool[['bucket_flag','max_time','min_time','safe_cool_time']]

    df_cool['list'] = df_cool[['bucket_flag','max_time','min_time','safe_cool_time']].\
        apply(lambda a : [a['min_time'],a['max_time'],a['safe_cool_time']],axis = 1)
    
    #print(df_cool.head(50))
    max_min_sct__bucket_flag = dict(zip(df_cool['bucket_flag'],df_cool['list']))
    max_min_sct__bucket_flag['max_time_heat'] = t_ind - 1


    return max_min_sct__bucket_flag 

def get_prev_TW_index(to_get_prev_index_df,relavant_column = 'Memory_global_TW_index',grouping_column ='SatID'):
    rolling_window_len = len(to_get_prev_index_df)
    to_get_prev_index_df['prev_TW_index_list']= [x[relavant_column].values.tolist()[::-1] for x in to_get_prev_index_df.rolling(rolling_window_len)]
    to_get_prev_index_df['prev_endTime_list']= [x['end_time'].values.tolist()[::-1] for x in to_get_prev_index_df.rolling(rolling_window_len)]

    #to_get_prev_index_df['prev_TW_index_list'] = to_get_prev_index_df[['start_time','prev_TW_index_list','prev_endTime_list']].apply(lambda a : [a['prev_TW_index_list'][0]] + [v for i,v in enumerate(a['prev_TW_index_list']) if a['prev_endTime_list'][i] <= a['start_time']],axis=1)
    to_get_prev_index_df['concat'] = to_get_prev_index_df[grouping_column] + '_' + to_get_prev_index_df[relavant_column].astype(str)
    this_dict = dict(zip(to_get_prev_index_df['concat'] ,to_get_prev_index_df['prev_TW_index_list'] ))

    return this_dict

def get_active_assured_task(image_oppr_df,data):
    assured_tasking_dd = data['assured_tasking_basedOnDueDateEmergency_list']
    assured_tasking_input = data['assured_tasking_based_on_input_list']
    assured_tasking_list = assured_tasking_dd + assured_tasking_input

    Aoi_grouped_df = image_oppr_df.groupby('AoiID').agg(unique_strip = ('encoded_stripId',pd.Series.nunique)).reset_index()
    unique_strip_len__aoiID = dict(zip(Aoi_grouped_df['AoiID'],Aoi_grouped_df['unique_strip']))
    
    image_oppr_df = image_oppr_df[image_oppr_df['encoded_stripId'].isin(assured_tasking_list)]
    image_oppr_df['inflated_priority'] = image_oppr_df['encoded_stripId'].apply(lambda a :1000 if a in assured_tasking_input else 10 )

    inflating_factor__csjk = dict(zip(image_oppr_df['concat_SatID_encodedStripId_TWindex'],image_oppr_df['inflated_priority']))
 
    conflicting_sjk_list = [k for k,v in data['cs1j2k2Domainlist__cs1j1k1'].items() if v ]

    assured_task_eligible_df = image_oppr_df[~image_oppr_df['concat_SatID_encodedStripId_TWindex'].isin(conflicting_sjk_list)]
    Aoi_grouped_assured_task_eligible_df  = assured_task_eligible_df.groupby('AoiID').agg(this_unique_strip_len = ('encoded_stripId',pd.Series.nunique)).reset_index()
 
    Aoi_grouped_assured_task_eligible_df['original_len'] = Aoi_grouped_assured_task_eligible_df['AoiID'].map(unique_strip_len__aoiID)
    Aoi_grouped_assured_task_eligible_df = Aoi_grouped_assured_task_eligible_df[Aoi_grouped_assured_task_eligible_df['original_len'] == Aoi_grouped_assured_task_eligible_df['this_unique_strip_len']]
    Aoi_grouped_unequal_df = Aoi_grouped_assured_task_eligible_df[Aoi_grouped_assured_task_eligible_df['original_len'] != Aoi_grouped_assured_task_eligible_df['this_unique_strip_len']]
    non_con_flicted_but_unequalStrip_list = list(Aoi_grouped_unequal_df['AoiID'].unique())
    assured_task_unequal_df = assured_task_eligible_df[assured_task_eligible_df['AoiID'].isin(non_con_flicted_but_unequalStrip_list)]
    #assured_task_unequal_strip_list = list(assured_task_unequal_df['encoded_stripId'].unique())

    non_con_flicted_AOIids_list = list(Aoi_grouped_assured_task_eligible_df['AoiID'].unique())
    assured_task_eligible_df = assured_task_eligible_df[assured_task_eligible_df['AoiID'].isin(non_con_flicted_AOIids_list)]
    assured_task_non_conflicting_list = assured_task_eligible_df['encoded_stripId'].unique()

    image_oppr_df = image_oppr_df[image_oppr_df['concat_SatID_encodedStripId_TWindex'].isin(conflicting_sjk_list)]
    image_oppr_df = pd.concat([assured_task_unequal_df,image_oppr_df])
    #image_oppr_df = image_oppr_df.drop_duplicates()
    all_assured_possible_active_list = list(image_oppr_df['concat_SatID_encodedStripId_TWindex'].unique())
    all_assured_possible_active_strip_list = list(image_oppr_df['encoded_stripId'].unique())
    all_assured_possible_active_Aoi_list = list(image_oppr_df['AoiID'].unique())

    image_oppr_grouped_df = image_oppr_df.groupby('AoiID').agg(strip_set = ('encoded_stripId',set),\
                                                               len_strip = ('encoded_stripId',pd.Series.nunique)).reset_index()
    stripSet__AOI = dict(zip(image_oppr_grouped_df['AoiID'],image_oppr_grouped_df['strip_set']))
    stripSetLen__AOI = dict(zip(image_oppr_grouped_df['AoiID'],image_oppr_grouped_df['len_strip']))



    prob = LpProblem("Active_assurance_plan", LpMaximize)
    x_o = {'x_'+sjk : LpVariable('x_'+sjk, cat='Binary' ) for sjk in all_assured_possible_active_list }
    y_o = {'y_'+j : LpVariable('y_'+j, cat='Binary' ) for j in all_assured_possible_active_strip_list }
    a_o = {'z_'+a : LpVariable('z_'+a, cat='Binary' ) for a in all_assured_possible_active_Aoi_list }

    for sjk in all_assured_possible_active_list:
        l1 = data['cs1j2k2Domainlist__cs1j1k1'][sjk]
        for s1j1k1 in l1:
            if s1j1k1 in all_assured_possible_active_list:
                prob += x_o['x_'+sjk] + x_o['x_'+s1j1k1] <= 1 

    # one job selection
    for j in all_assured_possible_active_strip_list :
        prob += lpSum([x_o['x_'+sjk ] for sjk in data['csjkList__j'][j]]) <= y_o['y_'+j ] 

    # one job selection
    for aoi in all_assured_possible_active_Aoi_list :
        prob += lpSum([y_o['y_'+j ] for j in stripSet__AOI[aoi]]) == stripSetLen__AOI[aoi] * a_o['z_'+aoi] 

    prob += lpSum([x_o['x_'+sjk] * data['TotalPriority__csjk'][sjk] * inflating_factor__csjk[sjk] for sjk in all_assured_possible_active_list])

    solver = getSolver('HiGHS', msg = False)
    status=prob.solve(solver)
    #status=self.prob.solve()
    print("status_assured_tasking=",LpStatus[status])

    if LpStatus[status] =='Optimal':
        active_assured_oppr_list = [sjk for sjk in all_assured_possible_active_list if x_o['x_'+sjk].value()==1]

    image_oppr_df = image_oppr_df[image_oppr_df['concat_SatID_encodedStripId_TWindex'].isin(active_assured_oppr_list)]
    assured_strip_id_list = list(image_oppr_df['encoded_stripId'].unique())
    assured_strip_id_list = set(assured_strip_id_list + non_con_flicted_AOIids_list)
    print(assured_strip_id_list)
      

    return assured_strip_id_list

def get_eclipse_data(eclipse_df,config):
    #eclipse_df = pd.read_csv('1_input_data/filtered_eclipse_data.csv')
    #config['base_time_stamp_downlink'] = config['base_time_stamp_downlink']

    eclipse_df['base_time'] = config['base_time_stamp_downlink']
    eclipse_df['time_index_start'] = pd.to_datetime(eclipse_df['start_time']) - pd.to_datetime(eclipse_df['base_time'])
    eclipse_df['time_index_end'] = pd.to_datetime(eclipse_df['end_time']) - pd.to_datetime(eclipse_df['base_time'])
    eclipse_df['time_index_start'] = eclipse_df[['time_index_start']].apply(lambda a : a['time_index_start'].total_seconds(),axis=1)
    eclipse_df['time_index_end'] = eclipse_df[['time_index_end']].apply(lambda a : a['time_index_end'].total_seconds(),axis=1)

    eclipse_df['time_index_start'] = eclipse_df['time_index_start'].astype(int)
    eclipse_df['time_index_end'] = eclipse_df['time_index_end'].astype(int)
    eclipse_df = eclipse_df[['SatID','time_index_start','time_index_end','eclipse']]
    eclipse_df.sort_values(by='time_index_start',inplace=True)
    eclipse_df['till_now_max'] = eclipse_df.groupby('SatID')['time_index_end'].cummax()
    eclipse_df['prev_max'] = eclipse_df.groupby('SatID')['till_now_max'].shift(1)

    eclipse_df1 = eclipse_df[eclipse_df['time_index_start'] > eclipse_df['prev_max'] + 1] 
    eclipse_df1['time_index_start1'] = eclipse_df1['prev_max'] + 1 #TODO1 +1 is okay ?
    eclipse_df1['time_index_end1'] = eclipse_df1['time_index_start'] - 1
    eclipse_df1['eclipse'] = 0
    eclipse_df1['time_index_start1'] = eclipse_df1['time_index_start1'].astype(int)
    eclipse_df1['time_index_end1'] = eclipse_df1['time_index_end1'].astype(int)
    #print(eclipse_df1)

    eclipse_df1 = eclipse_df1.drop(['time_index_start', 'time_index_end','till_now_max','prev_max'], axis=1)
    eclipse_df1.rename(columns={'time_index_start1':'time_index_start','time_index_end1':'time_index_end'},inplace=True)
    #imgGS_union_df1 ==> contains TW without img and without gs pass  table without eclipse divide
    eclipse_df = pd.concat([eclipse_df1,eclipse_df])
    #print(eclipse_df)

    eclipse_df['time_index'] = eclipse_df[['time_index_start','time_index_end']].apply(lambda a : [i for i in range(a['time_index_start'],a['time_index_end']+1)],axis=1)
    eclipse_df =  eclipse_df.explode('time_index')
    eclipse_df = eclipse_df[['SatID','time_index','eclipse']]

    return eclipse_df


def get_conflicting_dict(df,data_dict,different_setup_time,conflicting_on = 'GsID',concat_filter ='concat_gsid_satid_TWIndex',LOS_column='LOSOffset',AOS_column='AOSOffset'):

    for on_item in df[conflicting_on].unique():
        this_df = df[df[conflicting_on] == on_item ]
        # if different_master_key:
        #     data_dict[different_master_key]['sgk_list'] [on_item] = this_df[concat_filter].unique()
        for csgk in this_df[concat_filter].unique():
            that_df = this_df[this_df[concat_filter] == csgk]
            this_LOS = list(that_df[LOS_column].unique())[0]
            this_AOS = list(that_df[AOS_column].unique())[0]


            that_df1 = this_df[this_df[AOS_column] >= different_setup_time  + this_LOS]
            that_df2 = this_df[this_df[LOS_column] <= this_AOS - different_setup_time]
            that_df3 = pd.concat([that_df1,that_df2])
            
            not_needed = list(that_df3[concat_filter].unique())
            that_df = this_df[~this_df[concat_filter].isin(not_needed)]

            that_df = that_df[that_df[concat_filter] != csgk]

            # if different_master_key:
            #     data_dict[different_master_key]['domain_of_csgk'] [csgk] = list(that_df[concat_filter].unique())
            # else:
            data_dict[csgk] = list(that_df[concat_filter].unique())
                
    return data_dict 
#assured_strip_id_list = get_active_assured_task(image_oppr_df,data)
    


 # rolling_window_len = len(to_get_power_prev_index_df)
# to_get_power_prev_index_df['prev_TW_index_list']= [x['power_global_TW_index'].values.tolist()[::-1] for x in to_get_power_prev_index_df.rolling(rolling_window_len)]
# to_get_power_prev_index_df['prev_endTime_list']= [x['end_time'].values.tolist()[::-1] for x in to_get_power_prev_index_df.rolling(rolling_window_len)]

# #to_get_prev_index_df['prev_TW_index_list'] = to_get_prev_index_df[['start_time','prev_TW_index_list','prev_endTime_list']].apply(lambda a : [a['prev_TW_index_list'][0]] + [v for i,v in enumerate(a['prev_TW_index_list']) if a['prev_endTime_list'][i] <= a['start_time']],axis=1)

# to_get_power_prev_index_df['concat'] = to_get_power_prev_index_df['SatID'] + '_' + to_get_power_prev_index_df['power_global_TW_index'].astype(str)
# this_dict = dict(zip(to_get_power_prev_index_df['concat'] ,to_get_power_prev_index_df['prev_TW_index_list'] ))
# prev_power_tWList__s_TWI_dict__s[s] = this_dict
