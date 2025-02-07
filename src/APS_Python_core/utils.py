import pandas as pd
import datetime

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

def check_opportunities_conflict_GSpass(TW,TW_list_gspass,GS_list,TW_index_gs_list,setup_time):
    l1 = []
    if TW_list_gspass == 'NA':
        return 0
    for i,TW_gspass in enumerate(TW_list_gspass):
        if  not ( (TW[1] <= TW_gspass[0] - setup_time)  or \
                  (TW[0] >= TW_gspass[1] + setup_time)  ) :
            
            l1.append([GS_list[i],TW_index_gs_list[i],TW_gspass])
          
    return l1
    

def get_EcStEnd_list(b,d,df):
    '''
    df : eclipse_df ['time_index'],'eclipse'
    b: start_time,d: end_time
    '''
    df1 = df[df['time_index']>=b]
    df2 = df1[df1['time_index']<=d]

    if len(df2)!=0:
        df2.sort_values(by = 'time_index', inplace=True)
        
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

        return  df3['TW'].to_list()
    else:
        return [['NA','NA','NA']]
    
def cal_cum_sum(sat_grp,eclipse_value,column_name):
        sat_grp[column_name] = (sat_grp['eclipse'] == eclipse_value ).cumsum()
        return sat_grp
    
def get_readout_TW(power_based_df):
    dedicatedTW_readout_df = power_based_df[power_based_df['gs_id'] == 'no_i_no_g']
    dedicatedTW_readout_df.sort_values(by=['sat_id','start_time'],inplace=True)

    dedicatedTW_readout_df = dedicatedTW_readout_df.\
                                            groupby('sat_id').apply(cal_cum_sum,eclipse_value = 0 ,column_name='r1')\
                                            .reset_index(drop=True)
    
    
    dedicatedTW_readout_df = dedicatedTW_readout_df.\
                                            groupby('sat_id').apply(cal_cum_sum,eclipse_value = 1 ,column_name='r2')\
                                            .reset_index(drop=True)
    

    imgGS_union_Dedicated_TW_sunlit_readout_df = dedicatedTW_readout_df[dedicatedTW_readout_df['eclipse'] == 0]
    imgGS_union_Dedicated_TW_eclipse_readout_df = dedicatedTW_readout_df[dedicatedTW_readout_df['eclipse'] == 1]
    
    
    imgGS_union_Dedicated_TW_sunlit_readout_df.rename(columns = {'r2':'common_r'},inplace=True)
    imgGS_union_Dedicated_TW_eclipse_readout_df.rename(columns = {'r1':'common_r'},inplace=True)
    dedicatedTW_readout_df = pd.concat([imgGS_union_Dedicated_TW_sunlit_readout_df,\
                                                        imgGS_union_Dedicated_TW_eclipse_readout_df])
    

    imgGS_union_Dedicated_TW_grouped_readout_df = dedicatedTW_readout_df.groupby(['sat_id','eclipse','common_r'])\
                                                                                        .agg(st_time_list = ('start_time',list),\
                                                                                            et_time_list =('end_time',list),\
                                                                                                power_global_tw_index_list =('power_global_tw_index',list),\
                                                                                                memory_global_tw_index_list =('memory_global_tw_index',list)).reset_index()
    
    imgGS_union_Dedicated_TW_grouped_readout_df[['start_time','end_time','power_global_tw_index','memory_global_tw_index']] = imgGS_union_Dedicated_TW_grouped_readout_df[['st_time_list','et_time_list',\
                                                                                                                                'power_global_tw_index_list','memory_global_tw_index_list']].\
                                                                                                                                apply(lambda a: pd.Series({'start_time': a['st_time_list'][-1],\
                                                                                                                                                        'end_time':a['et_time_list'][-1],\
                                                                                                                                                        'power_global_tw_index':a['power_global_tw_index_list'][-1],\
                                                                                                                                            'memory_global_tw_index':a['memory_global_tw_index_list'][-1]}),axis=1)
                                                                                            
    dedicated_readout_df = imgGS_union_Dedicated_TW_grouped_readout_df[imgGS_union_Dedicated_TW_grouped_readout_df['eclipse'] == 0]
    dedicated_readout_df['concat_sat_memory_tw_index'] = dedicated_readout_df['sat_id'] + '_' + dedicated_readout_df['memory_global_tw_index'].astype(str)
    dedicated_readout_df['list_format'] = dedicated_readout_df[['sat_id','start_time','end_time']]\
                                            .apply(lambda a: [a['sat_id'],a['start_time'],a['end_time']],axis=1)
    
    dedicated_readout_df.sort_values(by='start_time',inplace=True)
    dedicated_readout_df['readout_priority'] = dedicated_readout_df.groupby(['sat_id'])['start_time'].rank(method='dense')
    dedicated_readout_df['readout_priority'] = 1/dedicated_readout_df['readout_priority']

    return dedicated_readout_df
    
def get_delivery_time(delivery_type , capturedate):

    if delivery_type =='standard_delivery':
        return capturedate + datetime.timedelta(hours=36)
    if delivery_type =='expedited_delivery':
        return capturedate + datetime.timedelta(hours=24)
    if delivery_type =='super_expedited_delivery':
        return capturedate + datetime.timedelta(hours=12)
        

def get_prev_TW_index(to_get_prev_index_df,relavant_column = 'memory_global_tw_index',grouping_column ='sat_id'):
    rolling_window_len = len(to_get_prev_index_df)
    to_get_prev_index_df['prev_tw_index_list']= [x[relavant_column].values.tolist()[::-1] for x in to_get_prev_index_df.rolling(rolling_window_len)]
    to_get_prev_index_df['prev_end_time_list']= [x['end_time'].values.tolist()[::-1] for x in to_get_prev_index_df.rolling(rolling_window_len)]

    to_get_prev_index_df['concat'] = to_get_prev_index_df[grouping_column] + '_' + to_get_prev_index_df[relavant_column].astype(str)
    this_dict = dict(zip(to_get_prev_index_df['concat'] ,to_get_prev_index_df['prev_tw_index_list'] ))

    return this_dict


def get_eclipse_data(eclipse_df,config):

    eclipse_df['base_time'] = config['base_time_stamp_downlink'] # str(yyyy-mm-dd hh:mm:ss)
    eclipse_df['time_index_start'] = pd.to_datetime(eclipse_df['start_time']) - pd.to_datetime(eclipse_df['base_time'])
    eclipse_df['time_index_end'] = pd.to_datetime(eclipse_df['end_time']) - pd.to_datetime(eclipse_df['base_time'])
    eclipse_df['time_index_start'] = eclipse_df[['time_index_start']].apply(lambda a : a['time_index_start'].total_seconds(),axis=1)
    eclipse_df['time_index_end'] = eclipse_df[['time_index_end']].apply(lambda a : a['time_index_end'].total_seconds(),axis=1)

    eclipse_df['time_index_start'] = eclipse_df['time_index_start'].astype(int)
    eclipse_df['time_index_end'] = eclipse_df['time_index_end'].astype(int)
    eclipse_df = eclipse_df[['sat_id','time_index_start','time_index_end','eclipse']]
    eclipse_df.sort_values(by='time_index_start',inplace=True)
    eclipse_df['till_now_max'] = eclipse_df.groupby('sat_id')['time_index_end'].cummax()
    eclipse_df['prev_max'] = eclipse_df.groupby('sat_id')['till_now_max'].shift(1)

    eclipse_df1 = eclipse_df[eclipse_df['time_index_start'] > eclipse_df['prev_max'] + 1] 
    eclipse_df1['time_index_start1'] = eclipse_df1['prev_max'] + 1 #TODO1 +1 is okay ?
    eclipse_df1['time_index_end1'] = eclipse_df1['time_index_start'] - 1
    eclipse_df1['eclipse'] = 0
    eclipse_df1['time_index_start1'] = eclipse_df1['time_index_start1'].astype(int)
    eclipse_df1['time_index_end1'] = eclipse_df1['time_index_end1'].astype(int)

    eclipse_df1 = eclipse_df1.drop(['time_index_start', 'time_index_end','till_now_max','prev_max'], axis=1)
    eclipse_df1.rename(columns={'time_index_start1':'time_index_start','time_index_end1':'time_index_end'},inplace=True)
    eclipse_df = pd.concat([eclipse_df1,eclipse_df])

    eclipse_df['time_index'] = eclipse_df[['time_index_start','time_index_end']].apply(lambda a : [i for i in range(a['time_index_start'],a['time_index_end']+1)],axis=1)
    eclipse_df =  eclipse_df.explode('time_index')
    eclipse_df = eclipse_df[['sat_id','time_index','eclipse']]

    return eclipse_df


def get_conflicting_dict(df,data_dict,different_setup_time,conflicting_on = 'gs_id',concat_filter ='concat_gsid_satid_twindex',LOS_column='los_offset',AOS_column='aos_offset'):
    for on_item in df[conflicting_on].unique():
        this_df = df[df[conflicting_on] == on_item ]
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

            data_dict[csgk] = list(that_df[concat_filter].unique())
                
    return data_dict 

