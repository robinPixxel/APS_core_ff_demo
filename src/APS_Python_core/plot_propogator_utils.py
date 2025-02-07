
import pandas as pd
import plotly.graph_objects as go
import matplotlib.pyplot as plt
from APS_Python_core.themal_buckets import evaluate_cool_eqn




def correct_delta(this_df,delta_col = "delta_power" , initial_col = "initial_power" , cap_col = "power_cap" , lower_cap_col = "power_lower_cap" ):

    corrected_schedule_power_df = pd.DataFrame()
    for satid in this_df["sat_id"].unique():
        that_df = this_df[this_df['sat_id']==satid]
        delta_power_list = list(that_df[delta_col])
        initial_power = list(that_df[initial_col])[0]
        power_cap = list(that_df[cap_col])[0]
        lower_cap = list(that_df[lower_cap_col])[0]
        current_power = initial_power

        for i,this_value in enumerate(delta_power_list) :
            starting_power = current_power
            current_power += this_value 

            if delta_col == "delta_power":
                if current_power >=  power_cap:
                    delta_power_list[i] = power_cap - starting_power
                    current_power = power_cap

            if delta_col in  ["delta_camera_detector","delta_xbt","delta_nccm"]:
                if current_power <= lower_cap:
                    delta_power_list[i] =   lower_cap - starting_power
                    current_power = lower_cap
        
        
        that_df[delta_col] = delta_power_list
        corrected_schedule_power_df = pd.concat([corrected_schedule_power_df,that_df])

    final_schedule_power_df = corrected_schedule_power_df.copy()
    final_schedule_power_df.sort_values(by = 'start_time' , inplace = True)

    return final_schedule_power_df


def get_delta_power(operation,duration,Eclipse,sunlit_power_generate_rate,eclipse_power_consumption_rate,sunlit_power_consume_rate):

    if operation == "Imaging" and Eclipse == 0 :
        return duration * (sunlit_power_generate_rate - sunlit_power_consume_rate)
    if operation == "Imaging" and Eclipse == 1 :
        return duration * (sunlit_power_generate_rate - sunlit_power_consume_rate)
    
    if operation == "downlinking_from_Readout" and Eclipse == 0 :
        return duration * (sunlit_power_generate_rate -sunlit_power_consume_rate )  
    elif operation == "downlinking_from_Readout" and Eclipse == 1:
        return duration * (eclipse_power_consumption_rate) * -1 
    
    if operation == "Readout" and Eclipse == 0 :
        return  duration * (sunlit_power_generate_rate - sunlit_power_consume_rate)
    elif operation == "Readout" and Eclipse == 1 :
        return duration * (eclipse_power_consumption_rate) * -1 
    
    if operation == "idle" and Eclipse == 0 :
        return  duration * (sunlit_power_generate_rate - sunlit_power_consume_rate)
    elif operation == "idle" and Eclipse == 1 :
        return duration * (eclipse_power_consumption_rate) * -1   
    

def get_thermal_delta_list(final_schedule_df ,need_thermal_operation = "Imaging",heat_eqn_column = "camera_detector_heat_eqn", cool_eqn_col = "camera_detector_cool_eqn",initial_temp_col = "initial_camera_detector_temp" , a_cool_parameter_col = "camera_detector_a_cool_parameter" , b_cool_parameter_col = "camera_detector_b_cool_parameter" , delta_col = "delta_camera_detector"):
    #final_camera_detector_list = []
    new_final_schedule_df = pd.DataFrame()
    final_schedule_df.sort_values(by = 'start_time')
    for s in final_schedule_df["sat_id"].unique():
        this_camera_detector_sat_df = final_schedule_df[final_schedule_df["sat_id"] == s ]
        operation_list = this_camera_detector_sat_df["operation"].to_list()
        duration_list = this_camera_detector_sat_df["duration"].to_list()
        camera_detector_heat_eqn_list = this_camera_detector_sat_df[heat_eqn_column].to_list()
        initial_camera_detector_temp_list = this_camera_detector_sat_df[initial_temp_col].to_list()

        a_cool_parameter_list = this_camera_detector_sat_df[a_cool_parameter_col].to_list()
        b_cool_parameter_list = this_camera_detector_sat_df[b_cool_parameter_col].to_list()
        cool_eqn_list = this_camera_detector_sat_df[cool_eqn_col].to_list()

        camera_detector_delta_temp_list = get_thermal_delta_list_sat_wise(operation_list,duration_list,initial_camera_detector_temp_list,camera_detector_heat_eqn_list,a_cool_parameter_list,b_cool_parameter_list,cool_eqn_list, need_operation = need_thermal_operation )
        this_camera_detector_sat_df[delta_col] = camera_detector_delta_temp_list
        new_final_schedule_df = pd.concat([new_final_schedule_df,this_camera_detector_sat_df])

    return new_final_schedule_df


def get_thermal_delta_list_sat_wise(operation_list,duration_list,initial_temp_threshold_list,thermal_eqn_heat_list,a_cool_parameter_list, b_cool_parameter_list , cool_eqn_list , need_operation ="Imaging" ):

    delta_temp_list = []
    initial_temp_threshold = initial_temp_threshold_list[0]
    last_heat_temp = [initial_temp_threshold_list[0]]
    final_temp = initial_temp_threshold_list[0]

    for i,operation in enumerate(operation_list):
        if operation == need_operation:
            t = duration_list[i] 
            delta_heat = eval(thermal_eqn_heat_list[i])
            final_temp += delta_heat
            delta_temp_list.append(delta_heat)
            last_heat_temp.append(final_temp)

        else : 

            c = 0 
            if final_temp <= initial_temp_threshold :
                final_temp = initial_temp_threshold
                delta_temp_list.append(0)
            else:
                while final_temp >= initial_temp_threshold and c <= duration_list[i] and c <= 1000 : 
                    #initial_heat_temp , final_temp , interface_temp , duration , a_cool_parameter , b_cool_parameter , cool_eqn , operation = "Imaging" 
                    cool_temp = evaluate_cool_eqn(last_heat_temp[-1] , final_temp , initial_temp_threshold , c , a_cool_parameter_list[i], b_cool_parameter_list[i] , cool_eqn_list[i] , need_operation)
                        
                    #cool_temp = eval( t , T_c = final_temp , Teh = last_heat_temp[-1] )
                    final_temp = last_heat_temp[-1] + cool_temp
                    c += 1

                if c <= 1000:
                    delta_temp_list.append(cool_temp)
                else:
                    delta_temp_list.append(initial_temp_threshold - last_heat_temp[-1] )

   
    return delta_temp_list

    
def get_delta_memory(operation,imaging_rate,readout_rate,duration):
    if operation=='Imaging':
        return duration * imaging_rate
    elif operation=='Readout':
        return duration * readout_rate * -1
    else:
        return 0
    
def get_cummulative_power(x):
    l1 =[]
    ch = list(x["initial_power"].unique())[0]
    b_list = list(x["delta_power"])
    threshold = list(x["power_cap"].unique())[0]

    for i,j in enumerate(b_list):
        if ch+ j <= threshold :
            l1.append(ch + j )
        else:
            l1.append(threshold )
        ch = l1[-1]
    x["end_power"] = l1
    return x["end_power"] 


def map_dict():

    return {'imaging':"Imaging",\
            "downlinking":"downlinking_from_Readout",\
            "readout":"Readout",\
            "idle":"idle"} 


def get_df(final_schedule_df,end_col_name = "end_camera_mem", delta_col_name = "delta_camera_memory", start_col_name = "start_camera_mem"  ,initial_col_name = "camera_initial_memory"):

    if end_col_name == "end_power":
        final_schedule_df[end_col_name] = final_schedule_df[["sat_id","delta_power","initial_power","power_cap"]].groupby("sat_id").apply(get_cummulative_power).reset_index(drop=True)
    else:    
        final_schedule_df[end_col_name] = final_schedule_df.groupby("sat_id")[delta_col_name].cumsum()
        final_schedule_df[end_col_name] = final_schedule_df[end_col_name] + final_schedule_df[initial_col_name]

    final_schedule_df[start_col_name] = final_schedule_df.groupby("sat_id")[end_col_name].shift(1)
    null_ones_df = final_schedule_df[final_schedule_df[start_col_name].isnull()]
    not_null_ones_df = final_schedule_df[~final_schedule_df[start_col_name].isnull()]
    null_ones_df[start_col_name] = null_ones_df[initial_col_name]
    final_schedule_df = pd.concat([null_ones_df,not_null_ones_df])

    return final_schedule_df



def generate_profile_plots(final_schedule_df,start_col_name = "start_camera_mem" ,end_col_name = "end_camera_mem" , min_cap_col = "memory_min_cap",max_cap_col ="camera_memory_cap",title_name = "camera_memory" ):
    fig = go.Figure()
    c_list = ["blue","gray","red"]
    colors_dict_sat = { s:c_list[i]  for i,s in enumerate(list(final_schedule_df['sat_id'].unique())) }

   
    for satid in final_schedule_df["sat_id"].unique():
        plt.figure(figsize=(60, 60))
        this_plot_df = final_schedule_df[final_schedule_df['sat_id']==satid]

        fig.add_trace(go.Scatter(
                        x=this_plot_df[['start_time', 'end_time']].values.flatten(), 
                        y=this_plot_df[[start_col_name, end_col_name]].values.flatten(),
                        mode='lines',
                        name=f'SATID {satid} :' + title_name ,
                        line=dict(color=colors_dict_sat[satid], dash='dash'),  # Dashed line for the time period
                                    ))
        fig.add_trace(go.Scatter(
                        x=this_plot_df[['start_time']].values.flatten(), 
                        y=this_plot_df[[max_cap_col]].values.flatten(),
                        mode='lines',
                        name=f'SATID {satid} :' + "_limits" ,
                        line=dict(color=colors_dict_sat[satid], dash='solid'),  # Dashed line for the time period
                                    ))
        
        # fig.add_trace(go.Scatter(
        #                 x=this_plot_df[['start_time', 'end_time']].values.flatten(), 
        #                 y=this_plot_df[['start_ssd_mem', 'end_ssd_mem']].values.flatten(),
        #                 mode='lines',
        #                 name=f'SATID {satid} :SSD ',
        #                 line=dict(color=colors_dict_sat[satid], dash='solid'),  # Dashed line for the time period
        #                             ))
    fig.update_layout(
        title= title_name + 'for Each SATID',
        xaxis_title='Time',
        yaxis_title= title_name + 'Value',
        xaxis=dict(
            type='date',
            tickformat='%Y-%m-%d %H:%M',
            dtick="3600000",  # 1 hour interval
        ),
        showlegend=True,
    )
    return fig,final_schedule_df


def get_overlap_plots(fig,this_plot_df,color_id,satid,legend_Group = "All_Strips",title_name = "Strip to Strip Status",column_name ="strip_id",y_axis = "Strips"):
    """ 
    y axis =  StripID   
    title_name = Strip - Strip Status
    column_name = StripID
    legend_Group : AllStrips, Selected Strips

    y axis =  GsID   
    title_name = Strips To GsID Status
    column_name = GsID  
    legend_Group : All GSID , Selected GSID         
   
    y axis =  sat_id   
    title_name = GsID to sat_id Status
    column_name = sat_id
    legend_Group : All sat_id , Selected sat_id
    """
    
    l1 = this_plot_df[[column_name,column_name]].values.flatten()
    l1 = [[l1[i],l1[i+1]] for i in range(0,len(l1),2)]
    l2 = this_plot_df[['opportunity_start_time',"opportunity_end_time"]].values.flatten()
    l2 = [[l2[i],l2[i+1]] for i in range(0,len(l2),2)]
   
    for i in range(len(l1)):
        if i == len(l1)-1:
            fig.add_trace(go.Scatter(
                            x = l2[i], 
                            y = l1[i],
                            mode='markers',
                            name = satid +'_' + legend_Group ,
                            line=dict(color = color_id, dash='solid',width = 50000 ),  # Dashed line for the time period
                            legendgroup=legend_Group+"_"+ satid,\
                            showlegend= True))
        else:
             fig.add_trace(go.Scatter(
                            x = l2[i], 
                            y = l1[i],
                            mode='markers',
                            name=satid + "_" + legend_Group ,
                            line=dict(color=color_id, dash='solid',width = 50000 ),  # Dashed line for the time period
                            legendgroup=legend_Group+"_"+ satid,\
                            showlegend= False))
     
    fig.update_layout(
            title= title_name,
            xaxis_title='Time',
            yaxis_title= y_axis,
            xaxis=dict(
                type='date',
                tickformat='%Y-%m-%d %H:%M:%S',
                dtick="3600000",  # 1 hour interval
            ),
            showlegend=True,
            legend=dict(
            title = legend_Group,
            orientation='v',  # Horizontal orientation for the legend
            x=-0.1,  # Position legend at the center horizontally
            xanchor='right',  # Anchor legend at the center
            y=-0.1,  # Position the legend below the plot
            yanchor='bottom',  # Anchor legend at the top of the legend
            font=dict(size=12)  # Legend font size
                                )
        )
    return fig

