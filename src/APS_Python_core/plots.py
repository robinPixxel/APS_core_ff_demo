import pandas as pd
import random
from APS_Python_core.utils import get_EcStEnd_list
import matplotlib.pyplot as plt

from APS_Python_core.plot_propogator_utils import correct_delta,get_delta_power,get_thermal_delta_list,get_delta_memory,map_dict,get_df,generate_profile_plots,get_overlap_plots

global color_names
color_names = [
    "AliceBlue", "AntiqueWhite", "Aqua", "Aquamarine", "Azure", "Beige", "Bisque", 
    "Black", "BlanchedAlmond", "Blue", "BlueViolet", "Brown", "BurlyWood", "CadetBlue", 
    "Chartreuse", "Chocolate", "Coral", "CornflowerBlue", "Cornsilk", "Crimson", "Cyan", 
    "DarkBlue", "DarkCyan", "DarkGoldenrod", "DarkGray", "DarkGreen", "DarkKhaki", "DarkMagenta", 
    "DarkOliveGreen", "DarkOrange", "DarkOrchid", "DarkRed", "DarkSalmon", "DarkSeaGreen", 
    "DarkSlateBlue", "DarkSlateGray", "DarkTurquoise", "DarkViolet", "DeepPink", "DeepSkyBlue", 
    "DimGray", "DodgerBlue", "Firebrick", "FloralWhite", "ForestGreen", "Fuchsia", "Gainsboro", 
    "GhostWhite", "Gold", "Goldenrod", "Gray", "Green", "GreenYellow", "Honeydew", "HotPink", 
    "IndianRed", "Indigo", "Ivory", "Khaki", "Lavender", "LavenderBlush", "LawnGreen", "Lime", 
    "LimeGreen", "Linen", "Magenta", "Maroon", "MediumAquaMarine", "MediumBlue", "MediumOrchid", 
    "MediumPurple", "MediumSeaGreen", "MediumSlateBlue", "MediumSpringGreen", "MediumTurquoise", 
    "MediumVioletRed", "MidnightBlue", "MintCream", "MistyRose", "Moccasin", "NavajoWhite", 
    "Navy", "OldLace", "Olive", "OliveDrab", "Orange", "OrangeRed", "Orchid", "PaleGoldenrod", 
    "PaleGreen", "PaleTurquoise", "PaleVioletRed", "PapayaWhip", "PeachPuff", "Peru", "Pink", 
    "Plum", "PowderBlue", "Purple", "RebeccaPurple", "Red", "RosyBrown", "RoyalBlue", "SaddleBrown", 
    "Salmon", "SandyBrown", "SeaGreen", "SeaShell", "Sienna", "Silver", "SkyBlue", "SlateBlue", 
    "SlateGray", "Snow", "SpringGreen", "SteelBlue", "Tan", "Teal", "Thistle", "Tomato", "Turquoise", 
    "Violet", "Wheat", "White", "WhiteSmoke", "Yellow", "YellowGreen"
]


#========================================================================================================
        


def plot_memory_power_thermal(schedule_df,memory_data_df,mem_transfer_SatLvlData_df,\
                              power_data_df,power_transfer_data,eclipse_df_dict,thermal_data_df):
    """ 
    schedule_df : 'sat_id','start_time'(int/float),'end_time'(int/float),'operation'(Imaging,Readout,downlinking_from_Readout),"base_time"
    memory_data_df :sat_id,memory_device,initial_memory,memory_cap)
    mem_transfer_SatLvlData_df:sat_id,imaging_rate,readout_rate
    power_data_df:sat_id	, initial_power ,	power_cap
    power_transfer_data : 'sat_id', 'operation', 'sunlit_power_generate_rate',
                            'eclipse_power_consumption_rate', 'sunlit_power_consume_rate'

    eclipse_df_dict : {sat_id : eclipse_df : ['time_index'],'eclipse'}
    """
    schedule_df.sort_values(by=['sat_id','start_time'],inplace=True)

    schedule_df['till_now_max'] = schedule_df.groupby('sat_id')['end_time'].cummax()
    schedule_df['prev_max'] = schedule_df.groupby('sat_id')['till_now_max'].shift(1)

    schedule_df1 = schedule_df[schedule_df['start_time'] > schedule_df['prev_max'] + 1] 
    schedule_df1['start_time1'] = schedule_df1['prev_max'] + 1 #TODO1 +1 is okay ?
    schedule_df1['end_time1'] = schedule_df1['start_time'] - 1
    schedule_df1['operation'] = 'idle'

    schedule_df1 = schedule_df1[['sat_id','start_time1','end_time1','operation','base_time']]

    #memory_plot_df1 = memory_plot_df1.drop(['start_time', 'end_time','till_now_max','prev_max'], axis=1)
    schedule_df1.rename(columns={'start_time1':'start_time','end_time1':'end_time'},inplace=True)
    #imgGS_union_df1 ==> contains TW without img and without gs pass  table without eclipse divide
    final_schedule_df = pd.concat([schedule_df,schedule_df1])
    final_schedule_df['duration'] = final_schedule_df['end_time']-final_schedule_df['start_time']
    final_schedule_df.sort_values(by=['sat_id','start_time'],inplace=True)
    final_schedule_df = pd.merge(final_schedule_df,mem_transfer_SatLvlData_df,on='sat_id',how='left')

    camera_memory_df = memory_data_df[memory_data_df['memory_device']=='NCCM']
    ssd_memory_df = memory_data_df[memory_data_df['memory_device']=='SSD']

    camera_memory_df.rename(columns={"initial_memory":"camera_initial_memory",\
                                     "memory_cap":"camera_memory_cap"},inplace=True)
    ssd_memory_df.rename(columns={"initial_memory":"ssd_initial_memory",\
                                  "memory_cap":"ssd_memory_cap"},inplace=True)
    final_schedule_df = pd.merge(final_schedule_df,camera_memory_df[["sat_id","camera_initial_memory","camera_memory_cap"]],on='sat_id',how='left')
    
    ssd_memory_df_ = ssd_memory_df[["sat_id","ssd_initial_memory","ssd_memory_cap"]]
    final_schedule_df = pd.merge(final_schedule_df,ssd_memory_df_,on='sat_id',how='left')
    

    power_transfer_data['operation'] = power_transfer_data['operation'].map(map_dict())
    final_schedule_df = pd.merge(final_schedule_df,power_transfer_data,on=["sat_id","operation"],how='left')
    final_schedule_df = pd.merge(final_schedule_df,power_data_df,on=["sat_id"],how='left')

    final_schedule_df["delta_camera_memory"] = final_schedule_df[["operation","imaging_rate","readout_rate","duration"]].apply(lambda a: get_delta_memory(a['operation'],a['imaging_rate'],a['readout_rate'],a["duration"]),axis = 1 )
    final_schedule_df = get_df(final_schedule_df)

    final_schedule_df["delta_ssd_memory"] = final_schedule_df[["operation","imaging_rate","readout_rate","duration"]].apply(lambda a: a['duration'] * a["readout_rate"] if a["operation"]=='Readout' else 0 ,axis = 1 )
    final_schedule_df = get_df(final_schedule_df,"end_ssd_mem","delta_ssd_memory","start_ssd_mem","ssd_initial_memory")

    # ======================================================================================================================================================================================
    # thermal_df
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
    


    final_schedule_df = pd.merge(final_schedule_df,thermal_camera_detector_df,on="sat_id",how="left")
    final_schedule_df = pd.merge(final_schedule_df,thermal_XBT_df,on="sat_id",how="left")
    final_schedule_df = pd.merge(final_schedule_df,thermal_NCCM_df,on="sat_id",how="left")

    final_schedule_df.sort_values(by = 'start_time', inplace = True )

    final_schedule_df = get_thermal_delta_list(final_schedule_df ,need_thermal_operation = "Imaging",heat_eqn_column = "camera_detector_heat_eqn", cool_eqn_col = "camera_detector_cool_eqn",initial_temp_col = "initial_camera_detector_temp" , a_cool_parameter_col = "camera_detector_a_cool_parameter" , b_cool_parameter_col = "camera_detector_b_cool_parameter",delta_col= "delta_camera_detector")
    final_schedule_df= get_thermal_delta_list(final_schedule_df ,need_thermal_operation = "downlinking_from_Readout",heat_eqn_column = "XBT_heat_eqn", cool_eqn_col = "XBT_cool_eqn",initial_temp_col = "initial_xbt_temp" , a_cool_parameter_col = "XBT_a_cool_parameter" , b_cool_parameter_col = "XBT_b_cool_parameter",delta_col= "delta_xbt" )

    final_schedule_df= get_thermal_delta_list(final_schedule_df ,need_thermal_operation = "Readout",heat_eqn_column = "NCCM_heat_eqn", cool_eqn_col = "NCCM_cool_eqn",initial_temp_col = "initial_nccm_temp" , a_cool_parameter_col = "NCCM_a_cool_parameter" , b_cool_parameter_col = "NCCM_b_cool_parameter",delta_col= "delta_nccm" )
    final_schedule_df = correct_delta(final_schedule_df,delta_col="delta_camera_detector", initial_col = "initial_camera_detector_temp" , cap_col = "cap_camera_detector_temp" ,lower_cap_col= "initial_camera_detector_temp"  )
    final_schedule_df = correct_delta(final_schedule_df,delta_col="delta_xbt", initial_col = "initial_xbt_temp" , cap_col = "cap_xbt_temp" , lower_cap_col = "initial_xbt_temp"  )
    
    final_schedule_df = correct_delta(final_schedule_df,delta_col="delta_nccm", initial_col = "initial_nccm_temp" , cap_col = "cap_nccm_temp" , lower_cap_col = "initial_nccm_temp"  )

    # ======================================================================================================================================================================================
    ## power_df
    final_schedule_power_df = final_schedule_df.copy()
    final_schedule_power_df.sort_values(by="start_time",inplace = True)
    final_schedule_power_df['ec_st_end_list'] = final_schedule_power_df[["sat_id","start_time","end_time"]].apply(lambda a : get_EcStEnd_list(a["start_time"],a["end_time"],df = eclipse_df_dict[a["sat_id"]]),axis = 1)
   
    final_schedule_power_df = final_schedule_power_df.explode('ec_st_end_list')
    final_schedule_power_df['new_eclipse'] = final_schedule_power_df['ec_st_end_list'].apply(lambda a : a[0])
    final_schedule_power_df['new_start_time'] = final_schedule_power_df['ec_st_end_list'].apply(lambda a : a[1])
    final_schedule_power_df['new_end_time'] = final_schedule_power_df['ec_st_end_list'].apply(lambda a : a[2])
    # ==============
    check_l1 = len(final_schedule_power_df)
    #print(memory_based_copy_df[memory_based_copy_df['new_start_time'] =='NA'])
    final_schedule_power_df = final_schedule_power_df[final_schedule_power_df['new_start_time']!='NA']
    check_l2 = len(final_schedule_power_df)
    if check_l1!=check_l2:
        print("something is wrong in eclipse data or opprtunity start time or  end time")
    # ==============

    final_schedule_power_df.drop(['start_time','end_time','ec_st_end_list'],axis=1,inplace=True)
    final_schedule_power_df.rename(columns={'new_start_time':'start_time','new_end_time':'end_time','new_eclipse':'eclipse'},inplace=True)
    
    final_schedule_power_df["delta_power"] = final_schedule_power_df[["operation","duration","eclipse",\
                                                                        "sunlit_power_generate_rate",\
                                                                        "eclipse_power_consumption_rate",\
                                                                        "sunlit_power_consume_rate"]]\
                                                        .apply(lambda a: get_delta_power(a["operation"],\
                                                                                         a["duration"],\
                                                                                         a["eclipse"],\
                                                                                         a["sunlit_power_generate_rate"],\
                                                                                         a["eclipse_power_consumption_rate"],\
                                                                                         a["sunlit_power_consume_rate"]),axis=1)
    
    final_schedule_power_df = correct_delta(final_schedule_power_df)
       
    # ======================================================================================================================================================================================
    
    final_schedule_power_df = get_df(final_schedule_power_df,"end_power","delta_power","start_power","initial_power")
    final_schedule_df = get_df(final_schedule_df,"end_ssd_mem","delta_ssd_memory","start_ssd_mem","ssd_initial_memory")
    final_schedule_df = get_df(final_schedule_df,"end_camera_detector_temp","delta_camera_detector","start_camera_detector_temp","initial_camera_detector_temp")
    final_schedule_df = get_df(final_schedule_df,"end_xbt_temp","delta_xbt","start_xbt_temp","initial_xbt_temp")
    final_schedule_df = get_df(final_schedule_df,"end_nccm_temp","delta_nccm","start_nccm_temp","initial_nccm_temp")

    final_schedule_df['start_time'] = final_schedule_df[['start_time','base_time']].apply(lambda a: pd.to_datetime(a['base_time']) + pd.DateOffset(seconds=a['start_time']),axis=1)
    final_schedule_df['end_time'] = final_schedule_df[['end_time','base_time']].apply(lambda a: pd.to_datetime(a['base_time']) + pd.DateOffset(seconds=a['end_time']),axis=1)

    final_schedule_df["memory_min_cap"] = 0
    final_schedule_df_copy= final_schedule_df.copy()
 
    camera_memory_plots_fig_obj,final_schedule_camera_df = generate_profile_plots(final_schedule_df_copy)
    ssd_memory_plots_fig_obj,final_schedule_ssd_df  = generate_profile_plots(final_schedule_df_copy,"start_ssd_mem","end_ssd_mem","memory_min_cap","ssd_memory_cap","SSD_memory" )
    
    camera_detector_thermal_plots_fig_obj,final_schedule_camera_detector_df  = generate_profile_plots(final_schedule_df_copy,"start_camera_detector_temp","end_camera_detector_temp","initial_camera_detector_temp","cap_camera_detector_temp","Camera_detector_Profile" )
    XBT_thermal_plots_fig_obj,final_schedule_XBT_df  = generate_profile_plots(final_schedule_df_copy,"start_xbt_temp","end_xbt_temp","initial_xbt_temp","cap_xbt_temp","XBT_Profile" )
    NCCM_thermal_plots_fig_obj,final_schedule_NCCM_df  = generate_profile_plots(final_schedule_df_copy,"start_nccm_temp","end_nccm_temp","initial_nccm_temp","cap_nccm_temp","NCCM_Profile" )

    final_schedule_power_df["initial_power"] = final_schedule_power_df["initial_power"]/1000000
    final_schedule_power_df["power_lower_cap"] = final_schedule_power_df["power_lower_cap"]/1000000
    final_schedule_power_df["start_power"] = final_schedule_power_df["start_power"]/1000000
    final_schedule_power_df["end_power"] = final_schedule_power_df["end_power"]/1000000

    power_plots_fig_obj,final_schedule_power_df = generate_profile_plots(final_schedule_power_df,"start_power","end_power","initial_power","power_lower_cap","power" )
    camera_memory_plots_fig_obj.show()
    ssd_memory_plots_fig_obj.show()
    power_plots_fig_obj.show()

    camera_detector_thermal_plots_fig_obj.show()
    XBT_thermal_plots_fig_obj.show()
    NCCM_thermal_plots_fig_obj.show()

    return camera_memory_plots_fig_obj , ssd_memory_plots_fig_obj , power_plots_fig_obj , camera_detector_thermal_plots_fig_obj , XBT_thermal_plots_fig_obj , NCCM_thermal_plots_fig_obj



def plot_strip_status(fig,this_schedule_df,first_filter_col='sat_id',title_name="Strip to Strip Status",legend_Group = "All_Strips",second_col_filter="StripID",y_axis = "Strips"):
    """ 
    this_schedule_df : sat_id, flag , StripID , opportunity_start_time , opportunity_end_time
    this_schedule_df : sat_id, flag , GsID , opportunity_start_time , opportunity_end_time
    
    y-axis
    title_name
    legend_group

    title_name : Strips / GS

    # fig,this_plot_df,colors_dict_sat,satid,legend_Group = "All_Strips",title_name = "Strip to Strip Status",column_name ="StripID",y_axis = "Strips"
    """
    



    c = 0
    random_int = random.randint(0,30)
    
    sat_list = list(this_schedule_df[first_filter_col].unique())
    colour_list = color_names[random_int:random_int+len(sat_list)]
    colour_dict = {sat_list[i]:colour_list[i] for i in range(len(colour_list))}
    for satid in this_schedule_df[first_filter_col].unique():
        plt.figure(figsize=(60, 10))
        
        # color_id = (random.random(),random.random(),random.random())
        # color_id = 'rgb' + str(distinct_colors[c])
        color_id = colour_dict[satid]
        #colors_dict_sat = { s:c_list[i]  for i,s in enumerate(list(this_schedule_df['sat_id'].unique())) }

        this_plot_df = this_schedule_df[this_schedule_df[first_filter_col]==satid]
       
        fig = get_overlap_plots(fig,this_plot_df,color_id,satid,legend_Group,title_name,second_col_filter,y_axis)
        c += 1
    fig .show()
    return fig

