import pandas as pd

from APS_Python_core.utils import heating_func, cooling_func,get_time_list 

class GSpasspostprocess():
    def __init__(self,model_obj,model_input_data,config):
        self.model_obj = model_obj
        self.model_input_data = model_input_data
        self.config = config


    def to_check_thermal_constraint(self,o):
        
        o.sort_values(by = 'start_time', inplace = True)
        thermal_value ={}
        prev_time_cool = 0 
        this_state = 'steady' 
        # satellite gs
        satID = []
        time_index= []
        gsID = []
        thermal_value_list = []
        from_state = 0
        flag = []
        
        

        for s in o['SatID'].unique():
        
            thermal_value[s] = {}
            thermal_value[s][0] = self.model_input_data['initial_thermal_value'][s]
            this_o = o[ o['SatID'] == s ]
            #s = list(this_o['SatID'])[0]
            for t in range(1,self.config['scheduled_Hrs']*60*60):
                that_o = this_o[this_o['end_time'] > t]
                
                if len(that_o)!=0:
                    st = list(that_o['start_time'])[0]
                    et = list(that_o['end_time'])[0]   
                else:
                    et = -500
                    st = self.config['scheduled_Hrs'] + 1
                if t > st and t <= et :
                    gs_pass = list(that_o['gsID'])[0]
                    t_now = t


                    if from_state in ['heat','steady_NZ']:
                        t1 = st
                    if from_state == 'steady_HZ':
                        t1 = time_at_steady_HZ

                    if from_state not in ['cool_HZ','cool_NZ']:
                        
                        if t_now != t1:
                            thermal_heat = heating_func(t1,t_now) + thermal_value[s][t1]
                        else:
                            thermal_heat = thermal_value[s][t_now-1]
                    #print(self.model_input_data['thermal_capacity'][s])
                    if thermal_heat < self.model_input_data['thermal_capacity'][s] and this_state not in ['cool_HZ','cool_NZ']:
                        this_state = 'heat'
                        heat_upto_temp = thermal_heat
                        heat_upto_time =  t_now
                        thermal_value[s][t_now] = heat_upto_temp
                        #print(heat_upto_time,this_state,thermal_value[s][t-1],heat_upto_temp,from_state,t1,st,t,s,"A")

                        time_index.append(t)
                        satID.append(s)
                        gsID.append(gs_pass)
                        flag.append(this_state)
                        thermal_value_list.append(heat_upto_temp)

                    else:
                        if thermal_value[s][t-1] > 1.1 * self.model_input_data['initial_thermal_value'][s]:
                            this_state = 'cool_HZ'
                            if from_state in ['heat','cool_HZ','cool_NZ']:
                                thermal_cool =  cooling_func(heat_upto_time,t_now)
                                
                                #print(thermal_cool,heat_upto_time,this_state,t_now,t,heat_upto_time,from_state,s,"B")
                                thermal_value[s][t] = heat_upto_temp - abs(thermal_cool)

                                time_index.append(t)
                                satID.append(s)
                                gsID.append(gs_pass)
                                flag.append(this_state)
                                thermal_value_list.append(thermal_value[s][t])
                                
                        else:
                            this_state = 'steady_HZ'
                           
                            time_at_steady_HZ = t
                            thermal_value[s][t] = thermal_value[s][t-1]
                            #print(thermal_value[s][t],thermal_value[s][t-1],this_state,from_state,s,"C")

                            time_index.append(t)
                            satID.append(s)
                            gsID.append(gs_pass)
                            flag.append(this_state)
                            thermal_value_list.append(thermal_value[s][t])

                else:
                    if thermal_value[s][t-1] < 1.1 * self.model_input_data['initial_thermal_value'][s]: 
                        this_state = 'steady_NZ'
                        thermal_value[s][t] = thermal_value[s][t-1]
                        #print(thermal_value[s][t],thermal_value[s][t-1],this_state,from_state,s,"D")

                        time_index.append(t)
                        satID.append(s)
                        gsID.append('No_passing_zone')
                        flag.append(this_state)
                        thermal_value_list.append(thermal_value[s][t])
                    else:
                        this_state = 'cool_NZ'
                        if from_state in ['heat','cool_HZ','cool_NZ']:
                            thermal_cool =  cooling_func(heat_upto_time,t)
                            thermal_value[s][t] = heat_upto_temp - abs(thermal_cool)
                            #print(thermal_cool,heat_upto_time,this_state,t,heat_upto_time,from_state,s,"E")

                            time_index.append(t)
                            satID.append(s)
                            gsID.append('No_passing_zone')
                            flag.append(this_state)
                            thermal_value_list.append(thermal_value[s][t])

                        
                from_state = this_state
  
        return pd.DataFrame({'time_index':time_index,'SatID':satID,'gsID':gsID,'themal_value':thermal_value_list,'flag':flag})

    def get_gsPasses(self):
    
        output_dict = {}
        start_time = []
        end_time = []
        sat = []
        gs = []
        TW_index = []

        for csg in self.model_input_data['Concat_SatIdGSID']:
            for k in self.model_input_data['TW_index_list_SatIdGSID'][csg]:
                if self.model_obj.x['x_'+csg + '_'+ str(k)].value() == 1:
                    sat.append(self.model_input_data['get_satellite'][csg])
                    gs.append(self.model_input_data['get_grondstation'][csg])
                    start_time.append(self.model_obj.t_d['st_'+csg + '_'+ str(k)].value())
                    end_time.append(self.model_obj.t_d['st_'+csg + '_'+ str(k)].value() + self.model_obj.p['p_'+csg + '_'+ str(k)].value())
                    TW_index.append(k)
        
        output_dict['SatID'] = sat
        output_dict['gsID'] = gs
        output_dict['start_time'] = start_time
        output_dict['end_time'] = end_time
        output_dict['TW_index'] = TW_index
        o  = pd.DataFrame(output_dict)
        o['concat_sat_gs_k'] = o['SatID'].astype(str)  + '_' + o['gsID'].astype(str)  + '_' +o['TW_index'].astype(str) 
        o['AOSoffset'] = o['concat_sat_gs_k'].map(self.model_input_data['get_AOS'])
        o['LOSoffset'] = o['concat_sat_gs_k'].map(self.model_input_data['get_LOS'])

        if self.config['constraints']['Thermal_constraints_GS_pass']:
            thermal_value = self.to_check_thermal_constraint(o)
            thermal_value1 = thermal_value[thermal_value['flag'] == 'heat']

            thermal_value2 = thermal_value1.groupby(['SatID','gsID']).agg(list_time_index= ('time_index',list)).reset_index()
            #print(thermal_value2)
            
            thermal_value2['list'] = thermal_value2['list_time_index'].apply(  get_time_list )

            thermal_value2 = thermal_value2.explode('list')
            thermal_value2['start_time'] = thermal_value2['list'].apply(lambda a: a[0])
            thermal_value2['end_time'] = thermal_value2['list'].apply(lambda a: a[-1])
        else:
            thermal_value2 = o
            thermal_value =''
            

        return thermal_value2,thermal_value



