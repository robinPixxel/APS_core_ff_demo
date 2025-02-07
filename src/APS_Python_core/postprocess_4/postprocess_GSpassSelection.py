import pandas as pd
class GSpasspostprocess():
    def __init__(self,model_obj,model_input_data,config):
        self.model_obj = model_obj
        self.model_input_data = model_input_data
        self.config = config

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
        
        output_dict['sat_id'] = sat
        output_dict['gs_id'] = gs
        output_dict['start_time'] = start_time
        output_dict['end_time'] = end_time
        output_dict['tw_index'] = TW_index
        o  = pd.DataFrame(output_dict)
        o['concat_sat_gs_k'] = o['sat_id'].astype(str)  + '_' + o['gs_id'].astype(str)  + '_' +o['tw_index'].astype(str) 
        o['aos_offset'] = o['concat_sat_gs_k'].map(self.model_input_data['get_AOS'])
        o['los_offset'] = o['concat_sat_gs_k'].map(self.model_input_data['get_LOS'])
        thermal_value2 = o
       

        return thermal_value2



