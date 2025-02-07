
import pandas as pd
class ImageDownlinkPostProcess():
    def __init__(self,model_obj,model_input_data) :
        self.model_obj = model_obj
        self.model_input_data = model_input_data

    def get_schedule(self):

        image_id_list = []
        SatId_list = []
        gsID_list =[]
        start_time_list = []
        end_time_list= []
        number_of_tile_strips_capture_list = []
        Total_number_of_tiles_list = []
        bandset_list = []

        for i in self.model_input_data['all_satellite_list']:
            for j in self.model_input_data['capture_list__sat'][i]:
                for g in self.model_input_data['ground_station_list__sat'][i]:
                    TW_list = self.model_input_data['TW_list__concatSatGs'][i+'_'+g]
                    TW_index_list = self.model_input_data['TW_index_list__concatSatGs'][i+'_'+g]
                    
                    for ind,TW in enumerate(TW_list) : 
                        st = TW[0]
                        et = TW[1]
                        k = TW_index_list[ind]
                        SatId_list.append(i)
                        image_id_list.append(j)
                        gsID_list.append(g)
                        start_time_list.append(st)
                        end_time_list.append(et)
                        number_of_tile_strips_capture_list.append(self.model_obj.N['number'+'_'+str(i)+'_'+str(j)+'_'+str(g)+'_'+str(k)].value())
                        Total_number_of_tiles_list.append(self.model_input_data['NoOfTileStrip__imgID'][j])
                        bandset_list.append(self.model_input_data['bands__imgID'][j])


        downlink_result = pd.DataFrame({'sat_id':SatId_list,\
                             'gs_id':gsID_list,\
                             'start_time':start_time_list,\
                             'image_id':image_id_list,\
                             'end_time': end_time_list,\
                             'tilestrips_no_download':number_of_tile_strips_capture_list,\
                             "total_no_tilsstrips":Total_number_of_tiles_list,\
                             "bands":bandset_list})
        
        downlink_result = downlink_result[downlink_result['tilestrips_no_download']!=0]
        
        return downlink_result


