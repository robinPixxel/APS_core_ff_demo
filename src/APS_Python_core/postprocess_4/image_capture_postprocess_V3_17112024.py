import pandas as pd

class ImagecapturePostProcess():
    def __init__(self,model_obj,model_input_data) :
        self.model_obj = model_obj
        self.model_input_data = model_input_data
    
        self.start_time_list = []
        self.end_time_list = []
        self.SatId_list = []
        self.gsID_list = []
        self.encoded_strip_id_list = []
        self.flag_list =[]
        self.camera_memory_value_list = []
        self.delta_camera_memory_value_list = []

        self.readout_memory_value_list = []
        self.readout_delta_memory_value_list = []

        self.GP = []
        self.LP = []

        self.GP_mean = []
        self.LP_mean = []




    def get_schedule(self):

        for no_img_gs_pass_case in self.model_input_data['Memory_NoimageGs_TW_list']:
            st_W = no_img_gs_pass_case[0]
            et_W = no_img_gs_pass_case[1]
            s = no_img_gs_pass_case[2]
            n = no_img_gs_pass_case[3]

            if n not in self.model_input_data['dedicatedReadoutTWIndex__sat'][s]:
                continue

            if self.model_obj.XR['readout_happens'+s+'_'+str(n)].value() ==1 :
                #print("READOUT")
                st_readout = self.model_obj.ZS['readout_start_time_'+s+'_'+str(n)].value()
                et_readout = self.model_obj.ZE['readout_end_time_'+s+'_'+str(n)].value()
                camera_memory_value = self.model_obj.camera_memory_value['camera_memory_value'+s+'_'+str(n)].value()
                delta_camera_memory_value = self.model_obj.delta_camera_memory_value['delta_camera_memory_value'+s+'_'+str(n)].value()
                readout_memory_value = self.model_obj.readout_memory_value['readout_memory_value'+s+'_'+str(n)].value()
                readout_delta_memory_value = self.model_obj.readout_delta_memory_value['readout_delta_memory_value'+s+'_'+str(n)].value()

                self.start_time_list.append(st_readout)
                self.end_time_list.append(et_readout)
                self.SatId_list.append(s)
                self.encoded_strip_id_list.append("no_i_no_g")
                self.gsID_list.append("no_i_no_g")
                self.flag_list.append("Readout")
                self.camera_memory_value_list.append(camera_memory_value)
                self.delta_camera_memory_value_list.append(delta_camera_memory_value)
                self.readout_memory_value_list.append(readout_memory_value)
                self.readout_delta_memory_value_list.append(readout_delta_memory_value)

                self.GP.append('no_i_no_g')
                self.LP.append('no_i_no_g')
                self.GP_mean.append('no_i_no_g')
                self.LP_mean.append('no_i_no_g')
        #==================#==================#==================#==================#==================#==================#==================#==================
        for only_img_case in self.model_input_data['Memory_onlyImgTW_list']:
            st_W = only_img_case[0]
            et_W = only_img_case[1]
            s = only_img_case[2]
            n = only_img_case[3]
            j = only_img_case[4]
            k = only_img_case[5]


            if self.model_obj.x_o['oppr_'+s+'_'+j+'_'+str(k)].value() ==1 :
                st_img = st_W
                et_img = et_W
                camera_memory_value = self.model_obj.camera_memory_value['camera_memory_value'+s+'_'+str(n)].value()
                delta_camera_memory_value = self.model_obj.delta_camera_memory_value['delta_camera_memory_value'+s+'_'+str(n)].value()
                readout_memory_value = 'NA'#self.model_obj.readout_memory_value['readout_memory_value'+s+'_'+str(n)].value()
                readout_delta_memory_value = 'NA'#self.model_obj.readout_delta_memory_value['readout_delta_memory_value'+s+'_'+str(n)].value()


                self.start_time_list.append(st_img)
                self.end_time_list.append(et_img)
                self.SatId_list.append(s)
                self.encoded_strip_id_list.append(j)
                self.gsID_list.append("")
                self.flag_list.append("Imaging")
                self.camera_memory_value_list.append(camera_memory_value)
                self.delta_camera_memory_value_list.append(delta_camera_memory_value)
                self.readout_memory_value_list.append(readout_memory_value)
                self.readout_delta_memory_value_list.append(readout_delta_memory_value)

                self.GP.append(self.model_input_data['GlobalPriority__csjk'][s+'_'+str(j)+'_'+str(k)])
                self.LP.append(self.model_input_data['Local_Priority__csjk'][s+'_'+str(j)+'_'+str(k)])
                self.GP_mean.append(self.model_input_data['GlobalPriority__csj'][str(j)])
                self.LP_mean.append(self.model_input_data['Local_Priority__csj'][str(j)])

        #==================#==================#==================#==================#==================#==================#==================#==================
        for only_gs_case in self.model_input_data['Memory_onlyGsTW_list']:
            st_W = only_gs_case[0]
            et_W = only_gs_case[1]
            s = only_gs_case[2]
            n = only_gs_case[3]
            g = only_gs_case[4]

            if self.model_obj.GP['ground_PAss_happens_'+s+'_'+g+'_'+str(n)].value() == 1 :
                st_gs = st_W
                et_gs = st_W + self.model_obj.Pgs['process_time_GP_'+s+'_'+str(g)+'_'+str(n)].value()
                camera_memory_value = self.model_obj.camera_memory_value['camera_memory_value'+s+'_'+str(n)].value()
                delta_camera_memory_value = self.model_obj.delta_camera_memory_value['delta_camera_memory_value'+s+'_'+str(n)].value()
                readout_memory_value = 'NA'#self.model_obj.readout_memory_value['readout_memory_value'+s+'_'+str(n)].value()
                readout_delta_memory_value = 'NA'#self.model_obj.readout_delta_memory_value['readout_delta_memory_value'+s+'_'+str(n)].value()


                self.start_time_list.append(st_gs) # for now assume GP start at the time of starting GP Window
                self.end_time_list.append(et_gs)
                self.SatId_list.append(s)
                self.encoded_strip_id_list.append("")
                self.gsID_list.append(g)
                if self.model_obj.GR['downlink_readout_memory_'+s+'_'+str(g)+'_'+str(n)].value()==1:
                    self.flag_list.append("downlinking_from_Readout")
                elif self.model_obj.GC['downlink_camera_memory_'+s+'_'+str(g)+'_'+str(n)].value()==1:
                    self.flag_list.append("downlinking_from_camera")

                self.camera_memory_value_list.append(camera_memory_value)
                self.delta_camera_memory_value_list.append(delta_camera_memory_value)
                self.readout_memory_value_list.append(readout_memory_value)
                self.readout_delta_memory_value_list.append(readout_delta_memory_value)

                self.GP.append('no_i_no_g')
                self.LP.append('no_i_no_g')

                self.GP_mean.append('no_i_no_g')
                self.LP_mean.append('no_i_no_g')

        image_capture_output = pd.DataFrame(  {"sat_id":self.SatId_list,\
                        "start_time":self.start_time_list,\
                        "end_time":self.end_time_list,\
                        "encoded_strip_id":self.encoded_strip_id_list,\
                        "gs_id":self.gsID_list,\
                        "operation":self.flag_list,\
                        "camera_memory_value_end_of_tw":self.camera_memory_value_list,\
                        "delta_camera_memory_value_in_this_tw":self.delta_camera_memory_value_list,\
                        "SSD_memory_value_endof_tw":self.readout_memory_value_list,\
                        "delta_SSD_memory_value_in_this_tw":self.readout_delta_memory_value_list,\
                        "global_priority":self.GP,\
                        "local_priority":self.LP,
                        "mean_global_priority":self.GP_mean,\
                        "mean_local_priority":self.LP_mean,\
                        
                        }
                           )
        
        image_capture_output['strip_id'] = image_capture_output['encoded_strip_id'].map(self.model_input_data['stripid__encodedstripID'])
        image_capture_output['aoi_id'] = image_capture_output['encoded_strip_id'].map(self.model_input_data['AOIid__encodedstripID'])
       
        return image_capture_output

#========================================================================================================================================================================================================================================================================================
