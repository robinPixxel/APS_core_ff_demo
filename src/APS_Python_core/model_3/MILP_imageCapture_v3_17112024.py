
from pulp import *
import pandas as pd


class ImageCapturePlan():
    def __init__(self, data,config):
        
        self.setup_time = 120
        self.M = 1000000
        self.config = config

        self.data = data
        if self.config['constraints']['thermal_constraint_readout']:
            self.max_heat_time_readout_dict = self.data['max_readout_heat_dict']
        if self.config['constraints']['thermal_constraint_imaging']:
            self.max_heat_time_camera_dict = self.data['max_camera_heat_dict']

        self.create_model()
        self.create_DV()
        self.create_objective()
        self.create_constraints()
        self.solve_model()

    def create_model(self):
        self.prob = pulp.LpProblem("image_capture_plan", LpMaximize)
        
    def create_DV(self) :
        # whether opportunity is selected or not
        self.x_o = { 'oppr_'+sjk : LpVariable('oppr_'+sjk ,cat = 'Binary') for sjk in self.data['unique_img_opportunities_list'] }
        
        # start time of opportunity
        self.td_o = { 'st_'+sjk : LpVariable('st_'+sjk, cat ='Continuous' , lowBound = 0,upBound = self.data['TW__csjk'] [sjk][0] ) for sjk in self.data['unique_img_opportunities_list'] }

        # if j1 is strictly happening after j2 then thetha_o [j1,j2,s,k1,k2] =0 else 1.
        self.theth_o = { 'overlap_dv_' + s1j1k1+'_'+s1j2k2  : LpVariable('overlap_dv_' + s1j1k1+'_'+s1j2k2,cat = 'Binary' )\
                                                                       for s1 in self.data['imgery_sat_id_list']
                                                                       for s1j1k1 in self.data['csjkSet__s'][s1]\
                                                                       for s1j2k2 in self.data['cs1j2k2Domainlist__cs1j1k1'] [s1j1k1]}
        
        self.camera_memory_value = { 'camera_memory_value'+s+'_'+str(n) : LpVariable('camera_memory_value'+s+'_'+str(n),cat = 'Continuous',lowBound = 0, upBound = self.data['camera_memory_capacity__s'][s]) \
                             for s in self.data['imgery_sat_id_list']+self.data['only_gs_sat_id_list'] for n in self.data['MemoryglobalTWindexSortedList__s'][s]}
        
        self.delta_camera_memory_value = { 'delta_camera_memory_value'+s+'_'+str(n) : LpVariable('delta_camera_memory_value'+s+'_'+str(n),cat = 'Continuous') \
                             for s in self.data['imgery_sat_id_list']+self.data['only_gs_sat_id_list'] for n in self.data['MemoryglobalTWindexSortedList__s'][s]}
        
        self.readout_memory_value = { 'readout_memory_value'+s+'_'+str(n) : LpVariable('readout_memory_value'+s+'_'+str(n),cat = 'Continuous',lowBound = 0, upBound =self.data['readout_memory_capacity__s'][s]) \
                             for s in self.data['imgery_sat_id_list']+self.data['only_gs_sat_id_list'] if s in self.data['dedicatedReadoutTWIndex__sat'].keys() for n in self.data['dedicatedReadoutTWIndex__sat'][s]}
        
        self.readout_delta_memory_value = { 'readout_delta_memory_value'+s+'_'+str(n) : LpVariable('readout_delta_memory_value'+s+'_'+str(n),cat = 'Continuous') \
                             for s in self.data['imgery_sat_id_list']+self.data['only_gs_sat_id_list'] if s in self.data['dedicatedReadoutTWIndex__sat'].keys() for n in self.data['dedicatedReadoutTWIndex__sat'][s]}
        
        self.ZS ={'readout_start_time_'+s+'_'+str(n) : LpVariable('readout_start_time_'+s+'_'+str(n),cat = 'Continuous',lowBound=0,upBound=self.data['TW__csn'][str(s)+'_'+str(n)][1]) \
                             for s in self.data['imgery_sat_id_list']+self.data['only_gs_sat_id_list'] if s in self.data['dedicatedReadoutTWIndex__sat'].keys() for n in self.data['dedicatedReadoutTWIndex__sat'][s]}
        
        self.ZE ={'readout_end_time_'+s+'_'+str(n) : LpVariable('readout_end_time_'+s+'_'+str(n),cat = 'Continuous',lowBound= 0,upBound=self.data['TW__csn'][str(s)+'_'+str(n)][1]) \
                             for s in self.data['imgery_sat_id_list']+self.data['only_gs_sat_id_list'] if s in self.data['dedicatedReadoutTWIndex__sat'].keys() for n in self.data['dedicatedReadoutTWIndex__sat'][s]}
        
        self.XR ={'readout_happens'+s+'_'+str(n) : LpVariable('readout_happens'+s+'_'+str(n) ,cat = 'Binary') \
                             for s in self.data['imgery_sat_id_list']+self.data['only_gs_sat_id_list'] if s in self.data['dedicatedReadoutTWIndex__sat'].keys() for n in self.data['dedicatedReadoutTWIndex__sat'][s]}
        
        self.power_value = { 'power_value'+s+'_'+str(n) : LpVariable('power_value'+s+'_'+str(n),cat = 'Continuous',lowBound=self.data['power_capacity__s'][s]*0.1,upBound = self.data['power_capacity__s'][s]) \
                             for s in self.data['imgery_sat_id_list']+self.data['only_gs_sat_id_list'] for n in self.data['PowerglobalTWindexSortedList__s'][s] }
        
        self.delta_power_value = { 'delta_power_value'+s+'_'+str(n) : LpVariable('delta_power_value'+s+'_'+str(n),cat = 'Continuous',upBound = self.data['power_capacity__s'][s]) \
                             for s in self.data['imgery_sat_id_list']+self.data['only_gs_sat_id_list'] for n in self.data['PowerglobalTWindexSortedList__s'][s] }

        self.PtPG ={'Power_generation_'+s+'_'+str(n) : LpVariable('Power_generation_'+s+'_'+str(n),cat = 'Continuous',lowBound = 0 ) \
                             for s in self.data['imgery_sat_id_list']+self.data['only_gs_sat_id_list'] for n in self.data['PowerglobalTWindexSortedList__s'][s] }

        # self.GP = {'ground_PAss_happens_'+s+'_'+str(n):LpVariable('ground_PAss_happens_'+s+'_'+str(n), cat= 'Binary' )\
        #            for s in self.data['imgery_sat_id_list']+self.data['only_gs_sat_id_list'] for n in self.data['PowerglobalTWindexSortedList__s'][s] } # MemoryglobalTWindexSortedList__s
        self.GP = {'ground_PAss_happens_'+only_gs_case[2]+'_'+str(only_gs_case[3]):LpVariable('ground_PAss_happens_'+only_gs_case[2]+'_'+str(only_gs_case[3]), cat= 'Binary' ) \
                   for only_gs_case in self.data['Memory_onlyGsTW_list'] }
        
        self.Pgs = {'process_time_GP_'+only_gs_case[2]+'_'+str(only_gs_case[4])+'_'+str(only_gs_case[3]) \
                    :LpVariable('process_time_GP_'+only_gs_case[2]+'_'+str(only_gs_case[4])+'_'+str(only_gs_case[3]),cat= 'Continuous',lowBound = 0,upBound = only_gs_case[1] - only_gs_case[0] ) \
                    for only_gs_case in self.data['Memory_onlyGsTW_list'] }
        
        self.GC = {'downlink_camera_memory_'+only_gs_case[2]+'_'+str(only_gs_case[4])+'_'+str(only_gs_case[3]) \
                    :LpVariable('downlink_camera_memory_'+only_gs_case[2]+'_'+str(only_gs_case[4])+'_'+str(only_gs_case[3]),cat= 'Binary' ) \
                    for only_gs_case in self.data['Memory_onlyGsTW_list'] }
        
        self.GR = {'downlink_readout_memory_'+only_gs_case[2]+'_'+str(only_gs_case[4])+'_'+str(only_gs_case[3]) \
                    :LpVariable('downlink_readout_memory_'+only_gs_case[2]+'_'+str(only_gs_case[4])+'_'+str(only_gs_case[3]),cat= 'Binary' ) \
                    for only_gs_case in self.data['Memory_onlyGsTW_list'] }
        

        if self.config['constraints']['thermal_constraint_readout']:
            self.betaR = {'bucket_HZ_'+s+'_'+str(n)+'_'+str(bi) :LpVariable('bucket_HZ_'+s+'_'+str(n)+str(bi),cat = 'Binary') \
                             for s in self.data['imgery_sat_id_list']+self.data['only_gs_sat_id_list'] \
                                for n in self.data['MemoryglobalTWindexSortedList__s'][s]\
                                    for bi in self.data['heatTimeBucket_SCT_dict__s'][s].keys()}
        if self.config['constraints']['thermal_constraint_imaging']:
            self.betaC = {'bucketC_HZ_'+s+'_'+str(n)+'_'+str(bi) :LpVariable('bucketC_HZ_'+s+'_'+str(n)+str(bi),cat = 'Binary') \
                                for s in self.data['imgery_sat_id_list']+self.data['only_gs_sat_id_list'] \
                                    for n in self.data['MemoryglobalTWindexSortedList__s'][s]\
                                        for bi in self.data['heatCameraTimeBucket_SCT_dict__s'][s].keys()}
            
        self.Qtgs = {'Q_Power_generation_'+only_gs_case[2]+'_'+str(only_gs_case[6]) : LpVariable('Q_Power_generation_'+only_gs_case[2]+'_'+str(only_gs_case[6]) , \
                                                                                                 cat= 'Continuous',\
                                                                                                lowBound = 0,\
                                                                                                upBound =only_gs_case[1]-only_gs_case[0]) \
                                                                                                for only_gs_case in self.data['Power_GS_TW_list']}   

               
        

    def create_constraints(self) :
        
        # starting time should be equal than start of time window
        for sjk in self.data['unique_img_opportunities_list'] : 
            self.prob += self.td_o['st_'+sjk ] == self.data['TW__csjk'] [sjk][0] * self.x_o['oppr_'+sjk ]

        # overlaping constraint
        if self.config['constraints']['overlapping_constraints']:
            for s in self.data['imgery_sat_id_list']:
                for s1j1k1 in self.data['csjkSet__s'][s]:
                    for s1j2k2 in self.data['cs1j2k2Domainlist__cs1j1k1'] [s1j1k1]:
                        self.prob += self.td_o['st_'+s1j1k1]  - self.td_o['st_'+s1j2k2]  >= self.x_o['oppr_'+s1j2k2 ]*(self.data['TW__csjk'][s1j2k2][1]\
                                                                                                                    - self.data['TW__csjk'][s1j2k2][0]  \
                                                                                                                    + self.setup_time)\
                                                                                                                    - self.theth_o['overlap_dv_' + s1j1k1+'_'+s1j2k2 ]* self.M,"overlap_cons_part1_"+s1j1k1+'_'+s1j2k2
                        
                        self.prob += self.td_o['st_'+s1j2k2]  - self.td_o['st_'+s1j1k1]  >= self.x_o['oppr_'+s1j1k1 ]*(self.data['TW__csjk'][s1j1k1][1]\
                                                                                                                    - self.data['TW__csjk'][s1j1k1][0]  \
                                                                                                                    + self.setup_time)\
                                                                                                                    - self.theth_o['overlap_dv_' + s1j2k2+'_'+s1j1k1 ]*self.M,"overlap_cons_part2_"+s1j1k1+'_'+s1j2k2

                        self.prob += self.theth_o['overlap_dv_' + s1j1k1+'_'+s1j2k2 ] + self.theth_o['overlap_dv_' + s1j2k2+'_'+s1j1k1 ] >= self.x_o['oppr_'+s1j1k1 ] + self.x_o['oppr_'+s1j2k2 ] - 1,"overlap_cons_part3_"+'_'+s1j1k1+'_'+s1j2k2

                        self.prob += self.theth_o['overlap_dv_' + s1j1k1+'_'+s1j2k2 ] + self.theth_o['overlap_dv_' + s1j2k2+'_'+s1j1k1 ] <= 1,"overlap_cons_part4_"+'_'+s1j1k1+'_'+s1j2k2


        
        # one job selection
        for j in self.data['encoded_stripId_list'] :
            self.prob += lpSum([self.x_o['oppr_'+sjk ] for sjk in self.data['csjkList__j'][j]]) <= 1 ,"one_job_selection_for"+j

        #due date based assured 
        if self.config['constraints']['active_assured_strip_id_constraint']:
            for j in self.data['active_assured_strip_id_list']:
                self.prob += lpSum([self.x_o['oppr_'+sjk ] for sjk in self.data['csjkList__j'][j]]) == 1 ,"strict_one_job_selection_for_due_date_assurance"+j

        # input assured task 
        # for j in self.data['assured_tasking_based_on_input_list']:
        #     self.prob += lpSum([self.x_o['oppr_'+sjk ] for sjk in self.data['csjkList__j'][j]]) == 1 ,"strict_one_job_selection_for_input_assurance"+j

        # both img gs conflict constraint
        
        # for both_img_gs_case in self.data['both_img_gs_list'] :
      
        #     st_W = both_img_gs_case[0]
        #     et_W = both_img_gs_case[1]
        #     s = both_img_gs_case[2]
        #     n = both_img_gs_case[3]
        #     g = both_img_gs_case[4]
        #     j = both_img_gs_case[5]
        #     k = both_img_gs_case[6]

        #     # self.prob += self.x_o ['oppr_'+s+'_'+j+'_'+str(k)] <= self.GP['ground_PAss_happens_'+s+'_'+str(n)]
        #     self.prob += self.x_o ['oppr_'+s+'_'+j+'_'+str(k)] + self.GP['ground_PAss_happens_'+s+'_'+str(n)] <= 1

            
        #Memory constarint

        # when there is downlinking --> no imaging no readout and vice versa
        # Camera memory and Readout memory
        #1 : NO Img No gs 

        # ptw_list = self.data['prev_tWList__s_TWI_dict__s'][s][s+'_'+str(n)]
        # prev_window = ptw_list[1]
        # self.prob += self.camera_memory_value['camera_memory_value'+s+'_'+str(n)] == self.camera_memory_value['camera_memory_value'+s+'_'+str(prev_window)] + \
        #     self.delta_camera_memory_value['delta_camera_memory_value'+s+'_'+str(n)]
        
        #self.prob += self.camera_memory_value['camera_memory_value'+s+'_'+str(1)] == self.data['']
        if self.config['constraints']['memory_constrant']:
            for no_img_gs_pass_case in self.data['Memory_NoimageGs_TW_list']:
                st_W = no_img_gs_pass_case[0]
                et_W = no_img_gs_pass_case[1]
                s = no_img_gs_pass_case[2]
                n = no_img_gs_pass_case[3]
            # for TW belonging to readout dedicated Window
                if str(s)+'_'+str(n) in self.data['dedicatedReadoutTWlist__concat_sat_memoryTWindex'].keys() and (True):
                    DW_info_list = self.data['dedicatedReadoutTWlist__concat_sat_memoryTWindex'][str(s)+'_'+str(n)]
                    #print(s,n,"DTW",DW_info_list)
                    s_DW = DW_info_list[0]
                    st_DW = DW_info_list[1]
                    et_DW = DW_info_list[2]

                    # change in camera memory Due to Readout
                    self.prob += self.delta_camera_memory_value['delta_camera_memory_value'+s+'_'+str(n)] >= \
                        0 - self.data['Readout_rate'] *(self.ZE['readout_end_time_'+s+'_'+str(n)] -self.ZS['readout_start_time_'+s+'_'+str(n)]) \
                            -self.M *(1-self.XR['readout_happens'+s+'_'+str(n)])
                    
                    self.prob += self.delta_camera_memory_value['delta_camera_memory_value'+s+'_'+str(n)] <= \
                        0 - self.data['Readout_rate'] *(self.ZE['readout_end_time_'+s+'_'+str(n)] -self.ZS['readout_start_time_'+s+'_'+str(n)]) \
                            + self.M *(1-self.XR['readout_happens'+s+'_'+str(n)])
                    
                    self.prob += self.delta_camera_memory_value['delta_camera_memory_value'+s+'_'+str(n)] <= self.M * self.XR['readout_happens'+s+'_'+str(n)]
                    self.prob += self.delta_camera_memory_value['delta_camera_memory_value'+s+'_'+str(n)] >= -1* self.M * self.XR['readout_happens'+s+'_'+str(n)]

                    # change in readout  memory Due to Readout
                    self.prob += self.readout_delta_memory_value['readout_delta_memory_value'+s+'_'+str(n)] >= \
                            self.data['Readout_rate'] *(self.ZE['readout_end_time_'+s+'_'+str(n)] -self.ZS['readout_start_time_'+s+'_'+str(n)]) \
                            -self.M *(1-self.XR['readout_happens'+s+'_'+str(n)])
                    
                    self.prob += self.readout_delta_memory_value['readout_delta_memory_value'+s+'_'+str(n)] <= \
                            self.data['Readout_rate'] *(self.ZE['readout_end_time_'+s+'_'+str(n)] -self.ZS['readout_start_time_'+s+'_'+str(n)]) \
                            +self.M *(1-self.XR['readout_happens'+s+'_'+str(n)])
                    
                    self.prob += self.readout_delta_memory_value['readout_delta_memory_value'+s+'_'+str(n)] <= self.M * self.XR['readout_happens'+s+'_'+str(n)]
                    self.prob += self.readout_delta_memory_value['readout_delta_memory_value'+s+'_'+str(n)] >= -1* self.M * self.XR['readout_happens'+s+'_'+str(n)]

                    # start and end time bounds

                    self.prob +=  st_DW * self.XR['readout_happens'+s+'_'+str(n)] <= self.ZS['readout_start_time_'+s+'_'+str(n)]
                    self.prob +=  et_DW * self.XR['readout_happens'+s+'_'+str(n)] >= self.ZS['readout_start_time_'+s+'_'+str(n)]

                    self.prob +=  st_DW * self.XR['readout_happens'+s+'_'+str(n)] <= self.ZE['readout_end_time_'+s+'_'+str(n)]
                    self.prob +=  et_DW * self.XR['readout_happens'+s+'_'+str(n)] >= self.ZE['readout_end_time_'+s+'_'+str(n)]

                    self.prob += self.ZE['readout_end_time_'+s+'_'+str(n)] - self.ZS['readout_start_time_'+s+'_'+str(n)] >= self.XR['readout_happens'+s+'_'+str(n)] #* self.config['min_readou_time_seconds'] # min 50 secode readout min_readou_time_seconds
                    #self.prob += self.ZE['readout_end_time_'+s+'_'+str(n)] - self.ZS['readout_start_time_'+s+'_'+str(n)] <= self.M * self.XR['readout_happens'+s+'_'+str(n)]

                    ptw_list = self.data['prev_dedicatedReadoutIndex__s_TWI_dict__s'][s][s+'_'+str(n)]

                    if self.config['constraints']['thermal_constraint_readout']:
                        self.prob += self.ZE['readout_end_time_'+s+'_'+str(n)] - self.ZS['readout_start_time_'+s+'_'+str(n)] <= self.max_heat_time_readout_dict[s]
                        for p in ptw_list[1:]:
                            #self.prob += self.ZS['readout_start_time_'+s+'_'+str(n)] >= self.ZE['readout_end_time_'+s+'_'+str(p)] + self.max_heat_time_readout_dict[s] - self.M * (1- self.XR['readout_happens'+s+'_'+str(p)])#TODO2
                            self.prob += self.ZS['readout_start_time_'+s+'_'+str(n)] >= self.ZE['readout_end_time_'+s+'_'+str(p)] + \
                                                                                            lpSum([self.betaR['bucket_HZ_'+s+'_'+str(p)+'_'+str(bi) ] * v[2] \
                                                                                            for bi,v in self.data['heatTimeBucket_SCT_dict__s'][s].items()])\
                                                                                    - self.M * (1- self.XR['readout_happens'+s+'_'+str(p)]) \
                                                                                    - self.M * (1- self.XR['readout_happens'+s+'_'+str(n)])#TODO2
                            self.prob += lpSum([self.betaR['bucket_HZ_'+s+'_'+str(p)+'_'+str(bi)] 
                                                for bi in self.data['heatTimeBucket_SCT_dict__s'][s].keys()]) ==1 
                            
                            for bi,v in self.data['heatTimeBucket_SCT_dict__s'][s].items():
                                self.prob += self.ZE['readout_end_time_'+s+'_'+str(p)] -  self.ZS['readout_start_time_'+s+'_'+str(p)]\
                                            >= v[0] - self.M * (1- self.betaR['bucket_HZ_'+s+'_'+str(n)+'_'+str(bi)] )
                                self.prob += self.ZE['readout_end_time_'+s+'_'+str(p)] -  self.ZS['readout_start_time_'+s+'_'+str(p)]\
                                            <= v[1] + self.M * (1- self.betaR['bucket_HZ_'+s+'_'+str(n)+'_'+str(bi)] )
            

                    if len(ptw_list) != 1 :
                        prev_window = ptw_list[1]
                        self.prob += self.readout_memory_value['readout_memory_value'+s+'_'+str(n)] == self.readout_memory_value['readout_memory_value'+s+'_'+str(prev_window)] + \
                                    self.readout_delta_memory_value['readout_delta_memory_value'+s+'_'+str(n)]
                    else:
                        self.prob += self.readout_memory_value['readout_memory_value'+s+'_'+str(n)] == self.data['initial_readout_camera_memory_value__s'][s] + \
                            self.readout_delta_memory_value['readout_delta_memory_value'+s+'_'+str(n)]  
                    

                else:
                    #self.prob += self.readout_delta_memory_value['readout_delta_memory_value'+s+'_'+str(n)] == 0
                    self.prob += self.delta_camera_memory_value['delta_camera_memory_value'+s+'_'+str(n)] == 0
                    #self.prob += self.XR['readout_happens'+s+'_'+str(n)] == 0
                    #self.prob += self.XR['readout_happens'+s+'_'+str(n)] == 0

                    # start and end time bounds

                    # self.prob +=  st_W * self.XR['readout_happens'+s+'_'+str(n)] <= self.ZS['readout_start_time_'+s+'_'+str(n)]
                    # self.prob +=  et_W * self.XR['readout_happens'+s+'_'+str(n)] >= self.ZS['readout_start_time_'+s+'_'+str(n)]

                    # self.prob +=  st_W * self.XR['readout_happens'+s+'_'+str(n)] <= self.ZE['readout_end_time_'+s+'_'+str(n)]
                    # self.prob +=  et_W * self.XR['readout_happens'+s+'_'+str(n)] >= self.ZE['readout_end_time_'+s+'_'+str(n)]

                # processng time dependency on binary variable  # may be redundant
                # self.prob += self.ZE['readout_end_time_'+s+'_'+str(n)] - self.ZS['readout_start_time_'+s+'_'+str(n)] >= self.XR['readout_happens'+s+'_'+str(n)]
                # self.prob += self.ZE['readout_end_time_'+s+'_'+str(n)] - self.ZS['readout_start_time_'+s+'_'+str(n)] <= self.M * self.XR['readout_happens'+s+'_'+str(n)]
                
                # max_processing time of readout operation based on thermal constraint
                #self.prob += self.ZE['readout_end_time_'+s+'_'+str(n)] - self.ZS['readout_start_time_'+s+'_'+str(n)] <= 120 ## TODO1

                # max_processing time of readout operation based on thermal constraint
                ptw_list = self.data['prev_tWList__s_TWI_dict__s'][s][s+'_'+str(n)]
            
                # if self.config['constraints']['thermal_constraint_readout']:
                #     self.prob += self.ZE['readout_end_time_'+s+'_'+str(n)] - self.ZS['readout_start_time_'+s+'_'+str(n)] <= self.max_heat_time_readout_dict[s]
                #     for p in ptw_list[1:]:
                #         #self.prob += self.ZS['readout_start_time_'+s+'_'+str(n)] >= self.ZE['readout_end_time_'+s+'_'+str(p)] + self.max_heat_time_readout_dict[s] - self.M * (1- self.XR['readout_happens'+s+'_'+str(p)])#TODO2
                #         self.prob += self.ZS['readout_start_time_'+s+'_'+str(n)] >= self.ZE['readout_end_time_'+s+'_'+str(p)] + \
                #                                                                         lpSum([self.betaR['bucket_HZ_'+s+'_'+str(n)+'_'+str(bi) ] * v[2] \
                #                                                                         for bi,v in self.data['heatTimeBucket_SCT_dict__s'][s].items()])\
                #                                                                 - self.M * (1- self.XR['readout_happens'+s+'_'+str(p)]) \
                #                                                                 - self.M * (1- self.XR['readout_happens'+s+'_'+str(n)])#TODO2
                #         self.prob += lpSum([self.betaR['bucket_HZ_'+s+'_'+str(n)+'_'+str(bi)] 
                #                             for bi in self.data['heatTimeBucket_SCT_dict__s'][s].keys()]) ==1 
                        
                #         for bi,v in self.data['heatTimeBucket_SCT_dict__s'][s].items():
                #             self.prob += self.ZE['readout_end_time_'+s+'_'+str(p)] -  self.ZS['readout_start_time_'+s+'_'+str(p)]\
                #                         >= v[0] - self.M * (1- self.betaR['bucket_HZ_'+s+'_'+str(n)+'_'+str(bi)] )
                #             self.prob += self.ZE['readout_end_time_'+s+'_'+str(p)] -  self.ZS['readout_start_time_'+s+'_'+str(p)]\
                #                         <= v[1] + self.M * (1- self.betaR['bucket_HZ_'+s+'_'+str(n)+'_'+str(bi)] )
                    
                
                if len(ptw_list) != 1 :
                    prev_window = ptw_list[1]

                    self.prob += self.camera_memory_value['camera_memory_value'+s+'_'+str(n)] == self.camera_memory_value['camera_memory_value'+s+'_'+str(prev_window)] + \
                                self.delta_camera_memory_value['delta_camera_memory_value'+s+'_'+str(n)]
                    
                    # self.prob += self.readout_memory_value['readout_memory_value'+s+'_'+str(n)] == self.readout_memory_value['readout_memory_value'+s+'_'+str(prev_window)] + \
                    #             self.readout_delta_memory_value['readout_delta_memory_value'+s+'_'+str(n)]
                else:
                    print('noImgGs_check',s,n,ptw_list)
                    self.prob += self.camera_memory_value['camera_memory_value'+s+'_'+str(n)] == self.data['initial_camera_memory_value__s'][s] + \
                        self.delta_camera_memory_value['delta_camera_memory_value'+s+'_'+str(n)]
                    
                    # self.prob += self.readout_memory_value['readout_memory_value'+s+'_'+str(n)] == self.data['initial_readout_camera_memory_value__s'][s] + \
                    #     self.readout_delta_memory_value['readout_delta_memory_value'+s+'_'+str(n)]  

                # print(s,n)
                # self.solve_model() 
                # if len(ptw_list) != 1 :
                #     print('camera')
                #     print(self.camera_memory_value['camera_memory_value'+s+'_'+str(n)].value(),self.camera_memory_value['camera_memory_value'+s+'_'+str(prev_window)].value(),self.delta_camera_memory_value['delta_camera_memory_value'+s+'_'+str(n)].value(),self.prob.objective.value())
                #     print('readout')
                #     print(self.readout_memory_value['readout_memory_value'+s+'_'+str(n)].value(),self.readout_memory_value['readout_memory_value'+s+'_'+str(prev_window)].value(),self.readout_delta_memory_value['readout_delta_memory_value'+s+'_'+str(n)].value())


                # else:
                #     print('camera')
                #     print(self.camera_memory_value['camera_memory_value'+s+'_'+str(n)].value(),self.delta_camera_memory_value['delta_camera_memory_value'+s+'_'+str(n)].value(),self.prob.objective.value())
                #     print('readout')
                #     print(self.readout_memory_value['readout_memory_value'+s+'_'+str(n)].value(),self.readout_delta_memory_value['readout_delta_memory_value'+s+'_'+str(n)].value())


            for only_img_case in self.data['Memory_onlyImgTW_list']:

                st_W = only_img_case[0]
                et_W = only_img_case[1]
                s = only_img_case[2]
                n = only_img_case[3]
                j = only_img_case[4]
                k = only_img_case[5]
                #print(et_W,st_W,et_W-st_W)

            
                # for imaging readout opertion is strictly not happening.
                #self.prob += self.XR['readout_happens'+s+'_'+str(n)] == 0
                # change in readout memory
                #self.prob += self.readout_delta_memory_value['readout_delta_memory_value'+s+'_'+str(n)] == 0
                # change in camera memory
                #self.prob += self.delta_camera_memory_value['delta_camera_memory_value'+s+'_'+str(n)] == 0
                #  at most one of imaging or readout operation will happen
                #self.prob += self.XR['readout_happens'+s+'_'+str(n)] + self.x_o ['oppr_'+s+'_'+j+'_'+str(k)] <= 1

                # change in camera memory depends on imaging 
                self.prob += self.delta_camera_memory_value['delta_camera_memory_value'+s+'_'+str(n)] <= self.data['imaging_rate'] * (et_W-st_W) + self.M * (1-self.x_o ['oppr_'+s+'_'+j+'_'+str(k)] )
                self.prob += self.delta_camera_memory_value['delta_camera_memory_value'+s+'_'+str(n)] >= self.data['imaging_rate'] * (et_W-st_W) - self.M * (1-self.x_o ['oppr_'+s+'_'+j+'_'+str(k)] )

                self.prob += self.delta_camera_memory_value['delta_camera_memory_value'+s+'_'+str(n)] <= self.M * (self.x_o ['oppr_'+s+'_'+j+'_'+str(k)])
                # self.data['imaging_rate'] * (et_W-st_W) + self.M * (self.x_o ['oppr_'+s+'_'+j+'_'+str(k)] )
                self.prob += self.delta_camera_memory_value['delta_camera_memory_value'+s+'_'+str(n)] >= -1 * self.M * (self.x_o ['oppr_'+s+'_'+j+'_'+str(k)])
                # self.data['imaging_rate'] * (et_W-st_W) + self.M * (1-self.x_o ['oppr_'+s+'_'+j+'_'+str(k)] )

                # start and end time bounds

                #self.prob +=  st_W * self.XR['readout_happens'+s+'_'+str(n)] <= self.ZS['readout_start_time_'+s+'_'+str(n)]
                #self.prob +=  et_W * self.XR['readout_happens'+s+'_'+str(n)] >= self.ZS['readout_start_time_'+s+'_'+str(n)]

                #self.prob +=  st_W * self.XR['readout_happens'+s+'_'+str(n)] <= self.ZE['readout_end_time_'+s+'_'+str(n)]
                #self.prob +=  et_W * self.XR['readout_happens'+s+'_'+str(n)] >= self.ZE['readout_end_time_'+s+'_'+str(n)]

                # processng time dependency on binary variable  # may be redundant
                #self.prob += self.ZE['readout_end_time_'+s+'_'+str(n)] - self.ZS['readout_start_time_'+s+'_'+str(n)] >= self.XR['readout_happens'+s+'_'+str(n)]
                #self.prob += self.ZE['readout_end_time_'+s+'_'+str(n)] - self.ZS['readout_start_time_'+s+'_'+str(n)] <= self.M * self.XR['readout_happens'+s+'_'+str(n)]
                
                # max_processing time of readout operation based on thermal constraint
                #self.prob += self.ZE['readout_end_time_'+s+'_'+str(n)] - self.ZS['readout_start_time_'+s+'_'+str(n)] <= 120

                # max_processing time of readout operation based on thermal constraint
                
                # =====================================================================================================================
                # if self.config['constraints']['thermal_constraint_readout']:
                #     self.prob += self.ZE['readout_end_time_'+s+'_'+str(n)] - self.ZS['readout_start_time_'+s+'_'+str(n)] <= self.max_heat_time_readout_dict[s]

                #     ptw_list = self.data['prev_tWList__s_TWI_dict__s'][s][s+'_'+str(n)]
                #     for p in ptw_list[1:]:
                #         #self.prob += self.ZS['readout_start_time_'+s+'_'+str(n)] >= self.ZE['readout_end_time_'+s+'_'+str(p)] + 140 - self.M * (1- self.XR['readout_happens'+s+'_'+str(p)])#TODO2
                #         self.prob += self.ZS['readout_start_time_'+s+'_'+str(n)] >= self.ZE['readout_end_time_'+s+'_'+str(p)] + \
                #                                                                 lpSum([self.betaR['bucket_HZ_'+s+'_'+str(n)+'_'+str(bi) ] * v[2] \
                #                                                                         for bi,v in self.data['heatTimeBucket_SCT_dict__s'][s].items()])\
                #                                                                 - self.M * (1- self.XR['readout_happens'+s+'_'+str(p)]) \
                #                                                                 - self.M * (1- self.XR['readout_happens'+s+'_'+str(n)])#TODO2
                #         self.prob += lpSum([self.betaR['bucket_HZ_'+s+'_'+str(n)+'_'+str(bi)] 
                #                             for bi in self.data['heatTimeBucket_SCT_dict__s'][s].keys()]) ==1 
                        
                #         for bi,v in self.data['heatTimeBucket_SCT_dict__s'][s].items():
                #             self.prob += self.ZE['readout_end_time_'+s+'_'+str(p)] -  self.ZS['readout_start_time_'+s+'_'+str(p)]\
                #                         >= v[0] - self.M * (1- self.betaR['bucket_HZ_'+s+'_'+str(n)+'_'+str(bi)] )
                #             self.prob += self.ZE['readout_end_time_'+s+'_'+str(p)] -  self.ZS['readout_start_time_'+s+'_'+str(p)]\
                #                         <= v[1] + self.M * (1- self.betaR['bucket_HZ_'+s+'_'+str(n)+'_'+str(bi)] )
                    
                # =====================================================================================================================
                if self.config['constraints']['thermal_constraint_imaging']:
                    self.prob += et_W - st_W <= self.max_heat_time_camera_dict[s] + self.M *(1-self.x_o ['oppr_'+s+'_'+j+'_'+str(k)]),"Thermal_max_time_operation_"+str(s)+'_'+str(n)

                    ptw_list_ = self.data['prev_ImagingTWList__s_TWI_dict__s'][s][s+'_'+str(n)]
                
                    for p in ptw_list_[1:]:
                        #self.prob += self.ZS['readout_start_time_'+s+'_'+str(n)] >= self.ZE['readout_end_time_'+s+'_'+str(p)] + 140 - self.M * (1- self.XR['readout_happens'+s+'_'+str(p)])#TODO2
                        prev_index_list = [ [only_img_case[0],only_img_case[1],only_img_case[5],only_img_case[4]] for only_img_case in self.data['Memory_onlyImgTW_list'] if ((only_img_case[3]==p) and (only_img_case[2]==s))  ] 
                    
                        et_W_p = prev_index_list[0][1]
                        st_W_p = prev_index_list[0][0]
                        k_p = prev_index_list[0][2]
                        j_p = prev_index_list[0][3]
                        self.prob += st_W >= et_W_p+ \
                                            lpSum([self.betaC['bucketC_HZ_'+s+'_'+str(p)+'_'+str(bi) ] * v[2] \
                                                for bi,v in self.data['heatCameraTimeBucket_SCT_dict__s'][s].items()])\
                                            - self.M * (1- self.x_o ['oppr_'+s+'_'+j+'_'+str(k)]) \
                                            - self.M * (1- self.x_o ['oppr_'+s+'_'+j_p+'_'+str(k_p)]),"Thermal_safe_cool_time_"+str(s)+'_'+str(n)+'_'+str(p) ##TODO2
                        
                        self.prob += lpSum([self.betaC['bucketC_HZ_'+s+'_'+str(p)+'_'+str(bi)] 
                                            for bi in self.data['heatCameraTimeBucket_SCT_dict__s'][s].keys()]) == 1,"Thermal_one_bucket_selection_"+str(s)+'_'+str(n)+'_'+str(p)
                        
                        for bi,v in self.data['heatCameraTimeBucket_SCT_dict__s'][s].items():
                            self.prob += et_W_p -  st_W_p\
                                        >= v[0] - self.M * (1- self.betaC['bucketC_HZ_'+s+'_'+str(p)+'_'+str(bi)] )
                            self.prob += et_W_p -  st_W_p\
                                        <= v[1] + self.M * (1- self.betaC['bucketC_HZ_'+s+'_'+str(p)+'_'+str(bi)] )
                #=====================================================================================================================

                ptw_list = self.data['prev_tWList__s_TWI_dict__s'][s][s+'_'+str(n)]
                if len(ptw_list) != 1 :
                    prev_window = ptw_list[1]
                    self.prob += self.camera_memory_value['camera_memory_value'+s+'_'+str(n)] == self.camera_memory_value['camera_memory_value'+s+'_'+str(prev_window)] + \
                                self.delta_camera_memory_value['delta_camera_memory_value'+s+'_'+str(n)]
                    
                    # self.prob += self.readout_memory_value['readout_memory_value'+s+'_'+str(n)] == self.readout_memory_value['readout_memory_value'+s+'_'+str(prev_window)] + \
                    #             self.readout_delta_memory_value['readout_delta_memory_value'+s+'_'+str(n)]
                else:
                    print("img_check",s,n,ptw_list)
                    
                    self.prob += self.camera_memory_value['camera_memory_value'+s+'_'+str(n)] == self.data['initial_camera_memory_value__s'][s] + \
                        self.delta_camera_memory_value['delta_camera_memory_value'+s+'_'+str(n)]
                    
                    # self.prob += self.readout_memory_value['readout_memory_value'+s+'_'+str(n)] == self.data['initial_readout_camera_memory_value__s'][s] + \
                    #     self.readout_delta_memory_value['readout_delta_memory_value'+s+'_'+str(n)]
                # print(s,n)
                # self.solve_model()
                # if len(ptw_list) != 1 :
                #     print(self.camera_memory_value['camera_memory_value'+s+'_'+str(n)].value(),self.camera_memory_value['camera_memory_value'+s+'_'+str(prev_window)].value(),self.delta_camera_memory_value['delta_camera_memory_value'+s+'_'+str(n)].value(),self.prob.objective.value())

                # else:
                #     print(self.camera_memory_value['camera_memory_value'+s+'_'+str(n)].value(),self.delta_camera_memory_value['delta_camera_memory_value'+s+'_'+str(n)].value(),self.prob.objective.value())


            for only_gs_case in self.data['Memory_onlyGsTW_list']:

                st_W = only_gs_case[0]
                et_W = only_gs_case[1]
                s = only_gs_case[2]
                n = only_gs_case[3]
                g = only_gs_case[4]

                # DV
                #self.GC = {'downlink_camera_memory_'+s+'_'+str(g)+'_'+str(n):LpVariable('downlink_camera_memory_'+s+'_'+str(g)+'_'+str(n), cat= 'Binary' )}
                #self.GR = {'downlink_readout_memory_'+s+'_'+str(g)+'_'+str(n):LpVariable('downlink_readout_memory_'+s+'_'+str(g)+'_'+str(n), cat= 'Binary' )}
                #self.GP = {'ground_PAss_happens_'+s+'_'+str(n):LpVariable('ground_PAss_happens_'+s+'_'+str(n), cat= 'Binary' )}
                #self.Pgs = {'process_time_GP_'+s+'_'+str(g)+'_'+str(n) : LpVariable('process_time_GP_'+s+'_'+str(g)+'_'+str(n) , cat= 'Continous',lowBound = 0  ) }

                # No change in camera memory 
                
                self.prob += self.delta_camera_memory_value['delta_camera_memory_value'+s+'_'+str(n)] ==0
                
            

                # No chanege in readout memory 

                #self.prob += self.readout_delta_memory_value['readout_delta_memory_value'+s+'_'+str(n)] == 0

                


            
                # No readout Operation
                
                #self.prob += self.XR['readout_happens'+s+'_'+str(n)] == 0

                

            
                ## change in readout memory due to downlink
            

                ## relation between GC and GE with GP
                self.prob += self.GR['downlink_readout_memory_'+s+'_'+str(g)+'_'+str(n)] + self.GC['downlink_camera_memory_'+s+'_'+str(g)+'_'+str(n)] == self.GP['ground_PAss_happens_'+s+'_'+str(n)]
                self.prob += self.GC['downlink_camera_memory_'+s+'_'+str(g)+'_'+str(n)] == 0
                ## relation between XR and GP
                self.prob +=  self.GP['ground_PAss_happens_'+s+'_'+str(n)] <= 1

                # processing time bounds if GP doesn't happens then process time = 0 
                self.prob += self.Pgs['process_time_GP_'+s+'_'+str(g)+'_'+str(n)] <= (et_W-st_W) * self.GP['ground_PAss_happens_'+s+'_'+str(n)]

                # processing time will be at least 1 sec if GS pass happens  # TODO1 may be redundant
                self.prob += self.Pgs['process_time_GP_'+s+'_'+str(g)+'_'+str(n)] >= self.GP['ground_PAss_happens_'+s+'_'+str(n)]

                # start and end time bounds

                #self.prob +=  st_W * self.XR['readout_happens'+s+'_'+str(n)] <= self.ZS['readout_start_time_'+s+'_'+str(n)]
                #self.prob +=  et_W * self.XR['readout_happens'+s+'_'+str(n)] >= self.ZS['readout_start_time_'+s+'_'+str(n)]

                #self.prob +=  st_W * self.XR['readout_happens'+s+'_'+str(n)] <= self.ZE['readout_end_time_'+s+'_'+str(n)]
                #self.prob +=  et_W * self.XR['readout_happens'+s+'_'+str(n)] >= self.ZE['readout_end_time_'+s+'_'+str(n)]

                # processng time dependency on binary variable  # may be redundant
                #self.prob += self.ZE['readout_end_time_'+s+'_'+str(n)] - self.ZS['readout_start_time_'+s+'_'+str(n)] >= self.XR['readout_happens'+s+'_'+str(n)]
                #self.prob += self.ZE['readout_end_time_'+s+'_'+str(n)] - self.ZS['readout_start_time_'+s+'_'+str(n)] <= self.M * self.XR['readout_happens'+s+'_'+str(n)]
                
                # max_processing time of readout operation based on thermal constraint
                #self.prob += self.ZE['readout_end_time_'+s+'_'+str(n)] - self.ZS['readout_start_time_'+s+'_'+str(n)] <= 120

                # max_processing time of readout operation based on thermal constraint
                # =====================================================================================================================
                
                ptw_list = self.data['prev_tWList__s_TWI_dict__s'][s][s+'_'+str(n)]
                # if self.config['constraints']['thermal_constraint_readout']:
                #     self.prob += self.ZE['readout_end_time_'+s+'_'+str(n)] - self.ZS['readout_start_time_'+s+'_'+str(n)] <= self.max_heat_time_readout_dict[s]

                #     for p in ptw_list[1:]:
                #         self.prob += self.ZS['readout_start_time_'+s+'_'+str(n)] >= self.ZE['readout_end_time_'+s+'_'+str(p)] + 140 - self.M * (1- self.XR['readout_happens'+s+'_'+str(p)])#TODO2
                #         self.prob += self.ZS['readout_start_time_'+s+'_'+str(n)] >= self.ZE['readout_end_time_'+s+'_'+str(p)] + \
                #                                                                 lpSum([self.betaR['bucket_HZ_'+s+'_'+str(n)+'_'+str(bi) ] * v[2] \
                #                                                                         for bi,v in self.data['heatTimeBucket_SCT_dict__s'][s].items()])\
                #                                                                 - self.M * (1- self.XR['readout_happens'+s+'_'+str(p)]) \
                #                                                                 - self.M * (1- self.XR['readout_happens'+s+'_'+str(n)])#TODO2
                #         self.prob += lpSum([self.betaR['bucket_HZ_'+s+'_'+str(n)+'_'+str(bi)] 
                #                             for bi in self.data['heatTimeBucket_SCT_dict__s'][s].keys()]) ==1 
                        
                #         for bi,v in self.data['heatTimeBucket_SCT_dict__s'][s].items():
                #             self.prob += self.ZE['readout_end_time_'+s+'_'+str(p)] -  self.ZS['readout_start_time_'+s+'_'+str(p)]\
                #                         >= v[0] - self.M * (1- self.betaR['bucket_HZ_'+s+'_'+str(n)+'_'+str(bi)] )
                #             self.prob += self.ZE['readout_end_time_'+s+'_'+str(p)] -  self.ZS['readout_start_time_'+s+'_'+str(p)]\
                #                         <= v[1] + self.M * (1- self.betaR['bucket_HZ_'+s+'_'+str(n)+'_'+str(bi)] )
                    


                if len(ptw_list) != 1 :
                    prev_window = ptw_list[1]

                    self.prob += self.camera_memory_value['camera_memory_value'+s+'_'+str(n)] == self.camera_memory_value['camera_memory_value'+s+'_'+str(prev_window)] + \
                                self.delta_camera_memory_value['delta_camera_memory_value'+s+'_'+str(n)]
                    
                    # self.prob += self.readout_memory_value['readout_memory_value'+s+'_'+str(n)] == self.readout_memory_value['readout_memory_value'+s+'_'+str(prev_window)] + \
                    #             self.readout_delta_memory_value['readout_delta_memory_value'+s+'_'+str(n)]
                else:
                    print('only gs check ',s,n,ptw_list)
                    self.prob += self.camera_memory_value['camera_memory_value'+s+'_'+str(n)] == self.data['initial_camera_memory_value__s'][s] + \
                        self.delta_camera_memory_value['delta_camera_memory_value'+s+'_'+str(n)]
                    
                    # self.prob += self.readout_memory_value['readout_memory_value'+s+'_'+str(n)] == self.data['initial_readout_camera_memory_value__s'][s] + \
                    #     self.readout_delta_memory_value['readout_delta_memory_value'+s+'_'+str(n)]
                    
                # print(s,n)
                # self.solve_model()
                # if len(ptw_list) != 1 :
                #     print(self.camera_memory_value['camera_memory_value'+s+'_'+str(n)].value(),self.camera_memory_value['camera_memory_value'+s+'_'+str(prev_window)].value(),self.delta_camera_memory_value['delta_camera_memory_value'+s+'_'+str(n)].value(),self.prob.objective.value())

                # else:
                #     print(self.camera_memory_value['camera_memory_value'+s+'_'+str(n)].value(),self.delta_camera_memory_value['delta_camera_memory_value'+s+'_'+str(n)].value(),self.prob.objective.value())


                
        # power constraints
        if self.config['constraints']['power_constraints']:
            for no_img_gs_pass_case in self.data['Power_NoimageGs_TW_list']:

                # self.delta_power_value = { 'delta_power_value'+s+'_'+str(n) 
                # self.power_value = { 'power_value'+s+'_'+str(n) 
                # DV
                st_W = no_img_gs_pass_case[0]
                et_W = no_img_gs_pass_case[1]
                s = no_img_gs_pass_case[2]
                no_gs_img_id = no_img_gs_pass_case[3]
                no_use_TWindex = no_img_gs_pass_case[4]
                eclipse_event = no_img_gs_pass_case[5]
                n = no_img_gs_pass_case[6]
                
                if eclipse_event == 1:
                    #print((0-40.6)*(et_W-st_W),s,n,eclipse_event,"ABS")
                    self.prob += self.delta_power_value['delta_power_value'+s+'_'+str(n)] == (0-40.6)*(et_W-st_W)
                else:
                    #print((0-40.6)*(et_W-st_W),s,n,eclipse_event,"QWE")
                    self.prob += self.delta_power_value['delta_power_value'+s+'_'+str(n)] == 115 * self.PtPG['Power_generation_'+s+'_'+str(n)]\
                                                                                            - 28.1 *(et_W-st_W)
                    
                    self.prob += self.PtPG['Power_generation_'+s+'_'+str(n)] <=  et_W - st_W
                
                ptw_list = self.data['prev_power_tWList__s_TWI_dict__s'][s][s+'_'+str(n)]

                if len(ptw_list) != 1 :
                    prev_window = ptw_list[1]
                    self.prob += self.power_value['power_value'+s+'_'+str(n)] == self.power_value['power_value'+s+'_'+str(prev_window)] +  self.delta_power_value['delta_power_value'+s+'_'+str(n)]
                else:
                    self.prob += self.power_value['power_value'+s+'_'+str(n)] == self.data['initial_power_value__s'][s] +  self.delta_power_value['delta_power_value'+s+'_'+str(n)]


            for only_img_case in self.data['Power_image_TW_list']:

                # self.delta_power_value = { 'delta_power_value'+s+'_'+str(n) 
                # self.power_value = { 'power_value'+s+'_'+str(n) 

                
                st_W = only_img_case[0]
                et_W = only_img_case[1]
                s = only_img_case[2]
                j = only_img_case[3]
                k = only_img_case[4]
                eclipse_event = only_img_case[5]
                n = only_img_case[6]
                

                self.prob += self.delta_power_value['delta_power_value'+s+'_'+str(n)] <= (46-86) * (et_W-st_W) + self.M*(1- self.x_o ['oppr_'+s+'_'+j+'_'+str(k)])
                self.prob += self.delta_power_value['delta_power_value'+s+'_'+str(n)] >= (46-86) * (et_W-st_W) - self.M*(1- self.x_o ['oppr_'+s+'_'+j+'_'+str(k)])

                self.prob += self.delta_power_value['delta_power_value'+s+'_'+str(n)] <= (115) * self.PtPG['Power_generation_'+s+'_'+str(n)] - 28.1 *(et_W-st_W) + self.M*( self.x_o ['oppr_'+s+'_'+j+'_'+str(k)])
                self.prob += self.delta_power_value['delta_power_value'+s+'_'+str(n)] >= (115) * self.PtPG['Power_generation_'+s+'_'+str(n)] - 28.1 *(et_W-st_W) - self.M*( self.x_o ['oppr_'+s+'_'+j+'_'+str(k)])

                self.prob += self.PtPG['Power_generation_'+s+'_'+str(n)] <= (et_W-st_W)
                ptw_list = self.data['prev_power_tWList__s_TWI_dict__s'][s][s+'_'+str(n)]
                if len(ptw_list) != 1 :
                    prev_window = ptw_list[1]
                    self.prob += self.power_value['power_value'+s+'_'+str(n)] == self.power_value['power_value'+s+'_'+str(prev_window)] +  self.delta_power_value['delta_power_value'+s+'_'+str(n)]
                else:
                    self.prob += self.power_value['power_value'+s+'_'+str(n)] == self.data['initial_power_value__s'][s] +  self.delta_power_value['delta_power_value'+s+'_'+str(n)]


            for only_gs_case in self.data['Power_GS_TW_list']:
                # [a['start_time'],a['end_time'],a['SatID'],a['gsID'],a['TW_index'],a['Eclipse'],a['global_TW_index']]

                st_W = only_gs_case[0]
                et_W = only_gs_case[1]
                s = only_gs_case[2]
                g = only_gs_case[3]
                k = only_gs_case[4]
                eclipse_event = only_gs_case[5]
                n = only_gs_case[6]

                #DV
                #self.GP = {'ground_PAss_happens_'+s+'_'+str(n):LpVariable('ground_PAss_happens_'+s+'_'+str(n), cat= 'Binary' )}
                #self.Pgs = {'process_time_GP_'+s+'_'+str(g)+'_'+str(n) : LpVariable('process_time_GP_'+s+'_'+str(g)+'_'+str(n) , cat= 'Continous',lowBound = 0  ) }

                #self.Qtgs = {'Q_Power_generation_'+s+'_'+str(n) : LpVariable('Q_Power_generation_'+s+'_'+str(n) , cat= 'Continuous',lowBound = 0  ) }

                self.prob += self.PtPG['Power_generation_'+s+'_'+str(n)] <= (et_W-st_W)

                if eclipse_event == 1:
                    self.prob += self.delta_power_value['delta_power_value'+s+'_'+str(n)] >= (0-40.6)*(et_W-st_W) - self.M * self.GP['ground_PAss_happens_'+s+'_'+str(n)]
                    self.prob += self.delta_power_value['delta_power_value'+s+'_'+str(n)] <= (0-40.6)*(et_W-st_W) + self.M * self.GP['ground_PAss_happens_'+s+'_'+str(n)]
                    
                    self.prob += self.delta_power_value['delta_power_value'+s+'_'+str(n)] >= (0-122.9)* self.Pgs['process_time_GP_'+s+'_'+str(g)+'_'+str(n)] \
                                                                                            + (0-40.6) *(et_W - st_W -self.Pgs['process_time_GP_'+s+'_'+str(g)+'_'+str(n)] ) \
                                                                                                - self.M * (1-self.GP['ground_PAss_happens_'+s+'_'+str(n)])
                    
                    self.prob += self.delta_power_value['delta_power_value'+s+'_'+str(n)] <= (0-122.9)* self.Pgs['process_time_GP_'+s+'_'+str(g)+'_'+str(n)] \
                                                                                            + (0-40.6) *(et_W - st_W -self.Pgs['process_time_GP_'+s+'_'+str(g)+'_'+str(n)] ) \
                                                                                                + self.M * (1-self.GP['ground_PAss_happens_'+s+'_'+str(n)])
                    
                    
                else:
                    print(s,n)
                    self.prob += self.delta_power_value['delta_power_value'+s+'_'+str(n)] >= 115 * self.PtPG['Power_generation_'+s+'_'+str(n)] - 28.1 * (et_W - st_W) - self.M * self.GP['ground_PAss_happens_'+s+'_'+str(n)]
                    self.prob += self.delta_power_value['delta_power_value'+s+'_'+str(n)] <= 115 * self.PtPG['Power_generation_'+s+'_'+str(n)] - 28.1 * (et_W - st_W) + self.M * self.GP['ground_PAss_happens_'+s+'_'+str(n)]

                    self.prob += self.delta_power_value['delta_power_value'+s+'_'+str(n)] <= (60-110.4) * self.PtPG['Power_generation_'+s+'_'+str(n)] \
                                                                                        + 115 * self.Qtgs['Q_Power_generation_'+s+'_'+str(n)] \
                                                                                            - 28.1 * (et_W - st_W) + self.M * (1-self.GP['ground_PAss_happens_'+s+'_'+str(n)])
                    

                    self.prob += self.delta_power_value['delta_power_value'+s+'_'+str(n)] >= (60-110.4) * self.PtPG['Power_generation_'+s+'_'+str(n)] \
                                                                                        + 115 * self.Qtgs['Q_Power_generation_'+s+'_'+str(n)] \
                                                                                            - 28.1 * (et_W - st_W) - self.M * (1-self.GP['ground_PAss_happens_'+s+'_'+str(n)])

                    self.prob += self.Qtgs['Q_Power_generation_'+s+'_'+str(n)] <= et_W - st_W - self.PtPG['Power_generation_'+s+'_'+str(n)]
                ptw_list = self.data['prev_power_tWList__s_TWI_dict__s'][s][s+'_'+str(n)]
                if len(ptw_list) != 1 :
                    prev_window = ptw_list[1]
                    self.prob += self.power_value['power_value'+s+'_'+str(n)] == self.power_value['power_value'+s+'_'+str(prev_window)] +  self.delta_power_value['delta_power_value'+s+'_'+str(n)]
                else:
                    self.prob += self.power_value['power_value'+s+'_'+str(n)] == self.data['initial_power_value__s'][s] +  self.delta_power_value['delta_power_value'+s+'_'+str(n)]

    
    #self.x_o = { 'oppr_'+sjk : LpVariable('oppr_'+sjk ,cat = 'Binary') for sjk in self.data['unique_opportunities'] }   
    def create_objective(self):
        if self.config['objective']['GS_Pass_time']:
            print("GS_Pass_time_objective")
            A = lpSum([self.x_o['oppr_'+sjk] *  self.data['TotalPriority__csjk'][sjk] for sjk in self.data['unique_img_opportunities_list']] )
            # B = lpSum([self.ZE['readout_end_time_'+s+'_'+str(n)] - self.ZS['readout_start_time_'+s+'_'+str(n)]  \
            #                                                                                   for s in self.data['imgery_sat_id_list']+self.data['only_gs_sat_id_list'] \
            #                                                                                   if s in self.data['dedicatedReadoutTWIndex__sat'].keys()
            #                                                                                   for n in self.data['dedicatedReadoutTWIndex__sat'][s]]  )
            
            B = lpSum([self.readout_memory_value['readout_memory_value'+s+'_'+str(n)] * self.data['DROPriority__concat_sat_memoryTWindex'][s+'_'+str(n)] \
                                                                                                for s in self.data['imgery_sat_id_list']+self.data['only_gs_sat_id_list'] \
                                                                                                if s in self.data['dedicatedReadoutTWIndex__sat'].keys()
                                                                                                for n in self.data['dedicatedReadoutTWIndex__sat'][s]]  )

            #* self.data['DROPriority__concat_sat_memoryTWindex'][s+'_'+str(n)]


            
            
            C =lpSum([self.Pgs['process_time_GP_'+only_gs_case[2]+'_'+str(only_gs_case[4])+'_'+str(only_gs_case[3])] \
                                         for only_gs_case in self.data['Memory_onlyGsTW_list']])
            
            D = lpSum([self.XR['readout_happens'+s+'_'+str(n)] for s in self.data['imgery_sat_id_list']+self.data['only_gs_sat_id_list'] if s in self.data['dedicatedReadoutTWIndex__sat'].keys() for n in self.data['dedicatedReadoutTWIndex__sat'][s]])
            

            #self.prob += A >= sum([self.data['TotalPriority__csjk'][sjk] for sjk in self.data['unique_img_opportunities_list']])* 0.5
            #self.prob += A + B + C - D * 10 # #A + C#C + B #A + C +B

            # for s in self.data['imgery_sat_id_list']+self.data['only_gs_sat_id_list']:
            #     if s in self.data['dedicatedReadoutTWIndex__sat'].keys():
            #         for n in self.data['dedicatedReadoutTWIndex__sat'][s]:
            #             self.prob += self.ZE['readout_end_time_'+s+'_'+str(n)] - self.ZS['readout_start_time_'+s+'_'+str(n)] >= 50 * self.


            self.prob += (A + C)#(A + C)*1000 + B*0.01 #- D *10  # #A + C#C + B #A + C +B
            
            
        if self.config['objective']['total_priority']:
            print("total_priority_objective")
            self.prob += lpSum([self.Pgs['process_time_GP_'+only_gs_case[2]+'_'+str(only_gs_case[4])+'_'+str(only_gs_case[3])] \
                                         for only_gs_case in self.data['Memory_onlyGsTW_list']]) >= self.data['GS_Pass_time_objective']
            self.prob += lpSum([self.x_o['oppr_'+sjk] * self.data['TotalPriority__csjk'][sjk] for sjk in self.data['unique_img_opportunities_list']] )
            #lpSum([self.x_o['oppr_'+sjk] * self.data['TotalPriority__csjk'][sjk] for sjk in self.data['unique_img_opportunities_list']] )



        if self.config['objective']['total_readout_memory']:
            print("total_readout_memory_objective")
            # self.prob += lpSum([self.Pgs['process_time_GP_'+only_gs_case[2]+'_'+str(only_gs_case[4])+'_'+str(only_gs_case[3])] \
            #                              for only_gs_case in self.data['Memory_onlyGsTW_list']]) >= self.data['GS_Pass_time_objective']
            
            # self.prob += lpSum([self.x_o['oppr_'+sjk] * self.data['TotalPriority__csjk'][sjk] \
            #                     for sjk in self.data['unique_img_opportunities_list']]) >= self.data['total_priority_objective']

            # self.prob += lpSum([self.ZE['readout_end_time_'+s+'_'+str(n)] - self.ZS['readout_start_time_'+s+'_'+str(n)] for s in self.data['imgery_sat_id_list']+self.data['only_gs_sat_id_list'] \
            #                                                                                   for n in self.data['MemoryglobalTWindexSortedList__s'][s]] )

            A = lpSum([self.x_o['oppr_'+sjk] *  self.data['TotalPriority__csjk'][sjk] for sjk in self.data['unique_img_opportunities_list']] )
            # B = lpSum([self.ZE['readout_end_time_'+s+'_'+str(n)] - self.ZS['readout_start_time_'+s+'_'+str(n)]  \
            #                                                                                   for s in self.data['imgery_sat_id_list']+self.data['only_gs_sat_id_list'] \
            #                                                                                   if s in self.data['dedicatedReadoutTWIndex__sat'].keys()
            #                                                                                   for n in self.data['dedicatedReadoutTWIndex__sat'][s]]  )
            
            B = lpSum([self.readout_memory_value['readout_memory_value'+s+'_'+str(n)] * self.data['DROPriority__concat_sat_memoryTWindex'][s+'_'+str(n)] \
                                                                                                for s in self.data['imgery_sat_id_list']+self.data['only_gs_sat_id_list'] \
                                                                                                if s in self.data['dedicatedReadoutTWIndex__sat'].keys()
                                                                                                for n in self.data['dedicatedReadoutTWIndex__sat'][s]]  )

            #* self.data['DROPriority__concat_sat_memoryTWindex'][s+'_'+str(n)]


            
            
            C =lpSum([self.Pgs['process_time_GP_'+only_gs_case[2]+'_'+str(only_gs_case[4])+'_'+str(only_gs_case[3])] \
                                         for only_gs_case in self.data['Memory_onlyGsTW_list']])
            
            D = lpSum([self.XR['readout_happens'+s+'_'+str(n)] for s in self.data['imgery_sat_id_list']+self.data['only_gs_sat_id_list'] if s in self.data['dedicatedReadoutTWIndex__sat'].keys() for n in self.data['dedicatedReadoutTWIndex__sat'][s]])
            self.prob += (A + C) >= self.data['GS_Pass_time_objective']-1
            self.prob += B
            

    def solve_model(self):
        #solver = getSolver('PULP_CBC_CMD')
        solver = getSolver('HiGHS', timeLimit=180,msg = True,gapRel=0)
        status=self.prob.solve(solver)
        #status=self.prob.solve()
        print("status=",LpStatus[status])

