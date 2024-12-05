import pandas as pd
from pulp import *


class GSpassSelection():
    def __init__(self, data,config):
        '''
        data : 'satellite_id', 'GS_id', 'TW_list_SatIdGSID', 'TW_index_list_SatIdGSID', 'eclipse', 'Concat_SatIdGSID'
        '''
        self.M = 1000000000
        self.config = config
        self.setup_time_S2G = 120
        self.setup_time_G2S = 120
        self.data = data
        self.create_model()
        self.create_DV()
        self.create_objective()
        self.create_constraints()

        
        self.solve_model()

    def create_model(self):
        self.prob = pulp.LpProblem("GS_pass_selection", LpMaximize)
        
    def create_DV(self):
        # variable to select pass or not
        self.x = {'x_'+csg + '_'+ str(k): LpVariable('x_'+csg + '_'+ str(k), cat='Binary') for csg in self.data['Concat_SatIdGSID']\
                                                        for k in self.data['TW_index_list_SatIdGSID'][csg]  }

        # variable to get processing time
        self.p = {'p_'+csg + '_'+ str(k) : LpVariable('p_'+csg + '_'+ str(k), cat='Continuous' ,lowBound = 0 ) \
                  for csg in self.data['Concat_SatIdGSID']\
                  for k in self.data['TW_index_list_SatIdGSID'][csg]  }

        #start time
        self.t_d = {'st_'+csg + '_'+ str(k) : LpVariable('st_'+csg + '_'+ str(k), cat='Continuous' ,lowBound = 0 ) \
                    for csg in self.data['Concat_SatIdGSID'] \
                    for k in self.data['TW_index_list_SatIdGSID'][csg] }
        
        # if g2 pass is strictly happening after  g1 , then thetha_S2G sg1g2 =1 , else =0
        self.theta_S2G = {'thethaS2G_'+s+'_'+cs1g1k1 + '_'+ cs2g2k2 : \
                          LpVariable('thethaS2G_'+s+'_'+cs1g1k1 + '_'+ cs2g2k2, cat='Binary' )\
                                                            for s in self.data['satellite_id']\
                          for cs1g1k1 in self.data['SG1K1G2K2_pair']['sgk_list'][s]\
                          for cs2g2k2 in self.data['SG1K1G2K2_pair']['domain_of_csgk'][cs1g1k1]}
        #if s2 pass is strictly happening after  s1 , then thetha_G2S gs1s2 =1 , else =0
        self.theta_G2S = {'thethaG2S_'+g+'_'+cs1g1k1 + '_'+ cs2g2k2 : \
                          LpVariable('thethaG2S_'+g+'_'+cs1g1k1 + '_'+ cs2g2k2, cat='Binary' ) \
                                                            for g in self.data['GS_id']  
                                                            for cs1g1k1 in self.data['GS1K1S2K2_pair']['sgk_list'][g]
                                                            for cs2g2k2 in self.data['GS1K1S2K2_pair']['domain_of_csgk'][cs1g1k1]                               
                         }
        #to have equal disrtribution of time for each satellute
        self.Z =  LpVariable('Z', cat='Continuous' ,lowBound = 0 )

        self.thermal_value =  {'thermal_value'+s+'_'+str(t) : \
                          LpVariable('thermal_value'+s+'_'+str(t), cat='Continuous',lowBound = 0,upBound = self.data['thermal_capacity'][s] ) \
                                                            for s in self.data['satellite_id']  
                                                            for t in range(12*60*60)                               
                         }
        

        
    def create_objective(self):
        #self.prob += 0
        self.prob += lpSum(self.p['p_'+csg + '_'+ str(k)] for csg in self.data['Concat_SatIdGSID']\
                          for k in self.data['TW_index_list_SatIdGSID'][csg]) + self.Z 


    def create_constraints(self):
        
        #equal_distribution_of processing time for each satellite 
        for s in self.data['satellite_id']:
            self.prob += lpSum(self.p['p_'+csg + '_'+ str(k)] for csg in self.data['csgList_s'][s]\
                                                              for k in self.data['TW_index_list_SatIdGSID'][csg])\
                      >=  self.Z,"equal_distribution_for_"+s
            
        #enforces download time =0 , if download pass is not selected
        for csg in self.data['Concat_SatIdGSID']:
            for k in self.data['TW_index_list_SatIdGSID'][csg]:
                self.prob += (1-self.x['x_'+csg + '_'+ str(k)])* self.M + self.p['p_'+csg + '_'+ str(k)] <= self.M, "download process time =0_for"+csg+"_"+str(k)
                
                self.prob += self.p['p_'+csg + '_'+ str(k)] + self.t_d['st_'+csg + '_'+ str(k)] <= self.data['TW_csgk'][csg + '_'+ str(k)][1],"task end in upper bound of TW for_"+csg+"_"+str(k)
                
                self.prob += (1-self.x['x_'+csg + '_'+ str(k)])*self.M + self.t_d['st_'+csg + '_'+ str(k)] <= self.M,"if task not selected then start time enforced to zero for_"+csg+"_"+str(k)

                self.prob += self.x['x_'+csg + '_'+ str(k)] * self.M + self.data['TW_csgk'][csg + '_'+ str(k)][0] - self.t_d['st_'+csg + '_'+ str(k)] <= self.M ,"starting time shuold be grater than lower bound of time window for"+csg+"_"+str(k)


        #
        for s in self.data['satellite_id']:  
            for cs1g1k1 in self.data['SG1K1G2K2_pair']['sgk_list'][s]:
                for cs2g2k2 in self.data['SG1K1G2K2_pair']['domain_of_csgk'][cs1g1k1]:
                    self.prob += self.theta_S2G['thethaS2G_'+s+'_'+cs1g1k1 + '_'+ cs2g2k2 ]* self.M + self.p['p_'+cs1g1k1] \
                              + self.t_d['st_'+   cs1g1k1] + self.setup_time_S2G - self.t_d['st_'+ cs2g2k2] <= self.M,"S2G_avoide_overlapfor_"+s+"_"+cs1g1k1+"_"+cs2g2k2
            
                    self.prob += self.theta_S2G['thethaS2G_'+s+'_'+cs1g1k1 + '_'+ cs2g2k2 ] \
                              + self.theta_S2G['thethaS2G_'+s+'_'+cs2g2k2 + '_'+ cs1g1k1 ]== 1,"S2G_avoid_overlap_forpart2_"+s+"_"+cs1g1k1+"_"+cs2g2k2


        for g in self.data['GS_id']:  
            for cs1g1k1 in self.data['GS1K1S2K2_pair']['sgk_list'][g]:
                for cs2g2k2 in self.data['GS1K1S2K2_pair']['domain_of_csgk'][cs1g1k1]:\
                
                    self.prob += self.theta_G2S['thethaG2S_'+g+'_'+cs1g1k1 + '_'+ cs2g2k2 ]* self.M + self.p['p_'+cs1g1k1]\
                    + self.t_d['st_'+   cs1g1k1] + self.setup_time_G2S - self.t_d['st_'+ cs2g2k2] <= self.M,"G2S_avoide_overlapfor_"+s+"_"+cs1g1k1+"_"+cs2g2k2
            
                    
                    self.prob += self.theta_G2S['thethaG2S_'+g+'_'+cs1g1k1 + '_'+ cs2g2k2]\
                    + self.theta_G2S['thethaG2S_'+g+'_'+cs2g2k2 + '_'+ cs1g1k1 ] == 1,"G2S_avoide_overlapforPart2_"+s+"_"+cs1g1k1+"_"+cs2g2k2
        
        if self.config['constraints'] ['Thermal_constraints']: 
            for s in self.data['satellite_id']:
                self.prob += self.thermal_value['thermal_value'+s+'_'+str(0)] == self.data['initial_thermal_value'][s]

            for s in self.data['satellite_id']:
                for t in range(1,12*60*60):
                    #theree case :
                    #heating stage:
                    #print(s,t,"START") # GP

                    if self.data['temporal_data_flag'][s][t] != -1 : # GS PASS Possible
                        #print(s,t,"flag-1")
                        delta = 0
                        flag_ptw = 0

                        for ind,item in enumerate(self.data['temporal_data_TW_list'][s][t]): #sorted TW with respect to starting time
                            
                            if  t < item[0] or t > item[1] :
                                continue # TODO1 optimize code so that this loop does not come

                            twi = self.data['temporal_data_TW_index_list'][s][t][ind]
                            gs = self.data['temporal_data_GSlist'][s][t][ind]

                            if flag_ptw ==1 :
                                pass
                

                            self.heat_binary= { 'HB_'+s+'_'+gs+'_'+str(twi)+'_'+str(t) :\
                                            LpVariable('HB_'+s+'_'+gs+'_'+str(twi)+'_'+str(t), cat='Binary' )} 
                            
                            self.heat_binary_1= { 'HB1_'+s+'_'+gs+'_'+str(twi)+'_'+str(t) :\
                                            LpVariable('HB1_'+s+'_'+gs+'_'+str(twi)+'_'+str(t), cat='Binary' )}
                            self.heat_binary_2= { 'HB2_'+s+'_'+gs+'_'+str(twi)+'_'+str(t) :\
                                            LpVariable('HB2_'+s+'_'+gs+'_'+str(twi)+'_'+str(t), cat='Binary' )}
                            self.heat_binary_3= { 'HB3_'+s+'_'+gs+'_'+str(twi)+'_'+str(t) :\
                                            LpVariable('HB3_'+s+'_'+gs+'_'+str(twi)+'_'+str(t), cat='Binary' )}
                            
                            
                            self.cool_binary= { 'CB_'+s+'_'+gs+'_'+str(twi)+'_'+str(t) :\
                                            LpVariable('CB_'+s+'_'+gs+'_'+str(twi)+'_'+str(t), cat='Binary' )} 
                            
                            self.steady_binary= { 'steady_'+s+'_'+gs+'_'+str(twi)+'_'+str(t) :\
                                            LpVariable('steady_'+s+'_'+gs+'_'+str(twi)+'_'+str(t), cat='Binary' )} 
                            

                            
                            # heat
                            self.prob += self.heat_binary['HB_'+s+'_'+gs+'_'+str(twi)+'_'+str(t) ] + self.cool_binary['CB_'+s+'_'+gs+'_'+str(twi)+'_'+str(t)] + self.steady_binary['steady_'+s+'_'+gs+'_'+str(twi)+'_'+str(t)] == 1  

                            self.prob += self.heat_binary_1['HB1_'+s+'_'+gs+'_'+str(twi)+'_'+str(t)]  <=  self.x['x_'+s+'_'+gs+'_'+str(twi)] * self.M# h=1 => x =1 ,h =0 => x=0,1
                            #self.prob += self.heat_binary['HB_'+s+'_'+gs+'_'+str(twi)+'_'+str(t)]*self.M  >=  self.x['x_'+s+'_'+gs+'_'+str(twi)] # h =1 =>x=0,1 ,h =0 ==> x =0

                            self.prob +=  t  - self.t_d['st_'+s+'_'+gs+'_'+str(twi)] >= (1-self.heat_binary_2['HB2_'+s+'_'+gs+'_'+str(twi)+'_'+str(t)])*-1*self.M # h=1 ==> rc , h=0 ==> can be anything
                            #self.prob +=  t  - self.t_d['st_'+s+'_'+gs+'_'+str(twi)] <= self.heat_binary['HB_'+s+'_'+gs+'_'+str(twi)+'_'+str(t)]*1*self.M - 0.0001 # h=0 ==> r1c1 , h=1 ==>can be a nything

                            self.prob +=  self.t_d['st_'+s+'_'+gs+'_'+str(twi)]+self.p['p_'+s+'_'+gs+'_'+str(twi)] -t  >= (1-self.heat_binary_3['HB3_'+s+'_'+gs+'_'+str(twi)+'_'+str(t)])*-1*self.M# h=1 ==> rc , h=0 ==> can be anything
                            #self.prob +=  self.t_d['st_'+s+'_'+gs+'_'+str(twi)]+self.p['p_'+s+'_'+gs+'_'+str(twi)] -t  <= self.heat_binary['HB_'+s+'_'+gs+'_'+str(twi)+'_'+str(t)] *1* self.M -0.0001# h=0 ==> r1c1 , h=1 ==>can be a nything

                            self.prob += self.heat_binary_1['HB1_'+s+'_'+gs+'_'+str(twi)+'_'+str(t)] + self.heat_binary_2['HB2_'+s+'_'+gs+'_'+str(twi)+'_'+str(t)] + self.heat_binary_3['HB3_'+s+'_'+gs+'_'+str(twi)+'_'+str(t)] >= 3 * self.heat_binary['HB_'+s+'_'+gs+'_'+str(twi)+'_'+str(t) ]
                            self.prob += self.heat_binary_1['HB1_'+s+'_'+gs+'_'+str(twi)+'_'+str(t)] + self.heat_binary_2['HB2_'+s+'_'+gs+'_'+str(twi)+'_'+str(t)] + self.heat_binary_3['HB3_'+s+'_'+gs+'_'+str(twi)+'_'+str(t)] <= 3 * self.heat_binary['HB_'+s+'_'+gs+'_'+str(twi)+'_'+str(t) ] + self.M * (1- self.heat_binary['HB_'+s+'_'+gs+'_'+str(twi)+'_'+str(t) ])



                            #=========================================================================================================================================================================================================================
                            # steady
                            self.prob += self.data['initial_thermal_value'][s] * 1.01 -  self.thermal_value['thermal_value'+s+'_'+str(t-1)] >= (1-self.steady_binary['steady_'+s+'_'+gs+'_'+str(twi)+'_'+str(t)] )* -1 * self.M  - self.heat_binary['HB_'+s+'_'+gs+'_'+str(twi)+'_'+str(t)] * self.M       # sb = 1 ==> prev_themal_value = initial temp ,  sb =0 ==> prev_themal_value can be anything
                            self.prob += self.data['initial_thermal_value'][s] * 1.01 -  self.thermal_value['thermal_value'+s+'_'+str(t-1)] <= self.heat_binary['HB_'+s+'_'+gs+'_'+str(twi)+'_'+str(t)] * self.M  + (self.steady_binary['steady_'+s+'_'+gs+'_'+str(twi)+'_'+str(t)] )* 1 * self.M  - 0.001 # sb = 0 ==> prev_themal_value > initial temp*1.01 ,  sb =1 ==> prev_themal_value can be anything
                            # =========================================================================================================================================================================================================================
                           
                            # =====================================Formulation for heating while ground pass===================================================================
                            # if t-self.t_d['st_'+s+'_'+gs+'_'+str(twi)] < 5 --> (t-self.t_d['st_'+s+'_'+gs+'_'+str(twi)]) *5 
                            # if t-self.t_d['st_'+s+'_'+gs+'_'+str(twi)] >= 5 --> 10 +(t-5) * 10
                            # v1 , v2 ====>  binary , v3 =====> continuous
                            #self.prob +=  t - self.t_d['st_'+s+'_'+gs+'_'+str(twi)] - 5 <=(1- v1) * M => v1 = 1 , t - t_d <= 5
                            #self.prob +=  t - self.t_d['st_'+s+'_'+gs+'_'+str(twi)] >=  5.00001 - (1-v2)  * M  => v2 = 1 , t - t_d >5 
                            #  v1 * f1(t-d) + v2 * f2(t-t_d) - v3 <= (1-hb)  * M
                            #  v1 * f1(t-d) + v2 * f2(t-t_d) - v3 >= (1-hb) * M  * -1
                            # v1 + v2 == hb  
                            # v3 <= hb * self.M
                            # delta  = v3 (for heating)
                            # =====================================Formulation for cooling while ground pass==============================
                            # if t-self.t_d['st_'+s+'_'+gs+'_'+str(twi)] < 5 --> (t-self.t_d['st_'+s+'_'+gs+'_'+str(twi)]) *5 
                            # if t-self.t_d['st_'+s+'_'+gs+'_'+str(twi)] >= 5 --> 10 +(t-5) * 10
                            # v1 , v2 ====>  binary , v3 =====> continuous
                            #self.prob +=  t - self.t_d['st_'+s+'_'+gs+'_'+str(twi)] - 5 <=(1- v1) * M => v1 = 1 , t - t_d <= 5
                            #self.prob +=  t - self.t_d['st_'+s+'_'+gs+'_'+str(twi)] >=  5.00001 - (1-v2)  * M  => v2 = 1 , t - t_d >5 
                            #if flag_ptw ==1 : 
                                #self.prob +=  t - self.t_d['st_'+s+'_'+prevgs+'_'+str(prev_twi)] - 5 <=(1- v1) * M => v3 = 1 , t - t_d <= 5
                                #self.prob +=  t - self.t_d['st_'+s+'_'+prevgs+'_'+str(prev_twi)] >=  5.00001 - (1-v4)  * M  => v4 = 1 , t - t_d >5 

                            #  v1 * f1(t-d) + v2 * f2(t-t_d) + v3 * f1(t-t_d(prev)-(t_p)) + v4 *f2(t-t_d(prev)-(t_p)) - v5  <= (1-cb) * M  
                            #  v1 * f1(t-d) + v2 * f2(t-t_d) + v3 * f1(t-t_d(prev)-(t_p)) + v4 *f2(t-t_d(prev)-(t_p)) - v5  >= (1-cb) * M  * -1

                            # v1 + v2  + v3 + v4 == cb  
                            # v1+ v2 <= x_o[td] * self.M
                            # v3+ v4 <= x_o[td_prev] * self.M
                            
                            # # if t <= t_d then  v3+v4 = cb 
                            # t - t_d <= bin * self.M  , bin =0 ==> t- t_d <= 0
                            # v3+v4 >= cb - (bin)*M 
                            # v3+v4 <= cb + (bin)*M

                            # v5 <= cb * self.M
                            # delta  = v5 (for cooling)



                            delta += self.heat_binary['HB_'+s+'_'+gs+'_'+str(twi)+'_'+str(t)] * 1 + \
                                    self.steady_binary['steady_'+s+'_'+gs+'_'+str(twi)+'_'+str(t)] * 0 + \
                                    self.cool_binary['CB_'+s+'_'+gs+'_'+str(twi)+'_'+str(t)] * -1
                            
                            prev_twi =  twi
                            prev_gp =  gs
                            flag_ptw = 1

                    
                    else:

                        self.cool_binary= { 'CB_'+s+'_'+str(t) :\
                                            LpVariable('CB_'+s+'_'+str(t), cat='Binary' )} 

                        self.steady_binary = { 'steady_'+s+'_'+str(t) :\
                                            LpVariable('steady_'+s+'_'+str(t), cat='Binary' )} 
                        #============================================================================================================================
                        self.prob += self.data['initial_thermal_value'][s]  -  self.thermal_value['thermal_value'+s+'_'+str(t-1)] >= (1-self.steady_binary['steady_'+s+'_'+str(t)] )* -1 * self.M
                        self.prob += self.data['initial_thermal_value'][s]  -  self.thermal_value['thermal_value'+s+'_'+str(t-1)] <= (self.steady_binary['steady_'+s+'_'+str(t)] )* 1 * self.M  - 0.001
                        #===========================================================================================================================================================
                        self.prob += self.cool_binary['CB_'+s+'_'+str(t)] + self.steady_binary['steady_'+s+'_'+str(t)] == 1 

                        #delta =  self.steady_binary['steady_'+s+'_'+str(t)] * 0 + self.cool_binary['steady_'+s+'_'+str(t)] * f() #TOCHECK
                        #print(s,t)
                        delta =  self.steady_binary['steady_'+s+'_'+str(t)]  * 0 + self.cool_binary['CB_'+s+'_'+str(t)] * -1
                        

                    self.prob += self.thermal_value['thermal_value'+s+'_'+str(t)] == self.thermal_value['thermal_value'+s+'_'+str(t-1)] + delta
                    self.prob += self.thermal_value['thermal_value'+s+'_'+str(t)] <= self.data['thermal_capacity'][s] # self.thermal_capacity[s] # TOCHECK
            
                    

                # 
                # x[csgk] = 1, # t_d[xsgk]>= t, t<=t_d[xsgk] +p[xsgk]
                #     if t[xsgk] == n
                #     delta = f(t-n)
                # cooling_stage
                #     current_temp >= initial_temp , false(x[csgk] = 1, # t_d[xsgk]>= t, t<=t_d[xsgk] +p[xsgk],flag= -1)
                #      delta = f(t - (max(t_d[csgk] + p[csgk] ) for k less than  or eq to t )

                # staedy stage
                #     current_temp == initial_temp , false(x[csgk] = 1, # t_d[xsgk]>= t, t<=t_d[xsgk] +p[xsgk],flag= -1)
                #     delta = 0
                        
        #         if self.data['temporal_data'][s]['flag'] == -1 :
        #             ground_station_status = 'NO'
                    
        #         else:
        #             ground_station_status = self.data['temporal_data'][s]['flag'][1]
        #             time_window_index_status = self.data['temporal_data'][s]['flag'][0]
                
        #         self.data['temporal_data'][s]['flag']
        #         if ground_status 
        #         Thermal_value[s][t] = Thermal_value[s][t-1] + delta_thermal[t]
                
        #         if t <= self.t_d['st_'+csg + '_'+ str(k)]:
        #         if t not downlin
        #         delta_thermal[t] =

        #         self.prob += self.thermal_value['thermal_value'+s+'_'+str(t)] =  self.thermal_value['thermal_value'+s+'_'+str(t-1)] + delta

        #         if ground_station_status != 'NO' :
        #             if t <= self.t_d['st_'+   cs1g1k1]:
        #                 if cooling_stage != 'NO':
                    
        #                     delta = 0
        #                 else:
        #                     delta = eqn()
                    

    def solve_model(self):
        print("start_solving")
        status = self.prob.solve(PULP_CBC_CMD(msg=False,timeLimit = 300 ))
        print("==================")
        print("status",LpStatus[status])
        
        
        
                                