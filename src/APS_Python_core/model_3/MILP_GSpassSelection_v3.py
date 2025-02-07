import pandas as pd
from pulp import *
import time


class GSpassSelection():
    def __init__(self, data,config,logger):
        '''
        data : 'satellite_id', 'GS_id', 'TW_list_SatIdGSID', \
                'TW_index_list_SatIdGSID', 'eclipse', 'Concat_SatIdGSID',\
                'SG1K1G2K2_pair':{'sgk_list':,'domain_of_csgk':},\
                'GS1K1S2K2_pair':{'sgk_list':,'domain_of_csgk':},'thermal_capacity',\
                'csgList_s','TW_csgk'
                
        '''
        self.M = 1000000000
        self.config = config
        self.logger = logger
        self.data = data
        
        self.setup_time_S2G = self.data['gs_pass_setup_times'][("sat_id","gs_id")]
        self.setup_time_G2S = self.data['gs_pass_setup_times'][("gs_id","sat_id")]
       
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
        self.Z =  LpVariable('Z', cat='Continuous' ,lowBound = 0 )

        if self.config['constraints']['Thermal_constraints_GS_pass']:
            self.beta_gs = {'bucketC_HZ_'+csg+'_'+str(k)+'_'+str(bi) :LpVariable('bucketC_HZ_'+csg+'_'+str(k)+str(bi),cat = 'Binary') \
                             for csg in self.data['Concat_SatIdGSID']\
                                for k in self.data['TW_index_list_SatIdGSID'][csg]\
                                    for bi in self.data['heatTimeBucket_SCT_dict__s'][self.data['get_satellite'][csg]].keys()}
        
    def create_objective(self):
             
        self.prob += lpSum(self.p['p_'+csg + '_'+ str(k)] for csg in self.data['Concat_SatIdGSID']\
                          for k in self.data['TW_index_list_SatIdGSID'][csg])  + self.Z 


    def create_constraints(self):
        
        #equal_distribution_of processing time for each satellite 
        for s in self.data['satellite_id']:
            self.prob += lpSum([self.p['p_'+csg + '_'+ str(k)] for csg in self.data['csgList_s'][s]\
                                                              for k in self.data['TW_index_list_SatIdGSID'][csg]])\
                      >=  self.Z ,"equal_distribution_for_"+s
                      #>=  self.Z * self.data['weightage__sat'][s] ,"equal_distribution_for_"+s
            
        #enforces download time =0 , if download pass is not selected
        for csg in self.data['Concat_SatIdGSID']:
            for k in self.data['TW_index_list_SatIdGSID'][csg]:
                self.prob += (1-self.x['x_'+csg + '_'+ str(k)])* self.M + self.p['p_'+csg + '_'+ str(k)] <= self.M, "download process time =0_for"+csg+"_"+str(k)
                
                self.prob += self.p['p_'+csg + '_'+ str(k)] + self.t_d['st_'+csg + '_'+ str(k)] <= self.data['TW_csgk'][csg + '_'+ str(k)][1],"task end in upper bound of TW for_"+csg+"_"+str(k)
                
                self.prob += (1-self.x['x_'+csg + '_'+ str(k)])*self.M + self.t_d['st_'+csg + '_'+ str(k)] <= self.M,"if task not selected then start time enforced to zero for_"+csg+"_"+str(k)

                self.prob += self.x['x_'+csg + '_'+ str(k)] * self.M + self.data['TW_csgk'][csg + '_'+ str(k)][0] - self.t_d['st_'+csg + '_'+ str(k)] <= self.M ,"starting time shuold be grater than lower bound of time window for"+csg+"_"+str(k)


        l2 = []
        for s in self.data['satellite_id']:     
            for cs1g1k1 in self.data['SG1K1G2K2_pair']['sgk_list'][s]:
                for cs2g2k2 in self.data['SG1K1G2K2_pair']['domain_of_csgk'][cs1g1k1]:
                
                    if {cs1g1k1,cs2g2k2} not in l2:
                         
                        self.prob += self.x['x_'+cs2g2k2] + self.x['x_'+cs1g1k1] <= 1
                        
                        l2.append({cs1g1k1,cs2g2k2})
                            
        l1 = []
        for g in self.data['GS_id']:  
            for cs1g1k1 in self.data['GS1K1S2K2_pair']['sgk_list'][g]:
                for cs2g2k2 in self.data['GS1K1S2K2_pair']['domain_of_csgk'][cs1g1k1]:\
                
                    if {cs1g1k1,cs2g2k2} not in l1:
                        self.prob += self.x['x_'+cs2g2k2] + self.x['x_'+cs1g1k1] <= 1
                        l1.append({cs1g1k1,cs2g2k2})
        if self.config['constraints']['Thermal_constraints_GS_pass']:
            #st = time.time()
            for s in self.data['satellite_id']:
                for csg in self.data['csgList_s'][s]:
                    for k in self.data['TW_index_list_SatIdGSID'][csg]:
                        et_W = self.data['TW_csgk'][csg + '_'+ str(k)][1] 
                        st_W = self.data['TW_csgk'][csg + '_'+ str(k)][0]
                        self.prob += self.p['p_'+csg + '_'+ str(k)] <= self.data['max_XBT_heat_dict'][s] + self.M *(1-self.x['x_'+csg + '_'+ str(k)])

                        ptw_list_ = self.data['prev_tWList__s_TWI_dict__s'][s][csg+'_'+csg+'_'+str(k)]
                       
                        for csgk_p in ptw_list_[1:]:
                            prev_index_list = [ [only_img_case[3],only_img_case[4],only_img_case[1],only_img_case[5]] for only_img_case in self.data['prev_thermal_list'] if ((only_img_case[2]==csgk_p) and (only_img_case[0]==s)) ] 
                        
                            et_W_p = prev_index_list[0][1]
                            st_W_p = prev_index_list[0][0]
                            p = prev_index_list[0][3]
                            csg_p = prev_index_list[0][2]
                            
                            self.prob += self.t_d['st_'+csg + '_'+ str(k)] >= self.t_d['st_'+csg_p + '_'+ str(p)] +self.p['p_'+csg_p + '_'+ str(p)]  + \
                                                lpSum([self.beta_gs['bucketC_HZ_'+csg_p+'_'+str(p)+'_'+str(bi) ] * v[2] \
                                                    for bi,v in self.data['heatTimeBucket_SCT_dict__s'][s].items()])\
                                                - self.M * (1- self.x['x_'+csg + '_'+ str(k)]) \
                                                - self.M * (1- self.x['x_'+csg_p+ '_'+ str(p)])
                            
                            self.prob += lpSum([self.beta_gs['bucketC_HZ_'+csg_p+'_'+str(p)+'_'+str(bi) ] 
                                                for bi in self.data['heatTimeBucket_SCT_dict__s'][s].keys()]) == 1
                            for bi,v in self.data['heatTimeBucket_SCT_dict__s'][s].items():
                                self.prob += self.p['p_'+csg_p + '_'+ str(p)]\
                                            >= v[0] - self.M * (1- self.beta_gs['bucketC_HZ_'+csg_p+'_'+str(p)+'_'+str(bi) ] )
                                self.prob += self.p['p_'+csg_p + '_'+ str(p)]\
                                            <= v[1] + self.M * (1- self.beta_gs['bucketC_HZ_'+csg_p+'_'+str(p)+'_'+str(bi) ])
          
    def solve_model(self):

        solver = getSolver('HiGHS', timeLimit = self.config["timeLimit"] , msg = True, gapRel=0)
        status=self.prob.solve(solver)
      
        self.logger.info("GS-Pass-Selection-status="+LpStatus[status])
        
        
        
                                