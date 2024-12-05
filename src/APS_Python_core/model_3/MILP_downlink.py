from pulp import *
import pandas as pd


class ImageDownlinkPlan():
    def __init__(self, data,config):
        self.data = data
        self.config = config
        self.create_model()
        self.create_DV()
        self.create_constraints()
        self.create_objective()

        self.solve_model()


    def create_model(self):
        self.prob = pulp.LpProblem("image_downlink_plan", LpMaximize)
        
    def create_DV(self) :
        self.N = {'number'+'_'+str(i)+'_'+str(j)+'_'+str(g)+'_'+str(k):LpVariable('number'+'_'+str(i)+'_'+str(j)+'_'+str(g)+'_'+str(k),\
                                                                        lowBound = 0 , \
                                                                        upBound = self.data['NoOfTileStrip__imgID'][j],cat = 'Integer')\
                                                                        for i in self.data['all_satellite_list']\
                                                                        for j in self.data['capture_list__sat'][i]\
                                                                        for g in self.data['ground_station_list__sat'][i]\
                                                                        for k in self.data['TW_index_list__concatSatGs'][str(i)+'_'+str(g)]}
        
    
    def create_constraints(self):
        # 1 total time for tile strip cannot be greater than time allocated for GS pass Downlinking
        for i in self.data['all_satellite_list']:
            for g in self.data['ground_station_list__sat'][i]:
                TW_list = self.data['TW_list__concatSatGs'][i+'_'+g]
                TW_index_list = self.data['TW_index_list__concatSatGs'][i+'_'+g]
                for ind,TW in enumerate(TW_list) : 
                    st = TW[0]
                    et = TW[1]
                    k = TW_index_list[ind]
                    time_diff = et - st
                    self.prob += lpSum([self.N['number'+'_'+str(i)+'_'+str(j)+'_'+str(g)+'_'+str(k)] * self.data['time_per_tileStrip__imgID'][j] \
                                        for j in self.data['capture_list__sat'][i]] ) \
                                        <= time_diff
                    
        # 2 for assured downlinking task
        for item in self.data['assured_downlinking_imageSatIdList_list']:
            j = item[0]
            i = item[1]
            self.prob += lpSum([self.N ['number'+'_'+str(i)+'_'+str(j)+'_'+str(g)+'_'+str(k)] \
                                for g in self.data['ground_station_list__sat'][i]\
                                for k in self.data['TW_index_list__concatSatGs'][str(i)+'_'+str(g)]\
                                      ]) == self.data['tile_strip_no__concatSatidImgId'][str(i)+'_'+str(j)]
                    
    def create_objective(self):
        self.prob += lpSum([self.N['number'+'_'+str(i)+'_'+str(j)+'_'+str(g)+'_'+str(k)] * (self.data['totalPriority_imgID'][j]+ self.data['sat_gs_prioritydict__sat'][i][g])\
                            for i in self.data['all_satellite_list'] \
                            for j in self.data['capture_list__sat'][i] \
                            for g in self.data['ground_station_list__sat'][i]\
                            for k in self.data['TW_index_list__concatSatGs'][str(i)+'_'+str(g)]])
        
        
    def solve_model(self):
        #solver = getSolver('PULP_CBC_CMD')
        solver = getSolver('PULP_CBC_CMD', timeLimit=10,msg = False)
        status=self.prob.solve(solver)
        #status=self.prob.solve()
        print("status=",LpStatus[status])
     


    