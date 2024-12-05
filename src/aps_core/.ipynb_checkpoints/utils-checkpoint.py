
def get_flag_for_gs_pass(current_t,TW,TW_index,GS_list):
    flag = 0 
    for i,v in enumerate(TW):
        if current_t >= v[0] and current_t <= v[1]:
            flag =1
            return [TW_index[i],GS_list[i]]
    if flag ==0 :
        return -1