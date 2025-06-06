import os
import json
import pandas as pd

path5 = "D:/SRMK/Guvi/PhonePe-Transaction/pulse/data/map/user/hover/country/india/state/"
map_user_list = os.listdir(path5)

column5 = {'State' : [], 'Year':[], 'Quarter' : [], 'District' : [], 'RegisteredUsers': [], 'AppOpens': []}

for state in map_user_list:
    cur_state = path5 + state + '/'
    map_year_list = os.listdir(cur_state)
    
    for year in map_year_list:
        cur_year = cur_state + year + '/'
        map_file_list = os.listdir(cur_year)

        for file in map_file_list:
            cur_file = cur_year + file
            data = open (cur_file,'r') 

            E = json.load (data)

            for i in E['data']['hoverData'].items():
                district = i[0]
                registeredUsers = i[1]['registeredUsers']
                appOpens = i[1]['appOpens']
                column5['District'].append(district)
                column5['RegisteredUsers'].append(registeredUsers)
                column5['AppOpens'].append(appOpens)
                column5['State'].append(state)
                column5['Year'].append(year)
                column5['Quarter'].append(int(file.strip('.json')))


#Succesfully created a dataframe
Map_user = pd.DataFrame(column5)

print(Map_user)


                
        