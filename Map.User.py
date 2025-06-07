import os
import json
import pandas as pd

path6 = "D:/SRMK/Guvi/PhonePe-Transaction/pulse/data/map/user/hover/country/india/state/"
map_user_list = os.listdir(path6)

column6 = {'State' : [], 'Year':[], 'Quarter' : [], 'District' : [], 'RegisteredUsers': [], 'AppOpens': []}

for state in map_user_list:
    cur_state = path6 + state + '/'
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
                column6['District'].append(district)
                column6['RegisteredUsers'].append(registeredUsers)
                column6['AppOpens'].append(appOpens)
                column6['State'].append(state)
                column6['Year'].append(year)
                column6['Quarter'].append(int(file.strip('.json')))


#Succesfully created a dataframe
Map_user = pd.DataFrame(column6)

print(Map_user)

Map_user['State'] = Map_user['State'].str.replace('andaman-&-nicobar-islands', 'Andaman & Nicobar')
Map_user['State'] = Map_user['State'].str.replace('-', ' ')
Map_user['State'] = Map_user['State'].str.title()
Map_user['State'] = Map_user['State'].str.replace('dadra-&-nagar-haveli-&-daman-&-diu', 'dadra and nagar haveli and daman and diu')


                
        