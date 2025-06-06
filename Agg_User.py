import os
import json
import pandas as pd

path2 = 'D:/SRMK/Guvi/PhonePe-Transaction/pulse/data/aggregated/user/country/india/state/'
Agg_user_list = os.listdir(path2)

column2 = {'State' : [], 'Year':[], 'Quarter' : [], 'Brand':[], 'Transacion_count':[], 'Percentage':[]} 

for state in Agg_user_list:
    cur_state = path2 + state + '/'
    Agg_year_list = os.listdir(cur_state)

    for year in Agg_year_list:
        cur_year = cur_state + year + '/'
        Agg_file_list = os.listdir(cur_year)

        for file in Agg_file_list:
            cur_file = cur_year + file
            data = open (cur_file, 'r')

            B = json.load(data)

            try:
                for i in B['data']['usersByDevice']:
                    brand = i['brand']
                    count = i['count']
                    percentage = i['percentage']
                    column2['Brand'].append(brand)
                    column2['Transacion_count'].append(count)
                    column2['Percentage'].append(percentage)
                    column2['State'].append(state)
                    column2['Year'].append(year)
                    column2['Quarter'].append(int(file.strip('.json')))
            except:
                pass

                
#Succesfully created a dataframe
Agg_User = pd.DataFrame(column2)

print(Agg_User)



