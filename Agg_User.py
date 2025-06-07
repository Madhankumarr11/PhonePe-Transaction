import os
import json
import pandas as pd

path3 = 'D:/SRMK/Guvi/PhonePe-Transaction/pulse/data/aggregated/user/country/india/state/'
Agg_user_list = os.listdir(path3)

column3 = {'State' : [], 'Year':[], 'Quarter' : [], 'Brand':[], 'Transacion_count':[], 'Percentage':[]} 

for state in Agg_user_list:
    cur_state = path3 + state + '/'
    Agg_year_list = os.listdir(cur_state)

    for year in Agg_year_list:
        cur_year = cur_state + year + '/'
        Agg_file_list = os.listdir(cur_year)

        for file in Agg_file_list:
            cur_file = cur_year + file
            data = open (cur_file, 'r')

            C = json.load(data)

            try:
                for i in C['data']['usersByDevice']:
                    brand = i['brand']
                    count = i['count']
                    percentage = i['percentage']
                    column3['Brand'].append(brand)
                    column3['Transacion_count'].append(count)
                    column3['Percentage'].append(percentage)
                    column3['State'].append(state)
                    column3['Year'].append(year)
                    column3['Quarter'].append(int(file.strip('.json')))
            except:
                pass

                
#Succesfully created a dataframe
Agg_User = pd.DataFrame(column3)

print(Agg_User)

Agg_User['State'] = Agg_User['State'].str.replace('andaman-&-nicobar-islands', 'Andaman & Nicobar')
Agg_User['State'] = Agg_User['State'].str.replace('-', ' ')
Agg_User['State'] = Agg_User['State'].str.title()
Agg_User['State'] = Agg_User['State'].str.replace('dadra-&-nagar-haveli-&-daman-&-diu', 'dadra and nagar haveli and daman and diu')

