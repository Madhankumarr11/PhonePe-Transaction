import os
import json
import pandas as pd

path4 = "D:/SRMK/Guvi/PhonePe-Transaction/pulse/data/map/insurance/hover/country/india/state/"
map_insurance_list = os.listdir(path4)

column4 ={'State': [], 'Year': [], 'Quarter': [],'District': [], 'Transaction_count': [], 'Transaction_amount': []}

for state in map_insurance_list:
    cur_state = path4 + state + '/'
    map_year_list = os.listdir(cur_state)

    for year in map_year_list:
        cur_year = cur_state + year + '/'
        map_file_list = os.listdir(cur_year)

        for file in map_file_list:
            cur_file = cur_year + file
            data = open(cur_file,'r')

            D = json.load(data)

            for i in D['data']['hoverDataList']:
                name = i['name']
                count = i['metric'][0]['count']
                amount = i['metric'][0]['amount']
                column4['District'].append(name)
                column4['Transaction_count'].append(count)
                column4['Transaction_amount'].append(amount)
                column4['State'].append(state)
                column4['Year'].append(year)
                column4['Quarter'].append(int(file.strip('.json')))


#Succesfully created a dataframe
Map_insurance = pd.DataFrame(column4)

print(Map_insurance)

Map_insurance['State'] = Map_insurance['State'].str.replace('andaman-&-nicobar-islands', 'Andaman & Nicobar')
Map_insurance['State'] = Map_insurance['State'].str.replace('-', ' ')
Map_insurance['State'] = Map_insurance['State'].str.title()
Map_insurance['State'] = Map_insurance['State'].str.replace('dadra-&-nagar-haveli-&-daman-&-diu', 'dadra and nagar haveli and daman and diu')

