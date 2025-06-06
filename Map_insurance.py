import os
import json
import pandas as pd

path6 = "D:/SRMK/Guvi/PhonePe-Transaction/pulse/data/map/insurance/hover/country/india/state/"
map_insurance_list = os.listdir(path6)

column6 ={'State': [], 'Year': [], 'Quarter': [],'District': [], 'Transaction_count': [], 'Transaction_amount': []}

for state in map_insurance_list:
    cur_state = path6 + state + '/'
    map_year_list = os.listdir(cur_state)

    for year in map_year_list:
        cur_year = cur_state + year + '/'
        map_file_list = os.listdir(cur_year)

        for file in map_file_list:
            cur_file = cur_year + file
            data = open(cur_file,'r')

            F = json.load(data)

            for i in F['data']['hoverDataList']:
                name = i['name']
                count = i['metric'][0]['count']
                amount = i['metric'][0]['amount']
                column6['District'].append(name)
                column6['Transaction_count'].append(count)
                column6['Transaction_amount'].append(amount)
                column6['State'].append(state)
                column6['Year'].append(year)
                column6['Quarter'].append(int(file.strip('.json')))


#Succesfully created a dataframe
Map_insurance = pd.DataFrame(column6)

print(Map_insurance)
