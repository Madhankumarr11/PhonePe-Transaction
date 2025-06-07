import os
import json
import pandas as pd

path5 = "D:/SRMK/Guvi/PhonePe-Transaction/pulse/data/map/transaction/hover/country/india/state/"
map_trans_list = os.listdir(path5)

column5 = {'State' : [], 'Year':[], 'Quarter' : [], 'District' : [], 'Transaction_count': [], 'Transaction_amount': []}

for state in map_trans_list:
    cur_state = path5 + state + '/'
    map_year_list = os.listdir(cur_state)
    
    for year in map_year_list:
        cur_year = cur_state + year + '/'
        map_file_list = os.listdir(cur_year)

        for file in map_file_list:
            cur_file = cur_year + file
            data = open (cur_file,'r') 

            D = json.load (data)

            for i in D['data']['hoverDataList']:
                Name = i['name']
                Count = i['metric'][0]['count']
                amount = i['metric'][0]['amount']
                column5['District'].append(Name)
                column5['Transaction_count'].append(Count)
                column5['Transaction_amount'].append(amount)
                column5['State'].append(state)
                column5['Year'].append(year)
                column5['Quarter'].append(int(file.strip('.json')))


#Succesfully created a dataframe
Map_transaction = pd.DataFrame(column5)

print(Map_transaction)

Map_transaction['State'] = Map_transaction['State'].str.replace('andaman-&-nicobar-islands', 'Andaman & Nicobar')
Map_transaction['State'] = Map_transaction['State'].str.replace('-', ' ')
Map_transaction['State'] = Map_transaction['State'].str.title()
Map_transaction['State'] = Map_transaction['State'].str.replace('dadra-&-nagar-haveli-&-daman-&-diu', 'dadra and nagar haveli and daman and diu')


                
        