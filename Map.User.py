import os
import json
import pandas as pd

path5 = "D:/SRMK/Guvi/PhonePe-Transaction/pulse/data/map/user/hover/country/india/state/"
map_user_list = os.listdir(path5)

# column5 = {'State' : [], 'Year':[], 'Quarter' : [], 'District' : [], 'Transaction_count': [], 'Transaction_amount': []}

for state in map_user_list:
    cur_state = path5 + state + '/'
    Agg_year_list = os.listdir(cur_state)
    
    for year in Agg_year_list:
        cur_year = cur_state + year + '/'
        Agg_file_list = os.listdir(cur_year)

        for file in Agg_file_list:
            cur_file = cur_year + file
            data = open (cur_file,'r') 

            E = json.load (data)

#             for i in D['data']['hoverDataList']:
#                 Name = i['name']
#                 Count = i['metric'][0]['count']
#                 amount = i['metric'][0]['amount']
#                 column4['District'].append(Name)
#                 column4['Transaction_count'].append(Count)
#                 column4['Transaction_amount'].append(amount)
#                 column4['State'].append(file)
#                 column4['Year'].append(year)
#                 column4['Quarter'].append(int(file.strip('.json')))


# #Succesfully created a dataframe
# Map_transaction = pd.DataFrame(column4)

# print(Map_transaction)


                
        