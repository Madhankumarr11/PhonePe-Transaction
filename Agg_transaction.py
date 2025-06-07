import os
import json
import pandas as pd

#Once created the clone of GIT-HUB repository then,
#Required libraries for the program

#This is to direct the path to get the data as states

path2 = "D:/SRMK/Guvi/PhonePe-Transaction/pulse/data/aggregated/transaction/country/india/state/"
Agg_trans_list = os.listdir(path2)

# #Agg_state_list--> to get the list of states in India

# #<------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------>#

# #This is to extract the data's to create a dataframe

column2 = {'State' : [], 'Year':[], 'Quarter' : [], 'Transacion_type':[], 'Transacion_count':[], 'Transacion_amount':[]} 

for state in Agg_trans_list:
    cur_state = path2 + state + '/'
    Agg_year_list = os.listdir(cur_state)
    
    for year in Agg_year_list:
        cur_year = cur_state + year + '/'
        Agg_file_list = os.listdir(cur_year)

        for file in Agg_file_list:
            cur_file = cur_year + file
            data = open (cur_file,'r')     # Open to read mode
            # read = data.read()           # read issue clear code
            
            A = json.load(data)             #loads JSON content into a Python dict

            for i in A['data']['transactionData']:
                Name = i['name']
                count = i['paymentInstruments'][0]['count']
                amount = i['paymentInstruments'][0]['amount']
                column2['Transacion_type'].append(Name)
                column2['Transacion_count'].append(count)
                column2['Transacion_amount'].append(amount)
                column2['State'].append(state)
                column2['Year'].append(year)
                column2['Quarter'].append(int(file.strip('.json')))

                
#Succesfully created a dataframe
Agg_Transaction = pd.DataFrame(column2)

print(Agg_Transaction)

Agg_Transaction['State'] = Agg_Transaction['State'].str.replace('andaman-&-nicobar-islands', 'Andaman & Nicobar')
Agg_Transaction['State'] = Agg_Transaction['State'].str.replace('-', ' ')
Agg_Transaction['State'] = Agg_Transaction['State'].str.title()
Agg_Transaction['State'] = Agg_Transaction['State'].str.replace('dadra-&-nagar-haveli-&-daman-&-diu', 'dadra and nagar haveli and daman and diu')