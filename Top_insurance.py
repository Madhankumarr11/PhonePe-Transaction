import os
import json
import pandas as pd

path7 = "D:/SRMK/Guvi/PhonePe-Transaction/pulse/data/top/insurance/country/india/state/"
top_insurance_list = os.listdir(path7)

column7 = {'State': [], 'Year': [], 'Quarter': [], 'Pincode': [], 'Transaction_count' : [], 'Transaction_amount' : []}

for state in top_insurance_list:
    cur_state = path7 + state + '/'
    top_year_list = os.listdir(cur_state)

    for year in top_year_list:
        cur_year = cur_state + year + '/'
        top_file_list = os.listdir(cur_year)

        for file in top_file_list:
            cur_file = cur_year + file
            data = open(cur_file,'r')

            H = json.load(data)

            for i in H['data']['pincodes']:
                entityname = i['entityName']
                count = i['metric']['count']
                amount = i['metric']['amount']
                column7['Pincode'].append(entityname)
                column7['Transaction_count'].append(count)
                column7['Transaction_amount'].append(amount)
                column7['State'].append(state)
                column7['Year'].append(year)
                column7['Quarter'].append(int(file.strip('.json')))

#Succesfully created a dataframe
Top_transaction = pd.DataFrame(column7)

print(Top_transaction)

Top_transaction['State'] = Top_transaction['State'].str.replace('andaman-&-nicobar-islands', 'Andaman & Nicobar')
Top_transaction['State'] = Top_transaction['State'].str.replace('-', ' ')
Top_transaction['State'] = Top_transaction['State'].str.title()
Top_transaction['State'] = Top_transaction['State'].str.replace('dadra-&-nagar-haveli-&-daman-&-diu', 'dadra and nagar haveli and daman and diu')

