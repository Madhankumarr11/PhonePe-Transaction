import os
import json
import pandas as pd

path8 = 'D:/SRMK/Guvi/PhonePe-Transaction/pulse/data/top/transaction/country/india/state/'
top_transaction_list = os.listdir(path8)

column8 = {'State': [], 'Year': [], 'Quarter': [], 'Pincode': [], 'Transaction_count': [], 'Transaction_amount': []}

for state in top_transaction_list:
    cur_state = path8 + state + '/'
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
                column8['Pincode'].append(entityname)
                column8['Transaction_count'].append(count)
                column8['Transaction_amount'].append(amount)
                column8['State'].append(state)
                column8['Year'].append(year)
                column8['Quarter'].append(int(file.strip('.json')))

#Succesfully created a dataframe
Top_transaction = pd.DataFrame(column8)

print(Top_transaction)

Top_transaction['State'] = Top_transaction['State'].str.replace('andaman-&-nicobar-islands', 'Andaman & Nicobar')
Top_transaction['State'] = Top_transaction['State'].str.replace('-', ' ')
Top_transaction['State'] = Top_transaction['State'].str.title()
Top_transaction['State'] = Top_transaction['State'].str.replace('dadra-&-nagar-haveli-&-daman-&-diu', 'dadra and nagar haveli and daman and diu')



