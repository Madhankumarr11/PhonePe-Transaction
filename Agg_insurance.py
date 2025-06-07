import os
import json
import pandas as pd

path1 = 'D:/SRMK/Guvi/PhonePe-Transaction/pulse/data/aggregated/insurance/country/india/state/'
Agg_insure_list = os.listdir(path1)

column1 = {'State': [], 'Year': [], 'Quarter': [], 'Transacion_Name' : [], 'Transacion_count':[], 'Transacion_amount':[] }

for state in Agg_insure_list:
    cur_state = path1 + state + '/'
    Agg_year_list = os.listdir(cur_state)

    for year in Agg_year_list:
        cur_year = cur_state + year + '/'
        Agg_file_list = os.listdir(cur_year)

        for file in Agg_file_list:
            cur_file = cur_year + file
            data = open (cur_file, 'r')

            C = json.load(data)

            for i in C['data']['transactionData']:
                Name = i['name']
                count = i['paymentInstruments'][0]['count']
                amount = i['paymentInstruments'][0]['amount']
                column1['Transacion_Name'].append(Name)
                column1['Transacion_count'].append(count)
                column1['Transacion_amount'].append(amount)
                column1['State'].append(state)
                column1['Year'].append(year)
                column1['Quarter'].append(int(file.strip('.json')))

                
#Succesfully created a dataframe
Agg_insurance = pd.DataFrame(column1)

print(Agg_insurance)

Agg_insurance['State'] = Agg_insurance['State'].str.replace('andaman-&-nicobar-islands', 'Andaman & Nicobar')
Agg_insurance['State'] = Agg_insurance['State'].str.replace('-', ' ')
Agg_insurance['State'] = Agg_insurance['State'].str.title()
Agg_insurance['State'] = Agg_insurance['State'].str.replace('dadra-&-nagar-haveli-&-daman-&-diu', 'dadra and nagar haveli and daman and diu')
