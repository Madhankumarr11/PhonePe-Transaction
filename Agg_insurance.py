import os
import json
import pandas as pd

path3 = 'D:/SRMK/Guvi/PhonePe-Transaction/pulse/data/aggregated/insurance/country/india/state/'
Agg_insure_list = os.listdir(path3)

column3 = {'State': [], 'Year': [], 'Quarter': [], 'Transacion_Name' : [], 'Transacion_count':[], 'Transacion_amount':[] }

for state in Agg_insure_list:
    cur_state = path3 + state + '/'
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
                column3['Transacion_Name'].append(Name)
                column3['Transacion_count'].append(count)
                column3['Transacion_amount'].append(amount)
                column3['State'].append(file)
                column3['Year'].append(year)
                column3['Quarter'].append(int(file.strip('.json')))

                
#Succesfully created a dataframe
Agg_insurance = pd.DataFrame(column3)

print(Agg_insurance)
