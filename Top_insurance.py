import os
import json
import pandas as pd
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

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

            G = json.load(data)

            for i in G['data']['pincodes']:
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
Top_insurance = pd.DataFrame(column7)

print(Top_insurance)

Top_insurance['State'] = Top_insurance['State'].str.replace('andaman-&-nicobar-islands', 'Andaman & Nicobar')
Top_insurance['State'] = Top_insurance['State'].str.replace('-', ' ')
Top_insurance['State'] = Top_insurance['State'].str.title()
Top_insurance['State'] = Top_insurance['State'].str.replace('dadra-&-nagar-haveli-&-daman-&-diu', 'dadra and nagar haveli and daman and diu')

connect = psycopg2.connect(
     host = "localhost",
     user = 'postgres',
     port = '5432',
     password = '01234',
     database = 'phonepy_transaction'
)

connect.set_isolation_level (ISOLATION_LEVEL_AUTOCOMMIT)

cursor = connect.cursor()

cursor.execute ('''create table Top_insurance
                (State varchar(255), 
                Year int, 
                Quarter int, 
                Pincode bigint, 
                Transaction_count bigint,
                Transaction_amount bigint)''')

insert_query7 = '''insert into Top_insurance(State, Year, Quarter, Pincode, Transaction_count, Transaction_amount)
values (%s, %s, %s, %s, %s, %s)'''

data = Top_insurance.values.tolist()

cursor.executemany(insert_query7, data)

connect.commit()
cursor.close()
connect.close()