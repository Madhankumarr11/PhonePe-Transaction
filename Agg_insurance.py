import os
import json
import pandas as pd
import psycopg2

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

path1 = 'D:/SRMK/Guvi/PhonePe-Transaction/pulse/data/aggregated/insurance/country/india/state/'
Agg_insure_list = os.listdir(path1)

column1 = {'State': [], 'Year': [], 'Quarter': [], 'Transaction_Name' : [], 'Transaction_count':[], 'Transaction_amount':[] }

for state in Agg_insure_list:
    cur_state = path1 + state + '/'
    Agg_year_list = os.listdir(cur_state)

    for year in Agg_year_list:
        cur_year = cur_state + year + '/'
        Agg_file_list = os.listdir(cur_year)

        for file in Agg_file_list:
            cur_file = cur_year + file
            data = open (cur_file, 'r')

            A = json.load(data)

            for i in A['data']['transactionData']:
                Name = i['name']
                count = i['paymentInstruments'][0]['count']
                amount = i['paymentInstruments'][0]['amount']
                column1['Transaction_Name'].append(Name)
                column1['Transaction_count'].append(count)
                column1['Transaction_amount'].append(amount)
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


connect = psycopg2.connect(
    host = "localhost",
    user = 'postgres',
    password = '01234',
    port = '5432',
    database = 'phonepy_transaction'
)

connect.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)


#cursor --> Intermediator for python and postgres database
cursor = connect.cursor()

# cursor.execute("CREATE DATABASE Phonepy_Transaction")

# cursor.execute("DROP TABLE IF EXISTS Agg_insurance")


cursor.execute('''create table Agg_insurance 
               (States varchar(255),
               Year int,
               Quarter int,
               Transaction_Name varchar(255),
               Transaction_count bigint,
               Transaction_amount bigint)''')

insert_query ='''insert into Agg_insurance (States, Year, Quarter, Transaction_Name, Transaction_count, Transaction_amount)
values (%s, %s, %s, %s, %s, %s)'''

data = Agg_insurance.values.tolist()

cursor.executemany(insert_query, data)

connect.commit()
cursor.close()
connect.close()