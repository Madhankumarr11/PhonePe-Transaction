import os
import json
import pandas as pd
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

path9 = 'D:/SRMK/Guvi/PhonePe-Transaction/pulse/data/top/user/country/india/state/'
top_user_list = os.listdir(path9)

column9 = {'State': [], 'Year': [], 'Quarter': [], 'Pincode': [], 'RegisteredUsers': []}

for state in top_user_list:
    cur_state = path9 + state + '/'
    top_year_list = os.listdir(cur_state)

    for year in top_year_list:
        cur_year = cur_state + year + '/'
        top_file_list = os.listdir(cur_year)

        for file in top_file_list:
            cur_file = cur_year + file
            data = open(cur_file,'r')

            I = json.load(data)

            for i in I['data']['pincodes']:
                entityname = i['name']
                registeredusers = i['registeredUsers']
                column9['Pincode'].append(entityname)
                column9['RegisteredUsers'].append(registeredusers)
                column9['State'].append(state)
                column9['Year'].append(year)
                column9['Quarter'].append(int(file.strip('.json')))

#Succesfully created a dataframe
Top_User = pd.DataFrame(column9)

print(Top_User)

Top_User['State'] = Top_User['State'].str.replace('andaman-&-nicobar-islands', 'Andaman & Nicobar')
Top_User['State'] = Top_User['State'].str.replace('-', ' ')
Top_User['State'] = Top_User['State'].str.title()
Top_User['State'] = Top_User['State'].str.replace('dadra-&-nagar-haveli-&-daman-&-diu', 'dadra and nagar haveli and daman and diu')



connect = psycopg2.connect(
     host = "localhost",
     user = 'postgres',
     port = '5432',
     password = '01234',
     database = 'phonepy_transaction'
)

connect.set_isolation_level (ISOLATION_LEVEL_AUTOCOMMIT)

cursor = connect.cursor()

cursor.execute ('''create table Top_User
                (State varchar(255), 
                Year int, 
                Quarter int, 
                Pincode bigint, 
                RegisteredUsers bigint)''')

insert_query9 = '''insert into Top_User(State, Year, Quarter, Pincode, RegisteredUsers)
values (%s, %s, %s, %s, %s)'''

data = Top_User.values.tolist()

cursor.executemany(insert_query9, data)

connect.commit()
cursor.close()
connect.close()