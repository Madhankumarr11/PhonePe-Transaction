import os
import json
import pandas as pd
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

connect = psycopg2.connect(
     host = "localhost",
     user = 'postgres',
     port = '5432',
     password = '01234',
     database = 'phonepy_transaction'
)

connect.set_isolation_level (ISOLATION_LEVEL_AUTOCOMMIT)

cursor = connect.cursor()

#Agg_insurance

cursor.execute ("select * from agg_insurance")
connect.commit()
tabel1 = cursor.fetchall()

Agg_insurance = pd.DataFrame(tabel1, columns=('State', 'Year', 'Quarter', 'Transaction_Name', 'Transaction_count', 'Transaction_amount'))


# Agg_transaction

cursor.execute ("select * from agg_transaction")
connect.commit()
tabel2 = cursor.fetchall()

Agg_Transaction = pd.DataFrame(tabel2, columns=('State', 'Year', 'Quarter', 'Transaction_type', 'Transaction_count', 'Transaction_amount'))


# Agg_User

cursor.execute ("select * from agg_user")
connect.commit()
tabel3 = cursor.fetchall()

Agg_User = pd.DataFrame(tabel3, columns=('State', 'Year', 'Quarter', 'Brand', 'Transaction_count', 'Percentage'))


# Map_insurance

cursor.execute ("select * from map_insurance")
connect.commit()
tabel4 = cursor.fetchall()

Map_insurance = pd.DataFrame(tabel4, columns=('State', 'Year', 'Quarter', 'District', 'Transaction_count', 'Transaction_amount'))


# Map_transaction

cursor.execute ("select * from map_transaction")
connect.commit()
tabel5 = cursor.fetchall()

Map_Transaction = pd.DataFrame(tabel5, columns=('State', 'Year', 'Quarter', 'District', 'Transaction_count', 'Transaction_amount'))


# Map_User

cursor.execute ("select * from map_user")
connect.commit()
tabel6 = cursor.fetchall()

Map_User = pd.DataFrame(tabel6, columns=('State', 'Year', 'Quarter', 'District', 'RegisteredUsers', 'AppOpens'))


# Top_insurance

cursor.execute ("select * from top_insurance")
connect.commit()
tabel7 = cursor.fetchall()

Top_insurance = pd.DataFrame(tabel7, columns=('State', 'Year', 'Quarter', 'Pincode', 'Transaction_count', 'Transaction_amount'))


#Top_Transaction

cursor.execute('select * from top_transaction')
connect.commit()
tabel8 = cursor.fetchall()

Top_Transaction = pd.DataFrame(tabel8, columns=('State', 'Year', 'Quarter', 'Pincode', 'Transaction_count', 'Transaction_amount'))


#Top_User

cursor.execute('select * from top_user')
connect.commit()
tabel9 = cursor.fetchall()

Top_User = pd.DataFrame(tabel9, columns=('State', 'Year', 'Quarter', 'Pincode', 'RegisteredUsers'))
