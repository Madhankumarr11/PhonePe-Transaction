import pandas as pd
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

connect = psycopg2.connect(
    host = "localhost",
    user = 'postgres',
    password = '01234',
    port = '5432',
    database = 'phonepy_transaction'
)

cursor = connect.cursor()

connect.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

#Agg_Insurance_DF

cursor.execute('select * from agg_insurance')

table1 = cursor.fetchall()

Agg_insurance = pd.DataFrame(table1, columns = ('State', 'Year', 'Quarter', 'Transaction_Name', 'Transaction_count', 'Transaction_amount') )

print(Agg_insurance)


#Agg_transaction_DF

cursor.execute('select * from agg_transaction')

table2 = cursor.fetchall()

Agg_transaction = pd.DataFrame(table2, columns = ('State', 'Year', 'Quarter', 'Transaction_type', 'Transaction_count', 'Transaction_amount') )

print(Agg_transaction)


#Agg_User_DF

cursor.execute('select * from agg_user')

table3 = cursor.fetchall()

Agg_User = pd.DataFrame(table3, columns = ('State', 'Year', 'Quarter', 'Brand', 'Transaction_count', 'Percentage'))

print(Agg_User)


#Map_Insurance_DF

cursor.execute('select * from map_insurance')

table4 = cursor.fetchall()

Map_insurance = pd.DataFrame(table4, columns = ('State', 'Year', 'Quarter', 'District', 'Transaction_count', 'Transaction_amount'))

print(Map_insurance)



#Map_transaction_DF

cursor.execute('select * from map_transaction')

table5 = cursor.fetchall()

Map_transaction = pd.DataFrame(table5, columns = ('State', 'Year', 'Quarter', 'District', 'Transaction_count', 'Transaction_amount'))

print(Map_transaction)


#Map_User_DF

cursor.execute('select * from map_user')

table6 = cursor.fetchall()

Map_User = pd.DataFrame(table6, columns = ('State', 'Year', 'Quarter', 'District', 'RegisteredUsers', 'AppOpens'))

print(Map_User)



#Top_Insurance_DF

cursor.execute('select * from top_insurance')

table7 = cursor.fetchall()

Top_insurance = pd.DataFrame(table7, columns = ('State', 'Year', 'Quarter', 'Pincode', 'Transaction_count', 'Transaction_amount'))

print(Top_insurance)


#Top_Transaction_DF

cursor.execute('select * from top_transaction')

table8 = cursor.fetchall()

Top_transaction = pd.DataFrame(table8, columns = ('State', 'Year', 'Quarter', 'Pincode', 'Transaction_count', 'Transaction_amount'))

print(Top_transaction)


#Top_User_DF

cursor.execute('select * from top_user')

table9 = cursor.fetchall()

Top_user = pd.DataFrame(table9, columns = ('State', 'Year', 'Quarter', 'Pincode', 'RegisteredUsers'))

print(Top_user)
