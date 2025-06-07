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

cursor.execute ('''create table Agg_User
                (State varchar(255), 
                Year int, 
                Quarter int, 
                Brand varchar(255), 
                Transaction_count bigint,
                Percentage bigint)''')

insert_query3 = '''inser into Agg_User(State, Year, Quarter, Brand, Transaction_count, Percentage)
values = (%s, %s, %s, %s, %s, %s)'''

data = Agg_User.values.tolist()

cursor.executemany(insert_query3, data)

connect.commit()
cursor.close()
connect.close()
