import os
import json
import pandas as pd
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

connect = psycopg2.connect(
    host = "localhost",
    user = 'postgres',
    database = 'Phonepy_Transaction',
    port = '5432',
    password = '01234'
    
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
               Transacion_Name varchar(255),
               Transacion_count bigint,
               Transacion_amount bigint)''')

connect.commit()
cursor.close()
connect.close()