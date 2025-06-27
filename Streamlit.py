import os
import json
import pandas as pd
import psycopg2
import streamlit as st
from streamlit_option_menu import option_menu
import matplotlib.pyplot as plt
import plotly.express as px
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

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



def Transaction_amount_count_Y(df, year):


    #(Boolean Series)
    tacy = df[df['Year'] == year]
    tacy.reset_index(drop = True, inplace= True)

    tacyg = tacy.groupby('State')[['Transaction_count', 'Transaction_amount']].sum()
    tacyg.reset_index(inplace=True)

    col1, col2 =st.columns(2)

    with col1:

        fig_amount = px.bar(tacyg, x='State', y='Transaction_amount', title =f'{year} Transaction Amount', 
                            color_discrete_sequence= px.colors.sequential.Aggrnyl,height=650, width=600)
        st.plotly_chart(fig_amount)

    with col2:    

        fig_count = px.bar(tacyg,x = 'State', y='Transaction_count', title =f'{year} Transaction Count',
                        color_discrete_sequence= px.colors.sequential.Peach_r, height=650, width=600)
        st.plotly_chart(fig_count)


# Streamlit Part

# Title wide command
st.set_page_config (layout= 'wide')
st.title('PhonePe - Insurance Transaction Insights')

with st.sidebar: 
    select = option_menu ('Main Menu', ["Home", "DATA EXPLORATION", "TOP CHARTS"])

# Home Menu Details

if select == 'Home':
    pass

elif select == "DATA EXPLORATION":
    
    tab1, tab2, tab3 = st.tabs (['Aggregated Analysis', 'Map Analysis', 'Top Analysis'])

    with tab1 :

        method_1 = st.radio("Select The Method", ["Aggregated Insurance", "Aggregated Transaction", "Aggregated User"])

        if method_1 == 'Aggregated Insurance':

            col1, col2 = st.columns(2)
            with col1:

                    years = st.slider('Select The Year', Agg_insurance["Year"].min(), Agg_insurance["Year"].max(), Agg_insurance["Year"].min())
            Transaction_amount_count_Y(Agg_insurance, years)

        elif method_1 == "Aggregated Transaction":
            pass

        elif method_1 == "Aggregated User":
            pass
    

    with tab2 :
        method_2 = st.radio('Select The Method', ['Map Insurance', 'Map Transaction', "Map User"])

        if method_2 == "Map Insurance":
            pass

        elif method_2 == "Map Transaction":
            pass

        elif method_2 == "Map User":
            pass

    with tab3 :

        method_3 = st.radio('select The Method', ["Top Insurance", "Top Transaction", "Top User"])

        if method_3 == "Top Insurance":
            pass
        
        elif method_3 == "Top Transaction":
            pass

        elif method_3 == "Top User":
            pass


elif select == "TOP CHARTS":
    pass

