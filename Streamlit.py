import os
import json
import pandas as pd
import psycopg2
import streamlit as st
from streamlit_option_menu import option_menu
import matplotlib.pyplot as plt
import plotly.express as px
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import requests

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


#======================================================================================================================================================================

# Aggregated_Transaction_Year

def Transaction_amount_count_Y(df, year):


    #(Boolean Series)
    tacy = df[df['Year'] == year]
    tacy.reset_index(drop = True, inplace= True)

    tacyg = tacy.groupby('State')[['Transaction_count', 'Transaction_amount']].sum()
    tacyg.reset_index(inplace=True)

    col1, col2 =st.columns(2)

    with col1:

        fig_Amount = px.bar(tacyg, x='State', y='Transaction_amount', title =f'{year} Transaction Amount', 
                            color_discrete_sequence= px.colors.sequential.Aggrnyl, height=650, width=600)
        st.plotly_chart(fig_Amount)

    with col2:    

        fig_Count = px.bar(tacyg,x = 'State', y='Transaction_count', title =f'{year} Transaction Count',
                        color_discrete_sequence= px.colors.sequential.Bluered_r, height=650, width=600)
        st.plotly_chart(fig_Count)

    col1, col2= st.columns(2)
    with col1:

        url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response = requests.get(url)

        data1 = json.loads(response.content)

        states_name = []
        for feature in data1["features"]:
            states_name.append(feature['properties']['ST_NM'])

        states_name.sort()

        fig_india_1 = px.choropleth(tacyg, geojson= data1, locations='State', featureidkey= 'properties.ST_NM',  
                                    color= 'Transaction_amount', color_continuous_scale= 'Rainbow', 
                                    range_color= (tacyg ['Transaction_amount'].min(), tacyg ['Transaction_amount'].max()), 
                                    hover_name= 'State', title= f"{year} TRANSACTION AMOUNT",  fitbounds="locations",
                                    height=600, width=600 )
        
        fig_india_1.update_geos(visible= False)
        st.plotly_chart(fig_india_1)

      
    with col2:
    
        fig_india_2 = px.choropleth(tacyg, geojson= data1, locations='State', featureidkey= 'properties.ST_NM',  
                                    color= 'Transaction_count', color_continuous_scale= 'Rainbow', 
                                    range_color= (tacyg ['Transaction_count'].min(), tacyg ['Transaction_count'].max()), 
                                    hover_name= 'State', title= f"{year} TRANSACTION COUNT",  fitbounds="locations",
                                    height=600, width=600 )
        
        fig_india_2.update_geos(visible= False)
        st.plotly_chart(fig_india_2) 

    return tacy

#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------


# Aggregated_Transaction_Quarter

def Transaction_amount_count_Y_Q(df, quarter):
    #(Boolean Series)
    tacy = df[df['Quarter'] == quarter]
    tacy.reset_index(drop = True, inplace= True)

    tacyg = tacy.groupby('State')[['Transaction_count', 'Transaction_amount']].sum()
    tacyg.reset_index(inplace=True)

    col1,col2 = st.columns(2)
    with col1: 

        fig_amount = px.bar(tacyg, x='State', y='Transaction_amount', title =f"{tacy['Year'].min()} YEAR {quarter} QUARTER Transaction Amount", 
                            color_discrete_sequence= px.colors.sequential.Aggrnyl, height=650, width=600)
        st.plotly_chart(fig_amount)

    with col2:    
        fig_count = px.bar(tacyg,x = 'State', y='Transaction_count', title =f"{tacy['Year'].min()} YEAR {quarter} QUARTER Transaction Count",
                        color_discrete_sequence= px.colors.sequential.Aggrnyl, height=650, width=600)
        st.plotly_chart(fig_count)

    col1, col2 = st.columns(2)
    with col1:   

        url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response = requests.get(url)

        data1 = json.loads(response.content)

        states_name = []

        for feature in data1["features"]:
            print(feature['properties'])
            states_name.append(feature['properties']['ST_NM'])

        states_name.sort()

        fig_india_1 = px.choropleth(tacyg, geojson=data1, locations='State', featureidkey= 'properties.ST_NM',  
                                    color= 'Transaction_amount', color_continuous_scale= 'Rainbow', 
                                    range_color= (tacyg ['Transaction_amount'].min(), tacyg ['Transaction_amount'].max()), 
                                    hover_name= 'State', title= f"{tacy['Year'].min()} YEAR {quarter} QUARTER TRANSACTION AMOUNT",  fitbounds="locations",
                                    height=600, width=600 )
        
        fig_india_1.update_geos(visible= False)
        st.plotly_chart(fig_india_1)

    with col2:    
    
        fig_india_2 = px.choropleth(tacyg, geojson=data1, locations='State', featureidkey= 'properties.ST_NM',  
                                    color= 'Transaction_count', color_continuous_scale= 'Rainbow', 
                                    range_color= (tacyg ['Transaction_count'].min(), tacyg ['Transaction_count'].max()), 
                                    hover_name= 'State', title= f"{tacy['Year'].min()} YEAR {quarter} QUARTER TRANSACTION COUNT",  fitbounds="locations",
                                    height=600, width=600 )
        
        fig_india_2.update_geos(visible= False)
        st.plotly_chart(fig_india_2)

    return tacy

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Aggregated_Transaction_State

def Agg_trans_Transaction_type(df,state):

    tacy = df[df['State'] == state]
    tacy.reset_index(drop = True, inplace= True)

    tacyg = tacy.groupby('Transaction_type')[['Transaction_count', 'Transaction_amount']].sum()
    tacyg.reset_index(inplace=True)

    col1, col2 = st.columns(2)
    with col1:
        
        fig_pie_1 = px.pie(data_frame= tacyg, names= "Transaction_type", values= "Transaction_amount", 
                   width= 600, title= f"{state.upper()} TRANSATION_AMOUNT", hole= 0.5)
        st.plotly_chart(fig_pie_1)

    with col2:    

        fig_pie_2 = px.pie(data_frame= tacyg, names= "Transaction_type", values= "Transaction_count", 
                    width= 600, title= f"{state.upper()} TRANSATION_COUNT", hole= 0.5)
        st.plotly_chart(fig_pie_2)

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Aggregated_User_analysis_1

def Agg_User_plot_1(df, year):
    aguy = df[df["Year"] == year]
    aguy.reset_index(drop = True, inplace= True)

    aguyg = pd.DataFrame (aguy.groupby("Brand")["Transaction_count"].sum())
    aguyg.reset_index(inplace= True)


    fig_bar_1 = px.bar(aguyg, x= "Brand", y ="Transaction_count", title= f"{year} BRANDS and TRANSACTION_COUNT",
                    width=1000, color_discrete_sequence= px.colors.sequential.haline_r, hover_name= "Brand")
    st.plotly_chart (fig_bar_1)

    return aguy


#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------


#Aggre_User_Analysis_2

def Agg_User_plot_2(df, quarter):

    aguyq = df[df["Quarter"] == quarter]
    aguyq.reset_index(drop = True, inplace= True)

    aguyqg = pd.DataFrame(aguyq.groupby("Brand")["Transaction_count"].sum())
    aguyqg.reset_index(inplace=True)

    fig_bar_2 = px.bar(aguyqg, x= "Brand", y ="Transaction_count", title= f"{quarter} QUARTER BRANDS and TRANSACTION_COUNT",
                        width=1000, color_discrete_sequence= px.colors.sequential.haline_r, hover_name= "Brand")
    st.plotly_chart (fig_bar_2)

    return aguyq


#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


#Aggre_User_Analysis_3

def Agg_User_plot_3 (df, state):
    auyqs = df[df["State"] == state]
    auyqs.reset_index (drop = True, inplace= True)

    fig_line_3 = px.line(auyqs, x = "Brand", y = "Transaction_count", hover_data= "Percentage",
                        title= f"{state.upper()} BRAND, TRANSACTION_COUNT, PERENTAGE", width=1000, markers= 'd')

    st.plotly_chart(fig_line_3)



#---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#Map_Insurance_District

def Map_insur_District(df,state):

    tacy = df[df['State'] == state]
    tacy.reset_index(drop = True, inplace= True)

    tacyg = tacy.groupby('District')[['Transaction_count', 'Transaction_amount']].sum()
    tacyg.reset_index(inplace=True)

    col1, col2= st.columns(2)

    with col1:
        fig_bar_1 = px.bar(tacyg, x= "Transaction_amount", y= "District", orientation= "h", height=600,
                        title= f"{state.upper()} DISTRICT AND TRANSACTION AMOUNT", color_discrete_sequence= px.colors.sequential.Mint_r)
        st.plotly_chart(fig_bar_1)

    with col2:    
        fig_bar_2 = px.bar(tacyg, x= "Transaction_count", y= "District", orientation= "h", height=600,
                        title= f"{state.upper()} DISTRICT AND TRANSACTION COUNT", color_discrete_sequence= px.colors.sequential.Bluered_r)
        st.plotly_chart(fig_bar_2)

#---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#Map_User_plot_1

def Map_User_plot_1(df, year):
    muy = df[df["Year"] == year]
    muy.reset_index(drop = True, inplace= True)

    muyg = muy.groupby("State") [["RegisteredUsers", "AppOpens"]].sum()
    muyg.reset_index(inplace=True)

    fig_line_1 = px.line(muyg, x="State", y=["RegisteredUsers", "AppOpens"], 
                        color_discrete_map={"RegisteredUsers": "#65AE82", "AppOpens":"#ff7f0e"},
                        title=f"{year} REGISTERED USERS (vs) APP OPENS", width=1000, height=800, markers=True) 
    st.plotly_chart(fig_line_1)

    return muy

#---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#Map_User_plot_2

def Map_User_plot_2(df, quarter):
    muyq = df[df["Quarter"] == quarter]
    muyq.reset_index(drop = True, inplace= True)

    muyqg = muyq.groupby("State") [["RegisteredUsers", "AppOpens"]].sum()
    muyqg.reset_index(inplace=True)


    fig_line_1 = px.line(muyqg, x="State", y=["RegisteredUsers", "AppOpens"], 
                        color_discrete_map={"RegisteredUsers": "#12229E", "AppOpens":"#ff7f0e"},
                        title=f"{df['Year'].min()} YEARS {quarter} QUARTER REGISTERED USERS (vs) APP OPENS", width=1000, height=800, markers=True) 
    st.plotly_chart(fig_line_1)

    return muyq

#---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#Map_User_plot_3

def Map_User_plot_3(df, state):
    muyqs = df[df["State"] == state]
    muyqs.reset_index(drop = True, inplace= True)

    col1, col2 = st.columns(2)

    with col1:
        fig_map_user_bar_1 = px.bar(muyqs, x = "RegisteredUsers", y = "District", orientation="h", 
                                    title= f"{state.upper()} REGISTERED USERS", height= 800, color_discrete_sequence= px.colors.sequential.Rainbow_r)

        st.plotly_chart(fig_map_user_bar_1)

    with col2:
        fig_map_user_bar_2 = px.bar(muyqs, x = "AppOpens", y = "District", orientation="h", 
                                    title= f"{state.upper()} APPOPENS", height= 800, color_discrete_sequence=px.colors.sequential.Rainbow)

        st.plotly_chart(fig_map_user_bar_2)

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Top_Insurance_plot_1

def Top_insurance_plot_1(df, state):
    
    tiy = df[df["State"] == state]
    tiy.reset_index(drop = True, inplace= True)

    col1, col2 = st.columns(2)
    with col1:
        fig_top_insur_bar_1 = px.bar(tiy, x = "Quarter", y = "Transaction_amount", hover_data="Pincode", #orientation="h",
                                        title= "TRANSACTION AMOUNT", height= 650, width=600, color_discrete_sequence= px.colors.sequential.Darkmint_r)

        st.plotly_chart(fig_top_insur_bar_1)

    with col2:
        fig_top_insur_bar_2 = px.bar(tiy, x = "Quarter", y = "Transaction_amount", hover_data="Pincode", #orientation="h",
                                        title= "TRANSACTION COUNT", height= 650, width=600, color_discrete_sequence= px.colors.sequential.Agsunset_r)
        st.plotly_chart(fig_top_insur_bar_2)


#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Top_User_plot_1

def Top_User_plot_1(df, year):
    tuy = df[df["Year"] == year]
    tuy.reset_index(drop = True, inplace= True)

    tuyg = pd.DataFrame (tuy.groupby(["State", "Quarter"])["RegisteredUsers"].sum())
    tuyg.reset_index(inplace= True)

    fig_top_plot_1 = px.bar(tuyg, x= "State", y= "RegisteredUsers", color= "Quarter", width=1000, height=800, 
                            color_discrete_sequence= px.colors.sequential.Burgyl, hover_name= "State",
                            title= f"{year} REGISTERED USER")
    st.plotly_chart(fig_top_plot_1)

    return tuy

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#Top_User_plot_2

def Top_User_plot_2(df, State):

    tuys = df[df["State"]== State]
    tuys.reset_index(drop= True, inplace= True)

    fig_top_plot_2 = px.bar(tuys, x= "Quarter", y= "RegisteredUsers", hover_data= "Pincode",  width=1000, height=800, 
                                color_continuous_scale = px.colors.sequential.Viridis, 
                                title= "REGISTERED USER AND PINCODE QUARTER DATA")
    st.plotly_chart(fig_top_plot_2)


#=========================================================================================================================================================================

# Sql Query_1

def top_chart_transaction_amount(table_name): 
    
    connect = psycopg2.connect(
            host = "localhost",
            user = 'postgres',
            port = '5432',
            password = '01234',
            database = 'phonepy_transaction'
    )

    connect.set_isolation_level (ISOLATION_LEVEL_AUTOCOMMIT)

    cursor = connect.cursor()

    # Plot_1

    query1 = f'''select state, Sum(transaction_amount) as transaction_amount
            from {table_name}
            group  by state
            order by transaction_amount DESC
            LIMIT 10;'''

    cursor.execute(query1)
    tabel_1 = cursor.fetchall()
    connect.commit()


    df_1 = pd.DataFrame(tabel_1, columns= ("state", "transaction_amount"))

    col1, col2 = st.columns(2)
    with col1:

        fig_amount_1 = px.bar(df_1, x='state', y='transaction_amount', title = 'TOP 10 OF TRANSACTION AMOUNT', 
                                color_discrete_sequence= px.colors.sequential.Aggrnyl, height=650, width=600)
        st.plotly_chart(fig_amount_1)



    # Plot_2

    query2 = f'''select state, Sum(transaction_amount) as transaction_amount
                from {table_name}
                group  by state
                order by transaction_amount
                LIMIT 10;'''

    cursor.execute(query2)
    tabel_2 = cursor.fetchall()
    connect.commit()

    df_2 = pd.DataFrame(tabel_2, columns= ("state", "transaction_amount"))

    with col2:

        fig_amount_2 = px.bar(df_2, x='state', y='transaction_amount', title = 'LAST 10 OF TRANSACTION AMOUNT', 
                                color_discrete_sequence= px.colors.sequential.Aggrnyl_r, height=700, width=600)
        st.plotly_chart(fig_amount_2)


    # Plot_3

    query3 = f'''select state, Avg(transaction_amount) as transaction_amount
                from {table_name}
                group  by state
                order by transaction_amount;'''

    cursor.execute(query3)
    tabel_3 = cursor.fetchall()
    connect.commit()

    df_3 = pd.DataFrame(tabel_3, columns= ("state", "transaction_amount"))

    fig_amount_3 = px.bar(df_3, y='state', x='transaction_amount', title = 'AVERAGE OF TRANSACTION AMOUNT', hover_name= "state", orientation= 'h', 
                            color_discrete_sequence= px.colors.sequential.Bluered_r, height=800, width=1000)
    st.plotly_chart(fig_amount_3)

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Sql Query_2

def top_chart_transaction_count(table_name): 
    
    connect = psycopg2.connect(
            host = "localhost",
            user = 'postgres',
            port = '5432',
            password = '01234',
            database = 'phonepy_transaction'
    )

    connect.set_isolation_level (ISOLATION_LEVEL_AUTOCOMMIT)

    cursor = connect.cursor()

    # Plot_1

    query1 = f'''select state, Sum(transaction_count) as transaction_count
            from {table_name}
            group  by state
            order by transaction_count DESC
            LIMIT 10;'''

    cursor.execute(query1)
    tabel_1 = cursor.fetchall()
    connect.commit()

    df_1 = pd.DataFrame(tabel_1, columns= ("state", "transaction_count"))

    col1, col2 = st.columns(2)
    with col1:

        fig_amount_1 = px.bar(df_1, x='state', y='transaction_count', title = 'TOP 10 OF TRANSACTION COUNT', 
                                color_discrete_sequence= px.colors.sequential.Aggrnyl, height=650, width=600)
        st.plotly_chart(fig_amount_1)



    # Plot_2

    query2 = f'''select state, Sum(transaction_count) as transaction_count
                from {table_name}
                group  by state
                order by transaction_count
                LIMIT 10;'''

    cursor.execute(query2)
    tabel_2 = cursor.fetchall()
    connect.commit()

    df_2 = pd.DataFrame(tabel_2, columns= ("state", "transaction_count"))

    with col2:

        fig_amount_2 = px.bar(df_2, x='state', y='transaction_count', title = 'LAST 10 OF TRANSACTION COUNT', 
                                color_discrete_sequence= px.colors.sequential.Aggrnyl_r, height=700, width=600)
        st.plotly_chart(fig_amount_2)


    # Plot_3

    query3 = f'''select state, Avg(transaction_count) as transaction_count
                from {table_name}
                group  by state
                order by transaction_count;'''

    cursor.execute(query3)
    tabel_3 = cursor.fetchall()
    connect.commit()

    df_2 = pd.DataFrame(tabel_3, columns= ("state", "transaction_count"))

    fig_amount_3 = px.bar(df_2, y='state', x='transaction_count', title = 'AVERAGE OF TRANSACTION COUNT', hover_name= "state", orientation= 'h', 
                            color_discrete_sequence= px.colors.sequential.Bluered_r, height=800, width=1000)
    st.plotly_chart(fig_amount_3)

#=========================================================================================================================================================================


# Streamlit Part

# Title wide command
st.set_page_config (layout= 'wide')
st.title('PhonePe - Insurance Transaction Insights')

with st.sidebar: 
    select = option_menu ('Main Menu', ["Home", "DATA EXPLORATION", "TOP CHARTS"])

# Home Menu Details

if select == 'Home':
    pass


#====================================================================================================================================================================


elif select == "DATA EXPLORATION":
    
    tab1, tab2, tab3 = st.tabs (['Aggregated Analysis', 'Map Analysis', 'Top Analysis'])

    with tab1 :

        method_1 = st.radio("Select The Aggregated Method", ["Aggregated Insurance", "Aggregated Transaction", "Aggregated User"])

        if method_1 == 'Aggregated Insurance':

            col1, col2 = st.columns(2)
            with col1:

                years = st.slider('Select The Agg Insure Year', Agg_insurance["Year"].min(), Agg_insurance["Year"].max(), Agg_insurance["Year"].min())
            tac_Y = Transaction_amount_count_Y(Agg_insurance, years)

            col1, col2 = st.columns(2)
            with col1:

                quarter = st.slider('Select The Agg Insure Quarter', tac_Y["Quarter"].min(), tac_Y["Quarter"].max(), tac_Y["Quarter"].min())
            Transaction_amount_count_Y_Q(tac_Y, quarter)

#--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

        elif method_1 == "Aggregated Transaction":

            col1, col2 = st.columns(2)
            with col1:

                years = st.slider('Select The Agg Trans Year', Agg_Transaction["Year"].min(), Agg_Transaction["Year"].max(), Agg_Transaction["Year"].min())
            Agg_trans_tac_Y = Transaction_amount_count_Y(Agg_Transaction, years)

            col1, col2 = st.columns(2)
            with col1:
                states = st.selectbox("Select The Agg Trans State",Agg_trans_tac_Y["State"].unique())

            Agg_trans_Transaction_type(Agg_trans_tac_Y,states)

            col1, col2 = st.columns(2)
            with col1:

                quarter = st.slider('Select The Agg Trans Quarter',Agg_trans_tac_Y ["Quarter"].min(), Agg_trans_tac_Y["Quarter"].max(), Agg_trans_tac_Y["Quarter"].min())
            Agg_trans_tac_Y_Q = Transaction_amount_count_Y_Q(Agg_trans_tac_Y, quarter)

            col1, col2 = st.columns(2)
            with col1:
                states = st.selectbox("Select The Agg Trans State_Type",Agg_trans_tac_Y_Q["State"].unique())

            Agg_trans_Transaction_type(Agg_trans_tac_Y_Q,states)

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

        elif method_1 == "Aggregated User":

            col1, col2 = st.columns(2)
            with col1:

                years = st.slider('Select The Agg User Year', Agg_User["Year"].min(), Agg_User["Year"].max(), Agg_User["Year"].min())
            Agg_user_Y = Agg_User_plot_1(Agg_User, years)

            col1, col2 = st.columns(2)
            with col1:

                quarter = st.slider('Select The Agg User Quarter',Agg_user_Y ["Quarter"].min(), Agg_user_Y["Quarter"].max(), Agg_user_Y["Quarter"].min())
            Agg_user_Y_Q = Agg_User_plot_2(Agg_user_Y, quarter)


            col1, col2 = st.columns(2)
            with col1:
                states = st.selectbox("Select The Agg User State", Agg_user_Y_Q["State"].unique())

            Agg_User_plot_3(Agg_user_Y_Q, states)


#=========================================================================================================================================================================
    

    with tab2 :
        method_2 = st.radio('Select The Map Insurance Method', ['Map Insurance', 'Map Transaction', "Map User"])

        if method_2 == "Map Insurance":

            col1, col2 = st.columns(2)
            with col1:

                years = st.slider('Select The Map Insure Year', Map_insurance["Year"].min(), Map_insurance["Year"].max(), Map_insurance["Year"].min())
            map_insur_tac_Y = Transaction_amount_count_Y(Map_insurance, years)

            col1, col2 = st.columns(2)
            with col1:
                states = st.selectbox("Select The Map Insure State", map_insur_tac_Y["State"].unique())

            Map_insur_District(map_insur_tac_Y, states)

            col1, col2 = st.columns(2)
            with col1:

                quarter = st.slider('Select The Map Insures Quarter',map_insur_tac_Y ["Quarter"].min(), map_insur_tac_Y["Quarter"].max(), map_insur_tac_Y["Quarter"].min())
            map_insur_tac_Y_Q = Transaction_amount_count_Y_Q(map_insur_tac_Y, quarter)

            col1, col2 = st.columns(2)
            with col1:
                states = st.selectbox("Select The Map Insure State_Type",map_insur_tac_Y_Q["State"].unique())
            Map_insur_District(map_insur_tac_Y_Q, states)

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

        elif method_2 == "Map Transaction":
            
            col1, col2 = st.columns(2)
            with col1:

                years = st.slider('Select The Map Trans Year', Map_Transaction["Year"].min(), Map_Transaction["Year"].max(), Map_Transaction["Year"].min() )
            map_trans_tac_Y = Transaction_amount_count_Y(Map_Transaction, years)

            col1, col2 = st.columns(2)
            with col1:
                states = st.selectbox("Select The Map Trans State", map_trans_tac_Y["State"].unique())
            Map_insur_District(map_trans_tac_Y, states)


            col1, col2 = st.columns(2)
            with col1:

                quarter = st.slider('Select The Map Trans Quarter_mt',map_trans_tac_Y ["Quarter"].min(), map_trans_tac_Y["Quarter"].max(), map_trans_tac_Y["Quarter"].min())
            map_trans_tac_Y_Q = Transaction_amount_count_Y_Q(map_trans_tac_Y, quarter)

            col1, col2 = st.columns(2)
            with col1:
                states = st.selectbox("Select The Map Trans State_Type", map_trans_tac_Y_Q["State"].unique())
            Map_insur_District(map_trans_tac_Y_Q, states)

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

        elif method_2 == "Map User":

            col1, col2 = st.columns(2)
            with col1:

                years = st.slider('Select The Map User Year', Map_User["Year"].min(), Map_User["Year"].max(), Map_User["Year"].min() )
            Map_User_Y = Map_User_plot_1(Map_User, years)


            col1, col2 = st.columns(2)
            with col1:

                quarter = st.slider('Select The Map User Quarter',Map_User_Y ["Quarter"].min(), Map_User_Y["Quarter"].max(), Map_User_Y["Quarter"].min())
            Map_User_Y_Q = Map_User_plot_2(Map_User_Y, quarter)


            col1, col2 = st.columns(2)
            with col1:
                states = st.selectbox("Select The Map User State", Map_User_Y_Q["State"].unique())
            Map_User_plot_3(Map_User_Y_Q, states)



#=========================================================================================================================================================================


    with tab3 :

        method_3 = st.radio('select The Map User Method', ["Top Insurance", "Top Transaction", "Top User"])

        if method_3 == "Top Insurance":

            col1, col2 = st.columns(2)
            with col1:

                years = st.slider('Select The Top Insure Year', Top_insurance["Year"].min(), Top_insurance["Year"].max(), Top_insurance["Year"].min())
            Top_insur_tac_Y = Transaction_amount_count_Y(Top_insurance, years)


            col1, col2 = st.columns(2)
            with col1:

                states = st.selectbox("Select The Top Insure State", Top_insur_tac_Y["State"].unique())
            Top_insurance_plot_1(Top_insur_tac_Y, states)


            col1, col2 = st.columns(2)
            with col1:
                
                quarter = st.slider('Select The Top Insure Quarter',Top_insur_tac_Y ["Quarter"].min(), Top_insur_tac_Y["Quarter"].max(), Top_insur_tac_Y["Quarter"].min())
            Top_insur_tac_Y_Q = Transaction_amount_count_Y_Q(Top_insur_tac_Y, quarter)


#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

        elif method_3 == "Top Transaction":
            col1, col2 = st.columns(2)
            with col1:

                years = st.slider('Select The Top Trans Year', Top_Transaction["Year"].min(), Top_Transaction["Year"].max(), Top_Transaction["Year"].min())
            Top_trans_tac_Y = Transaction_amount_count_Y(Top_Transaction, years)


            col1, col2 = st.columns(2)
            with col1:

                states = st.selectbox("Select The Top Trans State", Top_trans_tac_Y["State"].unique())
            Top_insurance_plot_1(Top_trans_tac_Y, states)


            col1, col2 = st.columns(2)
            with col1:

                quarter = st.slider('Select The Top Trans Quarter',Top_trans_tac_Y ["Quarter"].min(), Top_trans_tac_Y["Quarter"].max(), Top_trans_tac_Y["Quarter"].min())
            Top_trans_tac_Y_Q = Transaction_amount_count_Y_Q(Top_trans_tac_Y, quarter)

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

        elif method_3 == "Top User":

            col1, col2 = st.columns(2)
            with col1:

                years = st.slider('Select The Top User Year', Top_User["Year"].min(), Top_User["Year"].max(), Top_User["Year"].min())
            Top_User_Y = Top_User_plot_1(Top_User, years)

            col1, col2 = st.columns(2)
            with col1:

                states = st.selectbox("Select The Top User State", Top_User_Y["State"].unique())
            Top_User_plot_2(Top_User_Y, states)

          

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

elif select == "TOP CHARTS":
    
    question = st.selectbox("Select the Question", ["1. Transaction Amount and Count of Aggregated Insurance",
                                                    "2. Transaction Amount and Count of Map Insurance",
                                                    "3. Transaction Amount and Count of Top Insurance",
                                                    "4. Transaction Amount and Count of Aggregated Transaction",
                                                    "5. Transaction Amount and Count of Map Transaction",
                                                    "6. Transaction Amount and Count of Top Transaction",
                                                    "7. Transaction Count of Aggregated User",
                                                    "8. Registered User of Map User",
                                                    "9. App Opens of Map User",
                                                    "10. Registered User of Top User",
                                                    ])
    
    if question == "1. Transaction Amount and Count of Aggregated Insurance":

        st.subheader("TRANSACTION AMOUNT")
        top_chart_transaction_amount("agg_insurance")

        st.subheader("TRANSACTION COUNT")
        top_chart_transaction_count("agg_insurance")


    elif question == "2. Transaction Amount and Count of Map Insurance":

        st.subheader("TRANSACTION AMOUNT")
        top_chart_transaction_amount("map_insurance")

        st.subheader("TRANSACTION COUNT")
        top_chart_transaction_count("map_insurance")

    
    elif question == "3. Transaction Amount and Count of Top Insurance":

        st.subheader("TRANSACTION AMOUNT")
        top_chart_transaction_amount("top_insurance")

        st.subheader("TRANSACTION COUNT")
        top_chart_transaction_count("top_insurance")

    
    elif question == "4. Transaction Amount and Count of Aggregated Transaction":

        st.subheader("TRANSACTION AMOUNT")
        top_chart_transaction_amount("agg_transaction")

        st.subheader("TRANSACTION COUNT")
        top_chart_transaction_count("agg_transaction")

    
    elif question == "5. Transaction Amount and Count of Map Transaction":

        st.subheader("TRANSACTION AMOUNT")
        top_chart_transaction_amount("map_transaction")

        st.subheader("TRANSACTION COUNT")
        top_chart_transaction_count("map_transaction")

    
    elif question == "6. Transaction Amount and Count of Top Transaction":

        st.subheader("TRANSACTION AMOUNT")
        top_chart_transaction_amount("top_transaction")

        st.subheader("TRANSACTION COUNT")
        top_chart_transaction_count("top_transaction")


    elif question == "7. Transaction Count of Aggregated User":

        st.subheader("TRANSACTION COUNT")
        top_chart_transaction_count("agg_user")


    
