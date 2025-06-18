import streamlit as st
from streamlit_option_menu import option_menu
import psycopg2
import pandas as pd

# Streamlit Part

# Title wide command

st.set_page_config (layout= 'wide')
st.title('PhonePe - Insurance Transaction Insights')

with st.sidebar: 

    select = option_menu ('Main Menu', ["Home", "DATA EXPLORATION", "TOP CHARTS"])

# Home Menu Details
if select == 'Home':
    pass

elif select == 'DATA EXPLORATION':

    tab1, tab2, tab3 = st.tabs (['Aggregated Analysis', 'Map Analysis', 'Top Analysis'])

    with tab1 :

        method_1 = st.radio("Select The Method", ["Aggregated Insurance", "Aggregated Transaction", "Aggregated User"])

        if method_1 == 'Aggregated Insurance':
            pass

        elif method_1 == "Aggregated Transaction":
            pass

        elif method_1 == "Aggregated User":
            pass
    

    with tab2 :
        method_2 = st.radio('Select The Method', ['Map Insurance', 'Map Transaction', "Map User"])

        if method_2 == "Map Insurance":
            pass

        if method_2 == "Map Transaction":
            pass

        if method_2 == "Map User":
            pass

    with tab3 :

        method_3 = st.radio('select The Method', ["Top Insurance", "Top Transaction", "Top User"])

        if method_3 == "Top Insurance":
            pass
        
        if method_3 == "Top Transaction":
            pass

        if method_3 == "Top User":
            pass


elif select == "TOP CHARTS":
    pass
