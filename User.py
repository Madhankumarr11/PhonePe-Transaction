import os
import json
# import pandas as pd

#Once created the clone of GIT-HUB repository then,
#Required libraries for the program

#This is to direct the path to get the data as states

path = 'D:/SRMK/Guvi/PhonePe-Transaction/pulse/data/aggregated/user/country/india/state/'
Agg_state_list = os.listdir(path)

#Agg_state_list--> to get the list of states in India

#<------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------>#

#This is to extract the data's to create a dataframe

for state in Agg_state_list:
    cur_states  = path + state + '/'
    Agg_year_list = os.listdir(cur_states)

    for year in Agg_year_list:
        cur_years = cur_states + year + '/'
        Agg_file_list = os.listdir(cur_years)

        for file in Agg_file_list:
            cur_files = cur_years + file
            data = open (cur_files)
            read = data.read()   # read issue clear code

            print(read)
            
        




