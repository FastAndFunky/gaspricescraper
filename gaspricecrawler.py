#!/usr/bin/env python3
import requests
import lxml
from bs4 import BeautifulSoup
import csv
import pandas as pd
from urllib.request import urlopen as uReq
import numpy as np
import os
from datetime import date
import sys
from termcolor import colored, cprint



# Function to remove tags
def remove_tags(html):
  
    # parse html content
    soup = BeautifulSoup(html, "html.parser")
  
    for data in soup(['style', 'script']):
        # Remove tags
        data.decompose()
  
    # return data by retrieving the tag content
    return ' '.join(soup.stripped_strings)


# Function to retrieve data from 98 bensin
url_98 = 'https://bensinpriser.nu/stationer/98/alla/alla'
url_diesel = 'https://bensinpriser.nu/stationer/diesel/alla/alla'
def get_site(link):
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36 QIHU 360SE'
    }
    f = requests.get(link, headers = headers)

    # Parse website through lxml as parser
    soup = BeautifulSoup(f.content,'lxml')
    return soup




# Function to extract the correct data froim the website
# Takes a parameter soup to work with
def extract_data(soup):
    location = soup.find(id='price_table').find_all('small')
    data = soup.find(id='price_table').find_all('tr', {"class":"table-row"})

    # Put into list and make string
    list = []
    for i in data:
        list.append(str(i))

    #Define lists for later saving
    tankstation = []
    stad = []
    pris = []
    is98 = False
    for k in range(len(list)):
        if "84845C" in list[k]:
            tankstation.append(list[k][(list[k].find("<b>")+3):list[k].find("<small>")])
            stad.append(list[k][(list[k].find("<small>")+7):list[k].find("</small>")])
            pris.append(list[k][(list[k].find("#84845C")+10):(list[k].find("</b><br/><small>")-2)])
            is98 = True
        elif "#000000" in list[k]:
            tankstation.append(list[k][(list[k].find("<b>")+3):list[k].find("<small>")])
            stad.append(list[k][(list[k].find("<small>")+7):list[k].find("</small>")])
            pris.append(list[k][(list[k].find("#000000")+10):(list[k].find("</b><br/><small>")-2)])


    resultat = pd.DataFrame([tankstation, stad, pris])
    additional = []
    if is98:
        for i in range(len(pris)):
            additional.append('98')
    else:
        for i in range(len(pris)):
            additional.append('Diesel')
    # resultat = pd.concat([resultat, pd.DataFrame(additional)], axis=1)
    row = pd.Series(additional, index = resultat.columns)
    resultat = resultat.append(row, ignore_index=True)
    return resultat


def save_to_csv(resultat98, resultat_diesel):

    #Create dataframe because that is needed for CSV
    df = pd.DataFrame(data=resultat98)
    df = pd.concat([resultat98,resultat_diesel])

    # Transpose to get other direction
    df = df.transpose()

    # Add Today's date to the data
    today = date.today()
    todaylist =[]
    for i in range(len(df)):
        todaylist.append(today)   
    df = pd.concat([df,pd.DataFrame(todaylist)], axis=1)

    # If the file already exists, do not include headers
    if os.path.isfile(r"C:\Users\M\Documents\Programmering\bensinpriser.csv"):
        read = pd.read_csv(r"C:\Users\M\Documents\Programmering\bensinpriser.csv")
        
        #If the date is the same do not add additional data
        print_red_on_cyan = lambda x: cprint(x, 'red', 'on_cyan')
        if read["Datum"].loc[read.index[2]] == today:
            print('\n')
            print('================================================')
            print_red_on_cyan("No additional information added since the date is the same")
            print('================================================')
            print('\n')
        else:
            df.to_csv(r"C:\Users\M\Documents\Programmering\bensinpriser.csv", mode='a', header=False, index=False)
        
            
    else:
        # Add descriptive column names as headers
        df.columns=["Tankstation","Stad","Pris", "Bränsle", "Tankstation","Stad","Pris", "Bränsle", "Datum"]
        df.to_csv(r"C:\Users\M\Documents\Programmering\bensinpriser.csv", mode='w', index=False)
    print(df)
    





# Execution

# Complex but working way of getting 98 data:
# Every step passes a data over to the next one, therefore the nesting
resultat98 = extract_data(get_site(url_98))

#Get diesel data:
resultat_diesel = extract_data(get_site(url_diesel))


save_to_csv(resultat98, resultat_diesel)

