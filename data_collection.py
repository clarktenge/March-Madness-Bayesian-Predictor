import pandas as pd
import requests
from bs4 import BeautifulSoup
from bs4 import SoupStrainer
import re
import matplotlib as mpl
from io import StringIO
import openpyxl




#This code automatically pulls team data into a dataframe
def get_data(url):
    data = requests.get(url)
    text = BeautifulSoup(data.text, 'html.parser')
    table = text.find('table')
    df = pd.read_html(StringIO(table.prettify()))[0]
    return df

#This pulls all the links for subsequent pages, think page 1->2->3,
#only issue is this seems to pull dupe page 2s
#Fixed issue above ^ by removing the last subsequent link in list, for some reason the dupe is always at the end
def get_subsequent_links(text, base_url="https://www.ncaa.com"):
    links = []
    alltags = text.find_all("a", attrs={"href": re.compile("stats")})
    
    for item in alltags:
        href = item.get("href")
        if href:  # make sure href is not None
            full_url = base_url + href
            links.append(full_url)
    return links

#This pulls the text from the url
def get_text(url):
    data = requests.get(url)
    text = BeautifulSoup(data.text, 'html.parser')
    return text

#This uses the above functions to pull the entire dataframe from the url
def get_df(url):
    text = get_text(url)
    link_list = get_subsequent_links(text)
    df_list = []

    for link in link_list[:-1]:    
        df = get_data(link)
        df_list.append(df)
    ret_df = pd.concat(df_list, ignore_index=True)
    return ret_df

#This returns a full pandas dataframe of every statistic on ncaa website
def get_full_data(urls):
    all_dfs = []

    #loops through list of links and adds the suffix to link
    for num in all_stats:
        link = 'https://www.ncaa.com/stats/basketball-men/d1/current/team/' + num
        df = get_df(link)

        #verifying that the future merge keys are strings
        df['Team'] = df['Team'].astype(str)

        #add current dataframe to list of dataframes
        all_dfs.append(df)

    #initilize the master dataframe
    master_df = None

    for i, df in enumerate(all_dfs):
        df['Team'] = df['Team'].astype(str)

        if master_df is None:
            master_df = df  # keep 'Rank' from first table if present
        else:
            # Identify overlapping columns other than 'Team'
            dup_cols = [col for col in df.columns if col in master_df.columns and col != 'Team']
            df = df.drop(columns=dup_cols)

            # Merge only on 'Team'
            master_df = pd.merge(master_df, df, on='Team', how='outer')

    return master_df


all_stats = [
    '474','216','1284','214', '1288', '1285', '148', '149' , '286', 
    '638', '150', '633', '151', '859', '857', '932', '146', '147', 
    '145', '215', '625', '152', '518', '153', '519', '931', '217', '168'
    ]

master_df = get_full_data(all_stats)
master_df.fillna(0, inplace=True)

#only run line if dont have sheet
#master_df.to_excel('data_raw.xlsx')

   