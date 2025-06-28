import pandas as pd
import requests
from bs4 import BeautifulSoup
from bs4 import SoupStrainer
import re
from io import StringIO




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


ATR_df = get_df('https://www.ncaa.com/stats/basketball-men/d1/current/team/474')
print(ATR_df.head)