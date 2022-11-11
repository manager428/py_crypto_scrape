import pandas as pd
import requests
import re
import json

from bs4 import BeautifulSoup

from ExtraClasses import Tools


def loop_data(link):
    n = 0
    while True:
        n = n + 1
        page = requests.get(link)
        data = page.text
        if data != '' or n > 1000:
            break
    return data


def scrape_one_horse(soup, link, cols_horse):
    df_one_horse = pd.DataFrame(columns=cols_horse, index=range(0, 1))
    
    birth_date = '0000-00-00 00:00:00'
    owner = ''
    owner_history = ''

    try:
        pre_data = re.search('window.PRELOADED_STATE = (.+?)};', soup.find('body').find('script').text)
        
        if pre_data:
            pre_json = json.loads(pre_data.group(1) + '}')
            birth_date = pre_json['profile']['horseDateOfBirth'][:10]
            owner = pre_json["profile"]['ownerName']
            previous_owners = pre_json['profile']['previousOwners']

            if previous_owners is not None:
                for owner_old in previous_owners:
                    owner_history = owner_old['ownerStyleName'] + ' owned the horse until ' + owner_old['ownerChangeDate'][:10] + ', ' + owner_history 
    except Exception as e:
        print(e, '[--- horse ---]', link)
        
    df_one_horse['link'] = link
    df_one_horse['birth_date'] = birth_date
    df_one_horse['owner'] = owner
    df_one_horse['owner_history'] = owner_history[:-2]
    
    return df_one_horse

def horse_res(horse_links, cols_horse):
    df_horse = pd.DataFrame(columns=cols_horse)

    for link in horse_links.tolist():
        data = loop_data(link)
        if data == '':
            break

        soup = BeautifulSoup(data, features="html.parser")
        
        df_one = scrape_one_horse(soup, link, cols_horse)
        df_horse = df_horse.append(df_one, ignore_index=True)
    
    return df_horse
