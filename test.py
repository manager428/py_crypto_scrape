
import requests
import re
from bs4 import BeautifulSoup
import json

def loop_data(link):
    n = 0
    while True:
        n = n + 1
        page = requests.get(link)
        data = page.text
        if data != '' or n > 1000:
            break
    return data

link = "https://www.racingpost.com/profile/horse/3161491/aim-for-the-stars/form"
data = loop_data(link)
soup = BeautifulSoup(data, features="html.parser")

profile = re.search('window.PRELOADED_STATE = (.+?);', soup.find('body').find('script').text)
if profile:
    json_profile = json.loads(profile.group(1))

    #print(json_profile['profile'])

    previous_owners =json_profile['profile']['previousOwners']
    owner_history = ""
    if previous_owners is not None:
        print("AAAA")
        for owner in previous_owners:
            owner_history = owner['ownerStyleName'] + ' owned the horse until ' + owner['ownerChangeDate'][:10] + owner_history 

    print(owner_history)
