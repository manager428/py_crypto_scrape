import time
from concurrent.futures.thread import ThreadPoolExecutor

import requests
from bs4 import BeautifulSoup

from ExtraClasses import Tools, myDB
from GeneralData import gen_res
from PlayerData import play_res
from HorseData import horse_res

gen_tbl = 'dbo.general_data'
play_tbl = 'dbo.player_data'
horse_tbl = 'dbo.horse_data'


def thread_scrape(start_date, end_date, threads):
    nTool = Tools()

    delta = (nTool.toDate(end_date) - nTool.toDate(start_date)).days

    if delta < threads:
        threads = delta

    date_pairs = nTool.split_date(start_date, end_date, threads)
    executor = ThreadPoolExecutor(max_workers=threads)

    general_rows = nTool.row_count(gen_tbl)
    player_rows = nTool.row_count(play_tbl)
    
    for i in range(0, threads):
        d1 = date_pairs[i].split(':')[0]
        d2 = date_pairs[i].split(':')[1]
        print('Thread start: ' + d1 + ' - ' + d2 + '\n')
        executor.submit(main_scrape, d1, d2)

    executor.shutdown(wait=True)

    print('Total ' + str(nTool.row_count(gen_tbl) - general_rows) + ' rows have been added to general table')
    print('Total ' + str(nTool.row_count(play_tbl) - player_rows) + ' rows have been added to player table')


def main_scrape(start_date, end_date):
    nTool = Tools()
    cPath = nTool.cPath()
    vdf = nTool.read_csv(cPath + './csv/VenuesData.csv')
    ndf = nTool.read_csv(cPath + './csv/NamesData.csv')
    sdf = nTool.read_csv(cPath +'./csv/DistData.csv')
    df = nTool.read_csv(cPath +'./csv/DistData.csv')
    cndf = nTool.read_csv(cPath +'./csv/CourseNameData.csv')
    ctdf = nTool.read_csv(cPath +'./csv/CourseTitleData.csv')

    ncon = myDB()
    cols_general = ['link', 'date', 'track', 'country', 'time', 'race_title', 'race_name', 'C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'race_class',
                    'handicap_rating', 'age_class', 'distance_mls', 'distance', 'going', 'prize', 'total_runners',
                    'winning_time', 'winning_timevar', 'total_sp', 'edate']

    cols_player = ['link', 'date', 'position', 'prize_currency', 'prize_money', 'row_index', 'draw', 'horse_name', 'horse_country', 'price_decimal',
                   'price_fraction', 'price_symbol', 'horse_age', 'birth_year', 'horse_weight', 'dist_upper',
                   'dist_beaten', 'racecard_number', 'horse_or', 'horse_ts', 'horse_rpr', 'horse_jockey',
                   'horse_trainer', 'color', 'sex', 'sire', 'sire_country', 'dam', 'dam_country', 'damsire', 'price_var', 'headgear', 'wind_12', 'horse_link']

    cols_horse = ['link', 'birth_date', 'owner', 'owner_history']

    for single_date in nTool.daterange(nTool.toDate(start_date), nTool.toDate(end_date)):
        next_date = nTool.qtDate(single_date)
        print('Beginning Scrape: ' + next_date + '\n')
        try:
            links_list = page_links(vdf, next_date, ncon)


            if len(links_list) == 0:
                print('Already scraped: ' + next_date + '\n')
                continue

            insert_df(links_list, vdf, ndf, sdf, cndf, ctdf, cols_general, cols_player, cols_horse, ncon, next_date)
            print('Done Scraping: ' + next_date + '\n')

        except Exception as e:
            print(e)
            pass

    ncon.cls()


def insert_df(links_list, vdf, ndf, sdf, cndf, ctdf, cols_gen, cols_play, cols_horse, ncon, next_date):
    for link in links_list:
        try:
            all_data = page_results(link, vdf, ndf, sdf, cndf, ctdf, cols_gen, cols_play, cols_horse, next_date)
            ncon.insert_many(all_data['general'], gen_tbl)
            ncon.insert_many(all_data['player'], play_tbl)
            ncon.insert_update_horse(all_data['horse'], horse_tbl)
            ncon.comm()
        except Exception as e:
            print(e, ' -- inserting')
            ncon.roll()
            pass

    return


def page_links(vdf, date_f, ncon):
    base_url = "https://www.racingpost.com"
    links_url = "/results/" + date_f + "/time-order"

    page = requests.get(base_url + links_url)
    data = page.text
    soup = BeautifulSoup(data, features="html.parser")

    links_list = []
    for link in soup.find_all('a'):
        link_f = link.get('href')
        base_f = "/results/"
        base_f2 = "#fullReplay-resultList"

        if base_f in link_f and \
                date_f in link_f and \
                base_f + date_f not in link_f and \
                base_f2 not in link_f:

            venue = link_f.split('/')[3]

            if venue in vdf[vdf.columns[0]].values:
                links_list.append(base_url + link_f)

    links_list = list(dict.fromkeys(links_list))
    links_list = check_list(links_list, date_f, ncon)

    return links_list


def check_list(links_list, date_f, ncon):
    query = "SELECT link FROM {} WHERE date = '{}'".format(gen_tbl, date_f)
    df = ncon.getDF(query)
    links_list = [x for x in links_list if x not in df['link'].values]
    return links_list


def loop_data(link):
    n = 0
    while True:
        n = n + 1
        page = requests.get(link)
        data = page.text
        if data != '' or n > 1000:
            break
    return data


def page_results(link, vdf, ndf, sdf, cndf, ctdf, cols_gen, cols_play, cols_horse, next_date):
    data = loop_data(link)
    if data == '':
        return

    soup = BeautifulSoup(data, features="html.parser")
    
    all_data = {}

    df_gen = gen_res(soup, cols_gen, link, next_date, vdf, ndf, cndf, ctdf)
    df_play = play_res(soup, cols_play, link, sdf, next_date)
    df_horse = horse_res(df_play['horse_link'], cols_horse)

    all_data['general'] = df_gen
    all_data['player'] = df_play
    all_data['horse'] = df_horse

    return all_data


def main():
    start_time = time.time()
    thread_scrape('2020-10-17', '2020-10-18', 2)
    print(time.time() - start_time)



if __name__ == "__main__":
    main()
