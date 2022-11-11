import pandas as pd

from ExtraClasses import Tools


def scrape_track(soup):
    nTool = Tools()
    class_name = '.rp-raceTimeCourseName__name'
    res = nTool.scrape_one(soup, class_name)
    res = nTool.trim_str(res)
    return res


def scrape_country(vdf, link):
    res = ''
    try:
        track = link.split('/')[5]
        index = vdf.index[vdf[vdf.columns[0]] == track].tolist()[0]
        res = vdf[vdf.columns[1]][index]
    except:
        pass
    return res


def scrape_time(soup):
    nTool = Tools()
    class_name = '.rp-raceTimeCourseName__time'
    res = nTool.scrape_one(soup, class_name)

    return res


def scrape_title(soup):
    nTool = Tools()
    class_name = '.rp-raceTimeCourseName__title'
    res = nTool.scrape_one(soup, class_name)

    return res


def scrape_name(df_gen, ndf):
    res = ''
    k = 0
    title = df_gen['race_title'][0]

    df_gen['C1'] = ""
    df_gen['C2'] = ""
    df_gen['C3'] = ""
    df_gen['C4'] = ""
    df_gen['C5'] = ""
    df_gen['C6'] = ""
    df_gen['C7'] = ""
    df_gen['C8'] = ""

    for i, row in ndf.iterrows():
        if row['Name'] in title:
            res = res + row['C'] + ': ' + row['Name'] + '; '
            df_gen[row['C']] = row['Name']
            k = k + 1

    if k == 0:
        res = 'All Other'
    else:
        res = res[:-1]
    
    try:
        # new categorization rule
        age_class = df_gen['age_class'][0]
        if 'Q.R.' in title:
            res = 'C2: Conditions; C3: Qualified Riders'
            df_gen['C2'] = 'Conditions;'
            df_gen['C3'] = 'Qualified Riders;'
        if 'Pro/Am' in title or 'Pro-Am' in title:
            res = 'C6: Bumper;'
            df_gen['C6'] = "Bumper"
            df_gen['C1'] = ""
            df_gen['C2'] = ""
            df_gen['C3'] = ""
            df_gen['C4'] = ""
            df_gen['C5'] = ""
            df_gen['C7'] = ""
            df_gen['C8'] = ""
        if age_class == '2yo':
            res = 'C3: Conditions;'
            df_gen['C1'] = ""
            df_gen['C2'] = ""
            df_gen['C3'] = "Conditions"
            df_gen['C4'] = ""
            df_gen['C5'] = ""
            df_gen['C6'] = ""
            df_gen['C7'] = ""
            df_gen['C8'] = ""

        if age_class == '3yo':
            res = 'C4: Rated Race;'
            df_gen['C1'] = ""
            df_gen['C2'] = ""
            df_gen['C3'] = ""
            df_gen['C4'] = "Rated Race"
            df_gen['C5'] = ""
            df_gen['C6'] = ""
            df_gen['C7'] = ""
            df_gen['C8'] = ""

        if df_gen['country'][0] == 'Ireland':
            if 'INH Flat Race' in df_gen['race_title'][0]:
                res = 'C6: Bumper;'
                df_gen['C6'] = "Bumper"
            else:
                res = 'C4: Rated Race;'
                df_gen['C4'] = "Rated Race"
            
            df_gen['C1'] = ""
            df_gen['C2'] = ""
            df_gen['C3'] = ""
            df_gen['C5'] = ""
            df_gen['C7'] = ""
            df_gen['C8'] = ""
        if df_gen['country'][0] == 'UK' and 'NH Flat Race' in df_gen['race_title'][0]:
            res = 'C6: Bumper;'
            df_gen['C6'] = "Bumper"
            df_gen['C1'] = ""
            df_gen['C2'] = ""
            df_gen['C3'] = ""
            df_gen['C4'] = ""
            df_gen['C5'] = ""
            df_gen['C7'] = ""
            df_gen['C8'] = ""
        if res == 'All Other' and df_gen['handicap_rating'][0] is not '':
            res = 'C4: Handicap;'
            df_gen['C4'] = 'Handicap'
    except Exception as e:
        print(e, ' Parse Error')

    df_gen['race_name'] = res
    return df_gen


def scrape_class(soup):
    nTool = Tools()
    class_name = '.rp-raceTimeCourseName_class'
    res = 0
    try:
        res = nTool.scrape_one(soup, class_name)
        res = nTool.trim_str(res)
        res = nTool.rem_br(res)
        res = int(res.replace('Class ', ''))
    except:
        pass

    return res


def rating_age(gen_info, soup):
    nTool = Tools()
    class_name = '.rp-raceTimeCourseName_ratingBandAndAgesAllowed'

    age_class = nTool.scrape_one(soup, class_name)
    age_class = nTool.trim_str(age_class)
    age_class = nTool.rem_br(age_class)
    age_class = age_class.split(', ')

    if len(age_class) >= 2:
        gen_info['handicap_rating'] = age_class[0].replace('--', '')
        gen_info['age_class'] = age_class[1]
    else:
        gen_info['handicap_rating'] = ''
        gen_info['age_class'] = age_class[0]

    return gen_info


def scrape_distmls(soup):
    nTool = Tools()
    class_name = '.rp-raceTimeCourseName_distanceFull'

    res = nTool.scrape_one(soup, class_name)
    res = nTool.trim_str(res)
    res = nTool.rem_br(res)
    res = nTool.dist_conv(res)

    return res


def scrape_distance(soup):
    nTool = Tools()
    class_name = '.rp-raceTimeCourseName_distance'

    res = nTool.scrape_one(soup, class_name)
    res = nTool.trim_str(res)
    res = res.replace('½', '.5')
    res = nTool.dist_conv(res)

    return res


def scrape_going(soup):
    nTool = Tools()
    class_name = '.rp-raceTimeCourseName_condition'

    res = nTool.scrape_one(soup, class_name)
    res = nTool.trim_str(res)

    return res


def scrape_prize(soup):
    nTool = Tools()
    prize = ''
    selector = 'data-test-selector'
    attr = 'text-prizeMoney'

    try:
        n = 0
        data = soup.find_all(attrs={selector: attr})[0]

        for child in data:

            n += 1

            if n % 2 == 1 and n > 1:
                prize = prize + str(n // 2) + ': ' + nTool.trim_str(child) + '; '
        prize = prize[:-2]
    except:
        pass

    return prize


def scrape_totrun(soup):
    nTool = Tools()
    class_name = '.rp-raceInfo__value_black'

    res = nTool.scrape_one(soup, class_name)
    res = nTool.trim_str(res)
    res = res.replace(' ran', '')

    try:
        res = int(res)
    except:
        res = 0

    return res


def bot_data(df_gen, soup):
    nTool = Tools()
    class_name = '.rp-raceInfo'
    data = soup.select(class_name)

    wint = 'Winning time:'
    sp = 'Total SP'

    wres = float(0)
    wvar = 0
    sp_res = 0

    for child in data:
        val = child.text
        val = nTool.trim_str(val)
        val = val.splitlines()

        for i, v in enumerate(val):
            if wint in v:
                wint_res = val[i + 1]
                wint_res = wint_res.rstrip()
                wsplit = wint_res.split('(')

                wres = wsplit[0]
                wres = wres.rstrip()
                wres = nTool.format_time(wres)
                wvar = timevar_mod(wsplit)

            if sp in v:
                try:
                    sp_res = val[i + 1]
                    sp_res = sp_res.rstrip()
                    sp_res = sp_res.replace('%', '')
                    sp_res = int(sp_res)
                except:
                    pass

    df_gen['winning_time'] = wres
    df_gen['winning_timevar'] = wvar
    df_gen['total_sp'] = sp_res
    return df_gen


def timevar_mod(wsplit):
    nTool = Tools()
    mult = float(0)

    if len(wsplit) < 2:
        return float(999)

    wvar = wsplit[1]
    wvar = nTool.rem_br(wvar)

    if 'standard' in wvar:
        return float(0)

    if 'fast' in wvar:
        mult = float(1)

    if 'slow' in wvar:
        mult = float(-1)

    wvar_split = wvar.split('by')
    tval = wvar_split[len(wvar_split) - 1]
    res = nTool.format_time(tval)
    res = mult * res

    return res

def scrape_marker(df_gen, cndf, ctdf):
    
    res = 'All Other'
    race_course = df_gen['track'][0]
    race_title = df_gen['race_title'][0]
    race_name = df_gen['race_name'][0]
    distance_mls = df_gen['distance_mls'][0]
    age_class = df_gen['age_class'][0]

    # check with race course name
    for i, row in cndf.iterrows():
        if row['Value'] in race_course:
            if row['Option'] == 1:
                if 'Bumper' in race_title or 'National Hunt' in race_title :
                    res = 'Jumps'
                else:
                    res = row['Marker']
            else:
                res = row['Marker']

    # check with race title
    for i, row in ctdf.iterrows():
        if row['Value'] in race_title:
            res = row['Marker']

    if distance_mls <= 1.25:
        res = 'Flat'
    if distance_mls >= 2.75:
        res = 'Jumps'

    if 'Handicap' in race_name:
        if distance_mls <= 1.75:
            res = 'Flat'
        elif age_class == '3yo+':
            res = 'Flat'
    
    if ('Maiden' in df_gen['C3'][0] or 'Novice' in df_gen['C3'][0]) and '3' in age_class:
        res = 'Flat'

    if df_gen['country'][0] == 'UK':
        if 'Conditions' in df_gen['C2'][0]:
            res = 'Flat'
        if 'NH Flat Race' in race_title:
            res = 'Jumps'
    
    if 'Listed' in df_gen['C4'][0]:
        if '3' in age_class or distance_mls <= 1.9 :
            res = 'Flat'

    if res == 'All other' and 'Claiming' in df_gen['C2'][0]:
        res = 'Flat'

    if res == 'All other' and 'Selling' in df_gen['C2'][0]:
        res = 'Flat'

    return res


def gen_res(soup, cols_gen, link, next_date, vdf, ndf, cndf, ctdf):
    nTool = Tools()
    df_gen = pd.DataFrame(columns=cols_gen, index=range(0, 1))
    df_gen['link'] = link
    df_gen['date'] = next_date
    df_gen['track'] = scrape_track(soup)
    df_gen['country'] = scrape_country(vdf, link)
    df_gen['time'] = scrape_time(soup)
    df_gen['race_title'] = scrape_title(soup)
    
    df_gen['race_class'] = scrape_class(soup)
    df_gen = rating_age(df_gen, soup)
    df_gen['distance_mls'] = scrape_distmls(soup)
    df_gen['distance'] = scrape_distance(soup)
    df_gen['going'] = scrape_going(soup)
    df_gen['prize'] = scrape_prize(soup)
    df_gen['total_runners'] = scrape_totrun(soup)
    df_gen = bot_data(df_gen, soup)
    df_gen['edate'] = nTool.cDate()
    
    df_gen = scrape_name(df_gen, ndf)
    df_gen['marker'] = scrape_marker(df_gen, cndf, ctdf)

    return df_gen
