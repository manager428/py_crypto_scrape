import pandas as pd

from ExtraClasses import Tools


def scrape_position(soup):
    nTool = Tools()
    class_name = '.rp-horseTable__pos__number'

    data = nTool.scrape_all(soup, class_name)

    for i, val in enumerate(data):
        res = val.split('(')[0]
        res = res.rstrip()
        data[i] = res

    data = nTool.list_zero(data)
    return data

# prize money from position
def scrape_prize(soup, df_play):
    nTool = Tools()
    
    selector = 'data-test-selector'
    attr = 'text-prizeMoney'
    prize_list = []
    currency_list = []
    
    try:
        n = 0
        data = soup.find_all(attrs={selector: attr})[0]

        for child in data:
            n += 1
            if n % 2 == 1 and n > 1:
                prize_list.append(float(nTool.trim_str(child)[1:].replace(',', '')))
                currency_list.append(nTool.trim_str(child)[:1])
    except Exception as e:
        print(e)
        pass

    
    for i in range(df_play['position'].count() - len(prize_list)):
        prize_list.append(0)
        currency_list.append(currency_list[0])

    df_play['prize_currency'] = currency_list
    df_play['prize_money'] = prize_list

    return df_play

def scrape_draw(soup):
    nTool = Tools()
    class_name = '.rp-horseTable__pos__draw'

    data = nTool.scrape_all(soup, class_name)

    for i, val in enumerate(data):
        res = nTool.rem_br(val)
        data[i] = res

    data = nTool.list_zero(data)
    return data


def scrape_hname(soup):
    nTool = Tools()
    class_name = '.rp-horseTable__horse__name'

    data = nTool.scrape_all(soup, class_name)
    data = nTool.list_zero(data)

    return data


def scrape_country(soup):
    nTool = Tools()
    class_name = '.rp-horseTable__horse__country'

    data = nTool.scrape_all(soup, class_name)

    for i, val in enumerate(data):
        res = nTool.rem_br(val)
        data[i] = res

    data = nTool.list_zero(data)
    return data


def scrape_price(soup, df_play):
    nTool = Tools()
    class_name = '.rp-horseTable__horse__price'

    price_d = []
    price_f = []
    symbol = []

    data = nTool.scrape_all(soup, class_name)

    for i, val in enumerate(data):
        res = val.replace('Evens', '1/1').replace('Evs', '1/1')
        data[i] = res

    for val in data:
        value_split = nTool.num_let(val)

        d = 0
        f = ''
        s = ''

        if len(value_split) >= 3:
            try:
                d = float(value_split[0]) / float(value_split[2]) + 1
            except:
                pass
            try:
                f = value_split[0] + '/' + value_split[2]
            except:
                pass

        if len(value_split) >= 4:
            s = value_split[3]

        price_d.append(d)
        price_f.append(f)
        symbol.append(s)

    df_play['price_decimal'] = price_d
    df_play['price_fraction'] = price_f
    df_play['price_symbol'] = symbol

    return df_play


def scrape_hage(soup, df_play, next_date):
    nTool = Tools()
    class_name = '.rp-horseTable__spanNarrow_age'
    data = nTool.scrape_all(soup, class_name)
    year = int(next_date[:4])

    age_data = []
    year_data = []
    for i, val in enumerate(data):
        age = 0
        byear = 0
        try:
            age = int(val)
            byear = year - age
        except:
            pass
        age_data.append(age)
        year_data.append(byear)

    age_data = nTool.list_zero(age_data)
    year_data = nTool.list_zero(year_data)

    df_play['horse_age'] = age_data
    df_play['birth_year'] = year_data

    return df_play


def scrape_weight(soup):
    nTool = Tools()
    class_name = '.rp-horseTable__st'
    selector = 'data-test-selector'
    attr = 'horse-weight-lb'

    data = []
    st = nTool.scrape_all(soup, class_name)
    lb = []

    for child in soup.find_all(attrs={selector: attr}):
        val = child.text
        lb.append(val)

    for i, s in enumerate(st):
        try:
            total = int(st[i]) * 14 + int(lb[i])
        except:
            total = st[i] + '-' + lb[i]
        data.append(total)

    data = nTool.list_zero(data)
    return data


def scrape_dist(soup, df_play, sdf):
    nTool = Tools()
    class_name = '.rp-horseTable__pos__length'

    upper = []
    beaten = []

    data = nTool.scrape_all(soup, class_name)

    for dist in data:
        res = 0
        val = dist.split('[')

        upres = nTool.replace_odds(val[0].rstrip())
        if '[' in dist:
            res = nTool.replace_odds(val[len(val) - 1].replace(']', '').lstrip())

        upper.append(nTool.replace_abbr(upres, sdf))
        beaten.append(nTool.replace_abbr(res, sdf))

    upper = nTool.list_zero(upper)
    beaten = nTool.list_zero(beaten)

    df_play['dist_upper'] = upper
    df_play['dist_beaten'] = beaten

    return df_play


def scrape_racen(soup):
    nTool = Tools()
    class_name = '.rp-horseTable__saddleClothNo'

    data = nTool.scrape_all(soup, class_name)

    for i, val in enumerate(data):
        res = val.replace('.', '')
        data[i] = res

    data = nTool.list_zero(data)
    return data


def scrape_ors(soup, attr):
    nTool = Tools()
    selector = 'data-ending'
    data = []

    for child in soup.find_all(attrs={selector: attr}):
        val = 0
        try:
            val = int(nTool.trim_str(child.text).replace('–', '0'))
        except:
            pass
        data.append(val)

    data = nTool.list_zero(data)
    return data


def scrape_name(soup, class_name):
    nTool = Tools()
    selector = 'data-test-selector'
    data = []
    n = 0
    for child in soup.find_all(attrs={selector: class_name}):
        n = n + 1
        if n % 2 == 0:
            continue
        val = nTool.trim_str(child.text)
        data.append(val)
    return data


def scrape_dams(soup, df_play):
    nTool = Tools()

    color = []
    sex = []
    sire = []
    sire_country = []
    dam = []
    dam_country = []
    damsire = []

    for child in soup.find_all(attrs={'data-test-selector': 'block-pedigreeInfoFullResults'}):
        val = child.text
        val = nTool.trim_str(val)
        val = val.splitlines()

        try:
            ndash = val.index('-')
        except:
            ndash = 2

        sexCol = val[0].rstrip()

        try:
            col_val = sexCol.split(' ')[0]
        except:
            col_val = ''
        try:
            sex_val = sexCol.split(' ')[1]
        except:
            sex_val = ''
        try:
            sire_val = nTool.split_combine(val[ndash - 1].rstrip().replace(' -', ''))
        except:
            sire_val = ''
        try:
            dam_val = nTool.split_combine(val[ndash + 1].rstrip())
        except:
            dam_val = ''
        try:
            damsire_val = nTool.rem_br(val[ndash + 2].rstrip())
        except:
            damsire_val = ''

        color.append(col_val)
        sex.append(sex_val)

        tmp_sire = sire_val.split('(')
        
        sire.append(tmp_sire[0])
        sire_cntry = ''
        if len(tmp_sire) == 2:
            sire_cntry = tmp_sire[1][:-1]
        sire_country.append(sire_cntry)

        tmp_dam = dam_val.split('(')
        dam.append(tmp_dam[0])
        dam_cntry = ''
        if len(tmp_dam) == 2:
            dam_cntry = tmp_dam[1][:-1]
        dam_country.append(dam_cntry)

        
        damsire.append(damsire_val)

    df_play['color'] = nTool.list_zero(color)
    df_play['sex'] = nTool.list_zero(sex)
    df_play['sire'] = nTool.list_zero(sire)
    df_play['sire_country'] = nTool.list_zero(sire_country)
    df_play['dam'] = nTool.list_zero(dam)
    df_play['dam_country'] = nTool.list_zero(dam_country)
    df_play['damsire'] = nTool.list_zero(damsire)

    return df_play


def scrape_pricevar(soup):
    nTool = Tools()
    class_name = '.rp-horseTable__commentRow'

    data = []
    values = nTool.scrape_all(soup, class_name)

    res = ''
    for val in values:
        if '(' in val:
            val_split = val.split('(')
            res_val = val_split[len(val_split) - 1]
            res_val = nTool.rem_br(res_val)
            res_val = res_val.replace('-', '/')

            if ' op ' in res_val or res_val.find('op ') == 0 or ' tchd ' in res_val or res_val.find('tchd ') == 0:
                res = res_val

        data.append(res)

    return data


def scrape_headgear(soup, df_play):
    nTool = Tools()
    class_name = '.rp-horseTable__spanNarrow.rp-horseTable__wgt'

    data = nTool.scrape_all(soup, class_name)
    hdg = []
    wnd = []

    for value in data:
        hdg_val = ''
        wnd_val = ''
        value_split = value.split('\n')
        print(value_split)

        if len(value_split) == 3:
            val = value_split[1]
            if nTool.is_int(val) is False:
                wnd_val = val
                hdg_val = nTool.rem_nums(value_split[2])
            else:
                val1 = value_split[2]
                if 'w' in val1 and len(val1) > 1:
                    wnd_val = val1
                else:
                    hdg_val = nTool.rem_nums(val1)
        elif len(value_split) == 2:
            val = value_split[1]
            if nTool.is_int(val) is False:
                if 'w' in val and len(val) > 1:
                    wnd_val = val
                else:
                    hdg_val = nTool.rem_nums(val)


        hdg.append(hdg_val)
        wnd.append(wnd_val)

    df_play['headgear'] = hdg
    df_play['wind_12'] = wnd
    return df_play

def scrape_horselink(soup):
    nTool = Tools()
    class_name = 'rp-horseTable__horse__name'
    data = nTool.scrape_all_links(soup, class_name)
    
    return data
def play_res(soup, cols_play, link, sdf, next_date):
    df_play = pd.DataFrame(columns=cols_play, index=range(0))

    
    df_play['position'] = scrape_position(soup)
    
    # Dmitriy
    df_play = scrape_prize(soup, df_play)

    df_play['row_index'] = df_play.index.values.tolist()
    df_play['link'] = link
    df_play['date'] = next_date
    df_play['draw'] = scrape_draw(soup)
    df_play['horse_name'] = scrape_hname(soup)
    df_play['horse_country'] = scrape_country(soup)
    df_play = scrape_price(soup, df_play)
    df_play = scrape_hage(soup, df_play, next_date)
    df_play['horse_weight'] = scrape_weight(soup)
    df_play = scrape_dist(soup, df_play, sdf)
    df_play['racecard_number'] = scrape_racen(soup)
    df_play['horse_or'] = scrape_ors(soup, 'OR')
    df_play['horse_ts'] = scrape_ors(soup, 'TS')
    df_play['horse_rpr'] = scrape_ors(soup, 'RPR')
    df_play['horse_jockey'] = scrape_name(soup, 'link-jockeyName')
    df_play['horse_trainer'] = scrape_name(soup, 'link-trainerName')
    df_play = scrape_dams(soup, df_play)
    df_play['price_var'] = scrape_pricevar(soup)
    df_play = scrape_headgear(soup, df_play)

    df_play['horse_link'] = scrape_horselink(soup)
    

    return df_play
