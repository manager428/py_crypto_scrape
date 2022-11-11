import csv
import re
from datetime import datetime, timedelta
import os
import sys
import numpy as np
import pandas as pd
import pyodbc
from qtpy import QtGui

class Tools(object):

    def cDate(self):
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def tDate(self):
        return datetime.now().strftime('%Y%m%d%H:%M:%S')

    def oDate(self):
        return datetime.now().strftime('%Y-%m-%d')

    def qtDate(self, date_val):
        return date_val.strftime('%Y-%m-%d')

    def toDate(self, date_val):
        return datetime.strptime(date_val, '%Y-%m-%d')

    def toStr(self, date_val):
        return date_val.strftime('%Y-%m-%d')

    def strDate(self, date_val):
        return datetime.strptime(date_val, '%d %b %Y')

    def dateDiff(self, start_date, end_date):
        s_date = self.toDate(start_date)
        e_date = self.toDate(end_date)

        diff = (e_date - s_date)

        return diff.days

    def nextDay(self, date_val):
        return date_val + timedelta(days=1)

    def trunc_date(self, date_val):
        return datetime.date(date_val)

    def rem_nums(self, text):
        return re.sub(r'\d+', '', text)

    def is_int(self, s):
        try: 
            int(s)
            return True
        except ValueError:
            return False
    
    def trim_str(self, text):
        return re.sub("\n\s*\n*", "\n", text).rstrip().lstrip()

    def rem_br(self, text):
        return text.replace('(', '').replace(')', '')

    def daterange(self, start_date, end_date):
        for n in range(int((end_date - start_date).days)):
            yield start_date + timedelta(n)

    def remove_quotes(self, input):
        input = input.replace("'", "") \
            .replace('"', '') \
            .replace('/*', '') \
            .replace('-- ', '') \
            .replace('#', '') \
            .replace('%', '')

        return input

    def font_format(self):
        font = QtGui.QFont()
        font.setPointSize(12)
        font.setBold(False)
        font.setWeight(50)
        return font

    def to_csv(self, file_name, data_list):
        with open(file_name, 'w', encoding='utf-8-sig', newline='') as f:
            wr = csv.writer(f)
            for rows in data_list:
                wr.writerow([rows])

    def a_csv(self, file_name, data_list):
        with open(file_name, 'a', encoding='utf-8-sig', newline='') as f:
            wr = csv.writer(f)
            wr.writerow(data_list)

    def read_csv(self, name):
        df = pd.read_csv(name, encoding='utf-8-sig')
        return df

    def split_date(self, start_date, end_date, chunks):
        s_date = self.toDate(start_date)
        e_date = self.toDate(end_date)

        diff = (e_date - s_date) / chunks

        date_list = []
        for i in range(0, chunks + 1):
            date_val = s_date + diff * i
            date_list.append(self.trunc_date(date_val))

        date_pairs = []
        for i in range(0, len(date_list) - 1):
            date_pairs.append(str(date_list[i]) + ':' + str(date_list[i + 1]))

        return date_pairs

    def format_time(self, time_val):

        split_time = time_val.split('m')
        min = float(0)
        sec = float(0)

        if len(split_time) == 1:
            if 'm' in time_val:
                min = float(time_val.replace('m', '')) * 60
            if 's' in time_val:
                sec = float(time_val.replace('s', ''))

        if len(split_time) == 2:
            min = float(split_time[0]) * 60
            sec = float(split_time[1].replace('s', '').lstrip())

        res = min + sec

        return res

    def dist_conv(self, value):
        value_split = value.replace('m', 'm,').replace('f', 'f,').split(',')

        res = float(0)
        for val in value_split:
            if 'm' in val:
                res = res + float(val.replace('m', ''))
            if 'f' in val:
                res = res + float(val.replace('f', '')) / 8
            if 'yds' in val:
                res = res + float(val.replace('yds', '')) / 1760

        return float(round(res, 4))

    def num_let(self, value):
        res = re.findall('\d+|\D+', value)
        return res

    def row_count(self, tbl):
        ncon = myDB()
        query = 'SELECT COUNT(*) FROM {}'.format(tbl)
        value = ncon.fetchOne(query)
        ncon.cls()
        return value

    def scrape_one(self, soup, class_name):
        res = ''
        try:
            res = soup.select_one(class_name).text
        except:
            pass
        return res

    def scrape_all(self, soup, class_name):
        data = []
        try:
            for child in soup.select(class_name):
                data.append(self.trim_str(str(child.text)))
        except:
            pass
        return data

    def scrape_all_links(self, soup, class_name):
        base_url = "https://www.racingpost.com"
        data = []
        for link in soup.find_all('a', {'class': class_name}):
            try:
                data.append(base_url + link['href'])
            except:
                pass
        return data

    def list_zero(self, data):
        if len(data) == 0:
            return ''
        return data

    def replace_abbr(self, value, sdf):
        try:
            res = float(value)
        except:
            res = float(round(sdf['Dist'][sdf[sdf.Abbr.str.match('nk')].index[0]], 4))

        return res

    def replace_odds(self, value):
        value = value.replace('½', '.5') \
            .replace('¾', '.75') \
            .replace('¼', '.25')
        if value[:1] == '.':
            value = '0' + value

        return value

    def check_dates(self, start_date, end_date):
        if "Date" in start_date:
            return True

        if "Date" in end_date:
            return True

        if self.dateDiff(start_date, end_date) < 1:
            return True

        return False

    def cPath(self):
        return str(os.path.dirname(sys.argv[0]))

    def double_quote(self, val):
        return val.replace("'", "''")

    def split_combine(self, txt):
        txt_split = txt.split('(')

        if '(' in txt:
            res = txt_split[0].rstrip() + ' (' + txt_split[1].lstrip()
        else:
            res = txt_split[0].rstrip()

        return res

class myConn(object):
    def __init__(self):
        nTool = Tools()
        cPath = nTool.cPath()
        csv = cPath + './csv/Connection.csv'
        self.cdf = nTool.read_csv(csv)
        self.cdf = self.cdf.replace(np.nan, '', regex=True)

    def get_ip(self):
        return self.cdf['IP'][0]

    def get_port(self):
        port = str(self.cdf['PORT'][0])
        if len(port) > 0:
            return ',' + port + ';'
        return ';'

    def get_db(self):
        return str(self.cdf['DB'][0]) + ';'

    def get_user(self):
        return str(self.cdf['USER'][0]) + ';'

    def get_pass(self):
        return str(self.cdf['PASS'][0]) + ';'

    def get_driver(self):
        drvs = pyodbc.drivers()
        return drvs[len(drvs) - 1]

class myDB(object):
    def __init__(self):
        mc = myConn()
        # aws db
        # self.con = pyodbc.connect('Driver={' + mc.get_driver() + '};'
        #                           'Server=' + mc.get_ip() + mc.get_port() +
        #                           'Database=' + mc.get_db() +
        #                           'uid=' + mc.get_user() +
        #                           'pwd=' + mc.get_pass() +
        #                           'Trusted_Connection=no;')

        # localhost
        self.con = pyodbc.connect('Driver={' + mc.get_driver() + '};'+'Server=localhost;Database=' + mc.get_db() + 'Trusted_Connection=yes;')
        self.cur = self.con.cursor()

    def qry(self, query):
        self.cur.execute(query)

    def fetchOne(self, query):
        try:
            self.qry(query)
            res = self.cur.fetchone()[0]
            if res is None:
                res = -1
        except:
            res = -1
        return res

    def fetchAll(self, query):
        self.qry(query)
        res = self.cur.fetchall()
        res = [i[0] for i in res]
        return res

    def getDF(self, query):
        return pd.read_sql(query, self.con)

    def many(self, query, params):
        self.cur.executemany(query, params)

    def insert_many(self, df, tbl):

        cols_list = df.columns.tolist()
        marks = ", ".join("?" for x in cols_list)
        cols = str(tuple(cols_list)).replace("'", "")

        if len(cols_list) == 1:
            cols = cols.replace(',)', ')')

        query = "INSERT INTO {} {} VALUES ({})".format(tbl, cols, marks)
        self.cur.executemany(query, df.values.tolist())

    def insert_update_horse(self, df, tbl):
        cols_list = df.columns.tolist()
        marks = ", ".join("?" for x in cols_list)
        cols = str(tuple(cols_list)).replace("'", "")

        if len(cols_list) == 1:
            cols = cols.replace(',)', ')')

        for index, row in df.iterrows():
            query = "SELECT link FROM {} WHERE link='{}'".format(tbl, row['link'])
            exist = len(self.fetchAll(query))
            if exist >= 1:
                query = "UPDATE {} SET owner = ?, owner_history = ? WHERE link = ? ".format(tbl)
                self.cur.execute(query, (row['owner'], row['owner_history'], row['link']))
                
            else:
               query = "INSERT INTO {} {} VALUES ({})".format(tbl, cols, marks) 
               self.cur.execute(query, row.tolist())


    def get_cols(self, gen_tbl):
        get_name = 'dbo.get_name'
        query = "EXEC {} @tname = N'{}'".format(get_name, gen_tbl)
        df = self.getDF(query)
        return df

    def comm(self):
        self.con.commit()

    def roll(self):
        self.con.rollback()

    def cls(self):
        self.cur.close()
        self.con.close()
