import csv

import numpy as np
import pandas as pd

from ExtraClasses import myDB, Tools
from Scraper import gen_tbl, play_tbl


def get_cols():
    ncon = myDB()
    df = ncon.get_cols(gen_tbl)
    df['Table'] = gen_tbl.replace('dbo.', '')

    df1 = ncon.get_cols(play_tbl)
    df1['Table'] = play_tbl.replace('dbo.', '')

    df = df.append(df1, ignore_index=True)
    ncon.cls()

    return df


def get_uniques(start_date, end_date):
    df = get_cols()
    df = df[~df['Column Name'].str.contains("link")]
    df = df[~df['Column Name'].str.contains("date")]
    df = df[~df['Column Name'].str.contains("edate")]
    df = df[~df['Column Name'].str.contains("race_title")].reset_index()

    ncon = myDB()
    for i, row in df.iterrows():

        col = row['Column Name']
        tbl = 'dbo.' + row['Table']

        date_qry = " WHERE {0}.date >= '{1}' and {0}.date < '{2}'".format(tbl, start_date, end_date)

        if col != 'time':
            query = "SELECT DISTINCT({}) FROM {}".format(col, tbl) + date_qry
        else:
            query = "SELECT DISTINCT(CONVERT(VARCHAR, {}, 8)) FROM {}".format(col, tbl) + date_qry

        data = ncon.fetchAll(query)

        ndf = pd.DataFrame(data, columns=[col])
        ndf = ndf.sort_values(by=col, ascending=False)

        if i == 0:
            df_res = ndf.copy()
        else:
            df_res = pd.concat([df_res, ndf], ignore_index=True, axis=1)


    df_res = df_res.replace(np.nan, '', regex=True)
    df_res.columns = df['Column Name'].values.tolist()

    return df_res


def get_data(file_name, start_date, end_date):
    nTool = Tools()
    cPath = nTool.cPath()

    logic_name = cPath + './csv/Logic.csv'

    df_logic = nTool.read_csv(logic_name)
    logic_dict = dict(zip(df_logic['Column'].values.tolist(), df_logic['Logic'].values.tolist()))


    df_qry = nTool.read_csv(file_name)
    df_qry = df_qry.replace('None', '')
    df_qry = df_qry.replace('none', '')

    col_heads = df_qry.columns.tolist()

    table_df = get_cols()
    table_vals = dict(zip(table_df['Column Name'].values.tolist(), table_df['Table'].values.tolist()))

    queries = []

    for head in col_heads:
        if head in df_logic['Column'].values.tolist():

            logic = logic_dict.get(head)
            table = 'dbo.' + table_vals.get(head)
            data = df_qry[head].values.tolist()
            data = [x for x in data if pd.notnull(x)]

            if logic == 'in':
                qry = table + '.' + head + ' in ('
                for val in data:
                    rval = nTool.double_quote(str(val)).replace('.0', '')
                    qry = qry + "'" + rval + "',"
                qry = qry[:-1] + ')'
                queries.append(qry)

            if logic == '=':
                qry = ''
                for val in data:
                    qry = qry + table + '.' + head + ' = ' + str(val) + " OR "
                qry = '(' + qry[:-3] + ')'
                queries.append(qry)

            if logic == 'like':
                qry = ''
                for val in data:
                    rval = nTool.double_quote(str(val))
                    if head == 'race_name':
                        rval = val.replace(',', '%')
                    qry = qry + table + '.' + head + ' LIKE ' + "'%" + rval + "%' AND "
                qry = qry[:-4]
                queries.append(qry)

    main_qry = ' WHERE '
    for qry in queries:
        main_qry = main_qry + qry + ' AND '

    date_qry = "dbo.general_data.date >= '{}' AND dbo.general_data.date < '{}'".format(start_date, end_date)
    main_qry = main_qry + date_qry

    query = 'SELECT * FROM {0} INNER JOIN {1} ON {0}.link = {1}.link {2} ORDER BY dbo.general_data.date DESC, country ASC, {0}.link ASC, row_index ASC'.format(
        gen_tbl, play_tbl, main_qry)

    ncon = myDB()
    res_df = ncon.getDF(query)
    ncon.cls()

    return res_df

def split_c(res_df):
    sc = 7

    for i in range(8, 0, -1):
        res_df.insert(sc, 'C' + str(i), '')

    for i, row in res_df.iterrows():
        for j in range(1, 9):
            cval = 'C' + str(j)
            csplit = row['race_name'].replace(' ', '').split(';')
            cres = ''

            for c in csplit:

                if cval in c:
                    cres += c.replace(cval + ':', '') + '; '
            print(str(i) + ' _ ' + cval + ' - ' + cres)
            res_df.loc[i].at[cval] = cres
            #res_df.at[i, cval] = cres
    return res_df

def out_res(df, save_name):
    if len(df.index) >= 0:
        save_name = save_name + '.csv'
        df.to_csv(save_name, encoding='utf-8-sig', index=False)
    else:
        save_name = save_name + '.xlsx'
        df.to_excel(save_name)



