from ExtraClasses import Tools, myDB
from Scraper import thread_scrape

gen_tbl = 'dbo.general_data'
play_tbl = 'dbo.player_data'

def get_rprs():

    ncon = myDB()
    query = 'SELECT link, date, rpr_sum from (SELECT link, date, SUM(horse_rpr) as rpr_sum FROM {} GROUP BY link, date) as rpr_totals WHERE rpr_sum = 0  AND date >= DATEADD(day, -730, GETDATE()) ORDER BY date ASC'.format(play_tbl)
    rpr_sums = ncon.getDF(query)
    ncon.cls()

    return rpr_sums

rpr_sums = get_rprs()

def clear_data(link):
    ncon = myDB()
    play_qry = "DELETE FROM {} WHERE link = '{}'".format(play_tbl, link)
    gen_qry = "DELETE FROM {} WHERE link = '{}'".format(gen_tbl, link)

    ncon.qry(play_qry)
    ncon.qry(gen_qry)
    ncon.comm()
    ncon.cls()

def validate_rpr():

    nTool = Tools()

    for i, row in rpr_sums.iterrows():
        current_date = nTool.toStr(row['date'])
        next_date = nTool.toStr(nTool.nextDay(row['date']))
        threads = 1

        clear_data(row['link'])
        thread_scrape(current_date, next_date, threads)


def main():
    validate_rpr()


if __name__ == "__main__":
    main()

