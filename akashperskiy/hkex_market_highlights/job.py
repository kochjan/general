from datetime import timedelta, date
import datetime
import pandas as pd
import requests
import json
import dateutil
import re
from pytz import timezone

def url_factory(dt):
    return "https://www.hkex.com.hk/eng/csm/ws/Highlightsearch.asmx/GetData?LangCode=en&TDD=%s&TMM=%s&TYYYY=%s" % (dt.day, dt.month, dt.year)

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

def create_df(lst=None):
    if lst is None:
        return pd.DataFrame(columns=["date", "exchange", "listed_companies", "listed_h_shares", "listed_red_stocks", "listed_securities", "total_market_capitalisation", "total_negotiable_capitalisation", "avg_pe_ratio", "total_turnover_shares", "total_turnover_hkd", "total_market_turnover_hkd"])
    else:
        return pd.DataFrame(lst, columns=["date", "exchange", "listed_companies", "listed_h_shares", "listed_red_stocks", "listed_securities", "total_market_capitalisation", "total_negotiable_capitalisation", "avg_pe_ratio", "total_turnover_shares", "total_turnover_hkd", "total_market_turnover_hkd"])

def create_df_from_data(data):
    name_list = ["", "", "hk_main", "hk_gem", "shanghai_a", "shanghai_b", "shenzhen_a", "shenzhen_b"]
    df = create_df()
    dt = dateutil.parser.parse(re.search("\((.*)\)", data.pop(0)['td'][1][0]).groups()[0])
    data.pop(0)
    for posx in range(1,4):
        for posy in range(2):
            row = [entry['td'][posx][posy] for entry in data[:-1]]
            row.append(data[-1]['td'][posx][0])
            row.insert(0, dt)
            row.insert(1, name_list[posx*2 + posy])
            df = pd.concat([df, create_df(lst=[row])])
    return df

def get_data_for_date(dt):
    data = json.loads(requests.get(url_factory(dt)).text)["data"]
    return data

def get_df_from_date(dt):
    data = get_data_for_date(dt)
    df = create_df_from_data(data)
    return df

def clean_data(data):
    if data == "n.a.":
        return ""
    split_point = data.index(" ")
    number = data[split_point+1:]
    number = float(number.replace(",", ""))
    return number

def clean_data_no_space(data):
    if data == "n.a.":
        return ""
    number = float(data.replace(",", ""))
    return number

def main():
    master_df = get_df_from_date(datetime.datetime.now(timezone('Asia/Hong_Kong')))
    master_df['total_market_capitalisation'] = master_df['total_market_capitalisation'].apply(clean_data)
    master_df['total_negotiable_capitalisation'] = master_df['total_negotiable_capitalisation'].apply(clean_data)
    master_df['total_turnover_hkd'] = master_df['total_turnover_hkd'].apply(clean_data)
    master_df['total_market_turnover_hkd'] = master_df['total_market_turnover_hkd'].apply(clean_data)
    master_df['total_turnover_shares'] = master_df['total_turnover_shares'].apply(clean_data_no_space)
    master_df['listed_securities'] = master_df['listed_securities'].apply(clean_data_no_space)
    master_df['listed_red_stocks'] = master_df['listed_red_stocks'].apply(clean_data_no_space)
    master_df['listed_h_shares'] = master_df['listed_h_shares'].apply(clean_data_no_space)
    master_df['listed_companies'] = master_df['listed_companies'].apply(clean_data_no_space)
    #TODO put master_df.tosql statement here.

if __name__ == "__main__":
    main()
