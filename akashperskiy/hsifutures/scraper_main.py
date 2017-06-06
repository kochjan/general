#!/opt/python2.7/bin/python
######################################################################
## Copyright (c) Nipun Capital, L.P., 2011-2016.  All rights reserved.
##
## CONFIDENTIAL
##
## This unpublished material is proprietary to Nipun Capital, L.P.
## The algorithms, methods, techniques and information disclosed or
## described herein are trade secrets and/or confidential information
## owned by Nipun Capital, L.P.  Any use, reproduction or
## distribution, in whole or in part, is prohibited, except by express
## written permission of Nipun Capital, L.P.
######################################################################

import requests
import pandas as pd
from bs4 import BeautifulSoup
import dateutil
import datetime
import nipun.sql_io as sl

SCRAPING_URL = "https://www.hkex.com.hk/eng/stat/dmstat/sharedata.htm"

def p2f(pvalue):
    return float(pvalue.strip("%"))/100.0

def format_quantity(quantity):
    return int(quantity.replace(",", ""))

"""
Arguments: url - the url of the hkex cache or original site containing hsi share data.

Returns: participants_df - dataframe for participants
         categorical_df - dataframe for interests and contracts
         last_three_df - dataframe for last three aggregated values
"""
def generate_hsi_dataframes(url):
    req = requests.get(url)
    dom = BeautifulSoup(req.text,'html.parser')

    table = dom.find('table', class_="ms-rteTable-1")
    data_list = table.find_all("tr")

    date = data_list[0].find_all("td")[2].text

    split_index = date.index("-")
    end_date = date[split_index+1:].strip()

    year = (datetime.datetime.today() - pd.tseries.offsets.BDay(3)).year
    formatted_date = dateutil.parser.parse(end_date)
    formatted_date.replace(year)

    long_open_contracts = format_quantity(data_list[1].find_all("td")[2].text)
    long_open_interest = p2f(data_list[2].find_all("td")[2].text)

    short_open_contracts = format_quantity(data_list[13].find_all("td")[2].text)
    short_open_interest = p2f(data_list[14].find_all("td")[2].text)

    ls_turnover_contracts = format_quantity(data_list[25].find_all("td")[2].text)
    ls_turnover_interest = p2f(data_list[26].find_all("td")[2].text)

    long_turnover_contracts = format_quantity(data_list[37].find_all("td")[2].text)
    long_turnover_interest = p2f(data_list[38].find_all("td")[2].text)

    short_turnover_contracts = format_quantity(data_list[49].find_all("td")[2].text)
    short_turnover_interest = p2f(data_list[50].find_all("td")[2].text)

    index_futures_turnover = format_quantity(data_list[61].find_all("td")[2].text)
    cash_market_turnover = p2f(data_list[62].find_all("td")[2].text)
    exchange_participants = int(data_list[63].find_all("td")[2].text)

    categorical_data = []

    long_open_row = {"type": "long_open", "contracts": long_open_contracts, "interest": long_open_interest, "datadate": formatted_date, "created_at": datetime.datetime.today()}
    short_open_row = {"type": "short_open", "contracts": short_open_contracts, "interest": short_open_interest, "datadate": formatted_date, "created_at": datetime.datetime.today()}
    ls_turnover_row = {"type": "ls_turnover", "contracts": ls_turnover_contracts, "interest": ls_turnover_interest, "datadate": formatted_date, "created_at": datetime.datetime.today()}
    long_turnover_row = {"type": "long_turnover", "contracts": long_turnover_contracts, "interest": long_turnover_interest, "datadate": formatted_date, "created_at": datetime.datetime.today()}
    short_turnover_row = {"type": "short_turnover", "contracts": short_turnover_contracts, "interest": short_turnover_interest, "datadate": formatted_date, "created_at": datetime.datetime.today()}

    categorical_data += [long_open_row, short_open_row, ls_turnover_row, long_turnover_row, short_turnover_row]

    last_three = [{"index_futures_turnover": index_futures_turnover, "cash_market_turnover": cash_market_turnover, "exchange_participants": exchange_participants, "created_at": datetime.datetime.today(), "datadate": formatted_date}]

    long_open_participants = [row.find_all("td")[2].text for row in data_list[3:13]]
    short_open_participants = [row.find_all("td")[2].text for row in data_list[15:25]]
    ls_turnover_participants = [row.find_all("td")[2].text for row in data_list[27:37]]
    long_turnover_participants = [row.find_all("td")[2].text for row in data_list[39:49]]
    short_turnover_participants = [row.find_all("td")[2].text for row in data_list[51:61]]

    participants_data = []

    for i in range(10):
        long_open_row = {"value": p2f(long_open_participants[i]), "type": "long_open", "datadate": formatted_date, "participant": i+1, "created_at": datetime.datetime.today()}
        short_open_row = {"value": p2f(short_open_participants[i]), "type": "short_open", "datadate": formatted_date, "participant": i+1, "created_at": datetime.datetime.today()}
        ls_turnover_row = {"value": p2f(ls_turnover_participants[i]), "type": "ls_turnover", "datadate": formatted_date, "participant": i+1, "created_at": datetime.datetime.today()}
        long_turnover_row = {"value": p2f(long_turnover_participants[i]), "type": "long_turnover", "datadate": formatted_date, "participant": i+1, "created_at": datetime.datetime.today()}
        short_turnover_row = {"value": p2f(short_turnover_participants[i]), "type": "short_turnover", "datadate": formatted_date, "participant": i+1, "created_at": datetime.datetime.today()}
        participants_data += [long_open_row, short_open_row, ls_turnover_row, long_turnover_row, short_turnover_row]

    participants_df = pd.DataFrame(data=participants_data)
    categorical_df = pd.DataFrame(data=categorical_data)
    last_three_df = pd.DataFrame(data=last_three)

    return participants_df, categorical_df, last_three_df

"""
Main entry
generates hsi dataframes and writes to database
"""
def main():
    participants_df, categorical_df, last_three_df = generate_hsi_dataframes(SCRAPING_URL)
    sys.exit()


if __name__ == '__main__':
    main()
