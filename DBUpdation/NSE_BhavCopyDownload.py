#!/usr/bin/env python # -*- coding: utf-8 -*-
# @Time : 04-10-2022 16:51
# @Author : Ankur
# Updated by: Viren@Incred// 4 April 2023

import pandas as pd
import datetime as dt
from datetime import datetime

import requests
import time
from random import randint



month_map = {1: 'JAN', 2: 'FEB', 3: 'MAR', 4: 'APR', 5: 'MAY', 6: 'JUN', 7: 'JUL', 8: 'AUG', 9: 'SEP', 10: 'OCT',11: 'NOV', 12: 'DEC'}

headers = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Cookie": "_gid=GA1.2.1546156665.1664770278; pointer=1; sym1=HDFC; _ga=GA1.1.1374087280.1663318620; _ga_PJSKY6CFJH=GS1.1.1664864656.33.0.1664864656.60.0.0; ak_bmsc=1E1BB664EBB2DCC7189D272A085ECE11~000000000000000000000000000000~YAAQNAosFyVPhqCDAQAAhI2EohHYPJgON7tTrUMQbzbeunFQHbjn9HDOV16ql53NqgoWb7Rfu1ldjvCYQZ3/hA+yv9dQcdeInrshQXoF3bagNtDIeHyZH5wj7PVLvXtEsM7nWfohjTtFCEKmtVmlDRpsiRgnZ3U5gbzm+iqXm7jzhmuVKczcDYVRGqj4lmpKlZ1aOVuRvMkBd+jX3Vu0fAVFYEXfsak3nJo+bMg8ZUt2mFfM82IFzVpHEoMp3bnQxNUo7bf3nept8zrXeAQLxQF0qe4IrRslPSUDbnN3H/gAwodHeWcbUkijbbQhk6dxmo84JC0zwhhjixyj1lkmOu4CJ7oJTIERK/xkfmj4TYd0VVGhZBcLkLvqa8WiMVCIWtA=; NSE-TEST-1=1910513674.20480.0000; bm_sv=C7779A43638721BB456D6A944CE5B84F~YAAQNAosF8VUhqCDAQAAPSCJohHVv01f3cUAFICmZc4czqK69Yo7VZLAO08aQDsn7NvVGqS3MMTOTKyjkv6cgE6Dbmub+P6krVSRbNmz1oLRAbQoYUgYuUAxEC66QzUjmPtpzSbIHua0UEjK/tZT9VWeZsQc6+BhSYr/t0Fh8Oxc2Yr+EIduUuuj8Z4IB+IAE8UNPFKsHiTtq4/9xpmJ5C4mjJ1mmAQJ50tKvA4PYOVLUTn7OkvBquD30tdclnUpF0E=~1; RT=z=1&dm=nseindia.com&si=aefb9c19-448e-46cb-a17d-eca7a6eb736c&ss=l8u1xmil&sl=1&tt=4w&bcn=%2F%2F684d0d4a.akstat.io%2F",
        "Host": "www1.nseindia.com",
        "Referer": "https://www1.nseindia.com/products/content/derivatives/equities/archieve_fo.htm",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
        "sec-ch-ua": "Google Chrome;v=105, Not)A;Brand;v=8,Chromium;v=105",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "Windows"
    }

def create_url(input_date):
    year = input_date.year
    month = month_map[input_date.month]
    full_date = input_date.strftime("%d%b%Y").upper()
    input_url = 'https://www1.nseindia.com/content/historical/DERIVATIVES/{}/{}/fo{}bhav.csv.zip'.format(year, month, full_date)
    return input_url


def main():
    #ref_dates = pd.read_excel('ref_dates.xlsx')
    #ref_dates['Date'] = pd.to_datetime(ref_dates['Date'], format="%Y-%d-%m")
    start_dt = datetime(2022, 12, 31)
    end_dt = datetime(2023, 4, 3)
    ref_dates_vals = [start_dt + dt.timedelta(it) for it in range(1, (end_dt - start_dt).days+1)]
    #ref_dates = ref_dates[(ref_dates['Date'] >= start_dt) & (ref_dates['Date'] < end_dt)]
    #ref_dates_vals = list(ref_dates['Date'])
    #output_path = r'G:\My Drive\resources\data\fo_bhavcopy\\'
    output_path = 'G:\Shared drives\BackTests\DB\Bhavcopy\\'
    for dt_val in ref_dates_vals:
        idate = dt_val.date()
        dt_str = idate.strftime("%Y%m%d")
        input_url = create_url(dt)
        resp = requests.get(input_url, headers=headers)
        if resp.status_code == 200:
            current_path = output_path + '\\' + str(dt.year) + '\\' + dt_str + '.zip'
            with open(current_path, 'wb') as f:
                f.write(resp.content)
                print('Downloaded data for date {}'.format(dt_str))
        else:
            print('Failed to extract url content for date {} with status code {}'.format(dt_str, resp.status_code))
        time.sleep(randint(1,5))


if __name__ == '__main__':
    main()

