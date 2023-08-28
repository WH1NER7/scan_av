import datetime
import time

import pandas as pd
import requests

from database.main import get_wb_sup_tokens, add_doc_to_fin_rep
from misc.arrays_n_xlsx import base64_to_xlsx
from misc.pathManager import PathManager


def wb_sell_report_by_date(date, company_name):
    cookie = get_wb_sup_tokens(company_name)
    # print(cookie)
    headers = {
        'Content-type': 'application/json',
        'Cookie': cookie,
        'Host': 'seller.wildberries.ru',
        'Origin': 'https://seller.wildberries.ru',
        'Referer': 'https://seller.wildberries.ru/analytics/sales',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 YaBrowser/23.5.4.674 Yowser/2.5 Safari/537.36'
    }

    data = {}
    response = requests.post(f'https://seller.wildberries.ru/ns/reportsviewer/analytics-back/api/report/supplier-goods/order?dateFrom={date}&dateTo={date}', headers=headers, data=data)
    response.raise_for_status()
    return response.json().get('data').get('id')


def download_wb_report(url_req, company_name):
    cookie = get_wb_sup_tokens(company_name)

    headers = {
        'Content-type': 'application/json',
        'Cookie': cookie,
        'Host': 'seller.wildberries.ru',
        'Origin': 'https://seller.wildberries.ru',
        'Referer': 'https://seller.wildberries.ru/analytics/sales',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 YaBrowser/23.5.4.674 Yowser/2.5 Safari/537.36'
    }

    response = requests.get(url_req, headers=headers)
    response.raise_for_status()
    return response.json().get('data')


def delete_wb_report(url_req, company_name):
    cookie = get_wb_sup_tokens(company_name)

    headers = {
        'Content-type': 'application/json',
        'Cookie': cookie,
        'Host': 'seller.wildberries.ru',
        'Origin': 'https://seller.wildberries.ru',
        'Referer': 'https://seller.wildberries.ru/analytics/sales',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 YaBrowser/23.5.4.674 Yowser/2.5 Safari/537.36'
    }

    response = requests.delete(url_req, headers=headers)
    response.raise_for_status()
    return response.status_code


def req_download_all_reports():
    date_str = datetime.datetime.now().strftime("%d-%m-%Y")

    # date_start = '25.08.23'
    # date_finish = '26.08.23'

    new_time = date_str

    # while datetime.datetime.strptime(date_finish, "%d.%m.%y") != datetime.datetime.strptime(new_time, "%d.%m.%y"):
    for company in ['MissYourKiss', 'Bonasita']:
        print(new_time)
        url = wb_sell_report_by_date(new_time, company)
        print(url)
        time.sleep(3)
        nice_url = "https://seller.wildberries.ru/ns/reportsviewer/analytics-back/api/report/supplier-goods/xlsx/" + str(url)
        decoded_excel = download_wb_report(nice_url, company)
        base64_to_xlsx(decoded_excel, PathManager.get(f'excels/sell_reports/{new_time}.xlsx'))

        delete_url = 'https://seller.wildberries.ru/ns/reportsviewer/analytics-back/api/report/supplier-goods/order/' + str(url)

        excel_data_df = pd.read_excel(PathManager.get(f'excels/sell_reports/{new_time}.xlsx'), engine='openpyxl')
        excel_data_df = excel_data_df.iloc[1:len(excel_data_df) - 1]
        columns = ['Brand', 'item', 'season', 'collection', 'name', 'seller_article', 'wb_article', 'barcode', 'size', 'contract', 'wh_name', 'income_pcs', 'ordered_pcs', 'order_sum', 'buyback_pcs', 'transfer_money', 'wh_limits']
        for line in excel_data_df.to_numpy():
            add_doc_to_fin_rep(columns, line, new_time)

        delete_wb_report(delete_url, company)

        # data_some_days_ago = datetime.datetime.strptime(new_time, "%d.%m.%y") + datetime.timedelta(days=1)
        # new_time = data_some_days_ago.strftime("%d.%m.%y")
        # if datetime.datetime.strptime(new_time, "%d.%m.%y") > datetime.datetime.strptime(date_finish, "%d.%m.%y"):
        #     break

    return 'goooood'


# req_download_all_reports('MissYourKiss')