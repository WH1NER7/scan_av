from datetime import datetime, timedelta
import os
import sys

import numpy as np
import pandas as pd

import openpyxl
import jinja2

from database.main import insert_todays_doc, add_sell_speed
from misc.arrays_n_xlsx import columns_names
from misc.inserter import inserter, get_actual_cards_info, wh_code
from misc.pathManager import PathManager
import schedule
from loguru import logger


logger.add("file_1.log", rotation="500 MB")
logger.add("file_1.log", colorize=True, format="<green>{time}</green> <level>{message}</level>")
logger.add(sys.stdout, colorize=True, format="<green>{time}</green> <level>{message}</level>")


def find_sell_speed(wh_name, nmId):
    data = pd.read_excel(
        PathManager.get(f'excels/speed_calc/global_speed.xlsx'))
    data_arrayed = data.values.tolist()
    for data_arr in data_arrayed:
        if data_arr[0] == wh_name and data_arr[1] == nmId:
            return data_arr[9]


@logger.catch
def start_day_sell_speed():
    print('start')
    if not os.path.isfile(
            PathManager.get(f'excels/speed_calc/sales_stats_{datetime.now().strftime("%d-%m-%Y")}.xlsx')):
        qty_wh_arr = inserter()
        qty_wh_arr = (list(map(list, {tuple(x) for x in qty_wh_arr})))
        excel_lines = []
        for item in qty_wh_arr:
            if item[0]:
                excel_lines.append([item[0], item[1], item[3], item[4], item[2], item[2], 0, 0, 0, 0])
        columns = ['Склад', 'Баркод', 'Артикул', 'Размер', f'{datetime.now().strftime("%H:%M")}', f'{datetime.now().strftime("%H:%M")}',
                   'Остатки в днях', 'Возвраты', 'Поставки', 'Потенциальная скорость']
        data = pd.DataFrame(excel_lines, columns=columns)
        data.style.format({'Баркод': "{:.2%}"})
        data.to_excel(
            PathManager.get(f'excels/speed_calc/sales_stats_{datetime.now().strftime("%d-%m-%Y")}.xlsx'),
            index=False)


def sell_speed():
    if os.path.isfile(PathManager.get(f'excels/speed_calc/sales_stats_{datetime.now().strftime("%d-%m-%Y")}.xlsx')) and datetime.now().strftime("%H:%M") > '00:09':
        print('start_sell_speed')
        date_str = datetime.now().strftime("%d-%m-%Y")
        data = pd.read_excel(
            PathManager.get(f'excels/speed_calc/sales_stats_{date_str}.xlsx'))

        columns = data.columns
        arr = data.to_numpy()
        arr = list(arr)
        qty_wh_arr = inserter()
        qty_wh_arr = (list(map(list, {tuple(x) for x in qty_wh_arr})))

        new_arr = []
        for item in arr:
            for line in qty_wh_arr:
                returns = 0
                supplies = 0
                sales = 0
                wh_time_not_empty = 0
                if item[0] == line[0] and item[1] == line[1]:
                    quantity_on_time = line[2]
                    sell_speed_skus_wh = find_sell_speed(line[0], line[1])
                    temp_arr = np.insert(item, len(item) - 4, quantity_on_time)
                    # print(line[2])
                    gaps_quantity = len(temp_arr[4:-4])
                    for qnt in temp_arr[4:-4]:
                        if qnt > 0:
                            wh_time_not_empty += 1
                    for i in range(len(temp_arr[4:-4]) - 1):
                        if temp_arr[4:-4][i] < temp_arr[4:-4][i + 1] and temp_arr[4:-4][i + 1] - temp_arr[4:-4][i] < 10:
                            returns = temp_arr[4:-4][i + 1] - temp_arr[4:-4][i] + returns
                        if temp_arr[4:-4][i] > temp_arr[4:-4][i + 1]:
                            sales = temp_arr[4:-4][i] - temp_arr[4:-4][i + 1] + sales
                        if temp_arr[4:-4][i] < temp_arr[4:-4][i + 1] and temp_arr[4:-4][i + 1] - temp_arr[4:-4][i] >= 10:
                            supplies = temp_arr[4:-4][i + 1] - temp_arr[4:-4][i] + supplies
                        try:
                            if temp_arr[4:-4][i] < temp_arr[4:-4][i + 1] and temp_arr[4:-4][i] == 0 and temp_arr[4:-4][i + 1] > 5 and temp_arr[4:-4][i - 1] > 5:
                                sales = 0
                                supplies = 0
                        except:
                            print('Вынужденный выход за пределы диапазона')
                    # temp_arr[len(temp_arr) - 1] = sales * wh_time_not_empty / gaps_quantity
                    temp_arr[len(temp_arr) - 1] = sales
                    temp_arr[len(temp_arr) - 2] = supplies
                    temp_arr[len(temp_arr) - 3] = returns
                    if sell_speed_skus_wh:
                        temp_arr[len(temp_arr) - 4] = quantity_on_time / sell_speed_skus_wh
                    else:
                        temp_arr[len(temp_arr) - 4] = 0
                    new_arr.append(temp_arr)
        columns = np.insert(columns, len(columns) - 4, f'{datetime.now().strftime("%H:%M")}')
        data_new = pd.DataFrame(new_arr, columns=columns)
        data_new.style.format({'Номенклатура': "{:.2%}"})
        data_new.to_excel(
            PathManager.get(f'excels/speed_calc/sales_stats_{date_str}.xlsx'),
            index=False)
        logger.info(f'Executed at time:{datetime.now()}', value=10)
        print('end')


def stat_for_day(time_delta):
    data_day_ago = datetime.now() - timedelta(days=time_delta)
    new_time = data_day_ago.strftime("%d-%m-%Y")

    data = pd.read_excel(
        PathManager.get(f'excels/speed_calc/sales_stats_{new_time}.xlsx'))
    data_arrayed = data.values.tolist()
    print(f'excels/speed_calc/sales_stats_{new_time}.xlsx')
    for arr in data_arrayed:
        del arr[2:-1]

    return data_arrayed


@logger.catch
def stats_for_day_per_hour():
    data_day_ago = datetime.now() - timedelta(days=1)
    new_time = data_day_ago.strftime("%d-%m-%Y")
    data = pd.read_excel(
        PathManager.get(f'excels/speed_calc/sales_stats_{new_time}.xlsx'))
    data_arrayed = data.values.tolist()
    global_data_per_day_by_hour = []
    columns_count = 0
    for arr in data_arrayed:
        data_for_hour_report = []
        wh_quantity_per_day = arr[2:-4]
        wh_name_n_barcode = arr[0:2]
        splits = [wh_quantity_per_day[i:i + 12] for i in range(0, len(wh_quantity_per_day), 11)]
        columns_count = len(splits)
        data_for_hour_report.append(wh_name_n_barcode[0])
        data_for_hour_report.append(wh_name_n_barcode[1])

        for hour_data in splits:
            returns = 0
            supplies = 0
            sales = 0
            excel_line = []
            for i in range(len(hour_data) - 1):
                if hour_data[i] < hour_data[i + 1] and hour_data[i + 1] - hour_data[i] < 2:
                    returns = hour_data[i + 1] - hour_data[i] + returns
                if hour_data[i] > hour_data[i + 1]:
                    sales = hour_data[i] - hour_data[i + 1] + sales
                if hour_data[i] < hour_data[i + 1] and hour_data[i + 1] - hour_data[i] >= 2:
                    supplies = hour_data[i + 1] - hour_data[i] + supplies
            excel_line.append(sales)
            excel_line.append(returns)
            excel_line.append(supplies)

            data_for_hour_report += excel_line
        global_data_per_day_by_hour.append(data_for_hour_report)

    columns = columns_names(columns_count)
    data = pd.DataFrame(global_data_per_day_by_hour, columns=columns)
    data.to_excel(
        PathManager.get(f'excels/speed_calc/stats_per_hours_{new_time}.xlsx'),
        index=False)


def global_sell_speed():
    speed_all_barc = []
    data_from_inserter = inserter()

    for qnt in data_from_inserter:
        speed_all_barc.append([qnt[0], qnt[1], qnt[3], qnt[4], 0, 0, 0, 0, 0, 0, 0, 0])

    for i in [7, 6, 5, 4, 3, 2, 1]:
        speed_for_day = stat_for_day(i)
        for wh_barcode in speed_for_day:
            for wh_barcode_global in speed_all_barc:
                if wh_barcode[0] == wh_barcode_global[0] and wh_barcode[1] == wh_barcode_global[1]:
                    wh_barcode_global[11 - i] = wh_barcode[2]

    for data in speed_all_barc:
        summ = sum(data[4:11])
        data[11] = summ / 7

    data = pd.DataFrame(speed_all_barc, columns=['Склад', 'Баркод', 'Артикул', 'Размер', '1', '2', '3', '4', '5', '6', '7', 'Усредненная скорость'])
    data.to_excel(PathManager.get(f'excels/speed_calc/global_speed.xlsx'), index=False)

# print(os.path.isfile(PathManager.get(f'excels/speed_calc/sales_stats_23-02-2023.xlsx')))


def stat_for_day_temp(time_delta):
    data_day_ago = datetime.now() - timedelta(days=time_delta)
    new_time = data_day_ago.strftime("%d-%m-%Y")

    data = pd.read_excel(
        PathManager.get(f'excels/speed_calc/sales_stats_{new_time}.xlsx'))
    columns = data.columns
    data_arrayed = data.to_numpy()
    print(f'excels/speed_calc/sales_stats_{new_time}.xlsx')

    return data_arrayed, columns


def rewrite_previous_reports():
    data_actual_info_cards = get_actual_cards_info()
    barcodes = data_actual_info_cards[0]
    articles = data_actual_info_cards[1]
    sizes = data_actual_info_cards[2]
    for i in range(3, 4):
        data_day_ago = datetime.now() - timedelta(days=i)
        new_time = data_day_ago.strftime("%d-%m-%Y")
        new_data_for_day = []
        data_for_day, columns = (stat_for_day_temp(i))
        columns = list(columns)
        for line in data_for_day:
            line = list(line)
            line[0] = wh_code(line[0])
            try:
                line.insert(2, articles[barcodes.index(line[1])])
            except:
                line.insert(2, 0)
            try:
                line.insert(3, sizes[barcodes.index(line[1])])
            except:
                line.insert(3, 0)
            new_data_for_day.append(line)
        columns.insert(2, 'Артикул')
        columns.insert(3, 'Размер')
        data = pd.DataFrame(new_data_for_day, columns=columns)
        data.to_excel(PathManager.get(f'excels/speed_calc/sales_stats_{new_time}.xlsx'), index=False)


def insert_qnt_on_wh():
    data = inserter()

    for qnt_on_wh in data:
        insert_todays_doc(qnt_on_wh[1], qnt_on_wh[2], qnt_on_wh[0], qnt_on_wh[3], qnt_on_wh[4],)
        print(qnt_on_wh)


def upd_qnt(date):
    data = inserter()

    for qnt_on_wh in data:
        add_sell_speed(qnt_on_wh[1], qnt_on_wh[2], qnt_on_wh[0], date)
        print(qnt_on_wh)


def start_day_sell_speed_test():
    insert_qnt_on_wh()


def main():
    schedule.every().day.at('00:00').do(start_day_sell_speed)
    schedule.every().day.at('00:00').do(start_day_sell_speed_test)
    schedule.every().day.at('00:04').do(global_sell_speed)
    schedule.every(5).minutes.do(sell_speed)
    schedule.every(5).minutes.do(upd_qnt(datetime.now().strftime('%d-%m-%Y')))
    # schedule.every().day.at('00:20').do(stats_for_day_per_hour)

    while True:
        schedule.run_pending()


if __name__ == '__main__':
    main()
