import datetime
import os
import sys

import numpy as np
import pandas as pd

from misc.arrays_n_xlsx import columns_names
from misc.inserter import inserter
from misc.pathManager import PathManager
import schedule
from loguru import logger

logger.add("file_1.log", rotation="500 MB")
logger.add(sys.stdout, colorize=True, format="<green>{time}</green> <level>{message}</level>")


@logger.catch
def sell_speed():
    if not os.path.isfile(
            PathManager.get(f'excels/speed_calc/sales_stats_{datetime.datetime.now().strftime("%d-%m-%Y")}.xlsx')):
        qty_wh_arr = inserter()
        qty_wh_arr = (list(map(list, {tuple(x) for x in qty_wh_arr})))
        excel_lines = []
        for item in qty_wh_arr:
            if item[0]:
                excel_lines.append([item[0], item[1], item[2], 0, 0, 0, 0])
        columns = ['Склад', 'Номенклатура', f'{datetime.datetime.now().strftime("%H:%M")}',
                   'Продажи', 'Возвраты', 'Поставки', 'Скорость продажи за день']
        data = pd.DataFrame(excel_lines, columns=columns)
        data.style.format({'Номенклатура': "{:.2%}"})
        data.to_excel(
            PathManager.get(f'excels/speed_calc/sales_stats_{datetime.datetime.now().strftime("%d-%m-%Y")}.xlsx'),
            index=False)
    elif os.path.isfile(
            PathManager.get(f'excels/speed_calc/sales_stats_{datetime.datetime.now().strftime("%d-%m-%Y")}.xlsx')):
        data = pd.read_excel(
            PathManager.get(f'excels/speed_calc/sales_stats_{datetime.datetime.now().strftime("%d-%m-%Y")}.xlsx'))
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
                if item[0] == line[0] and item[1] == line[1]:
                    temp_arr = np.insert(item, len(item) - 4, line[2])
                    for i in range(len(temp_arr[2:-4]) - 1):
                        if temp_arr[2:-4][i] < temp_arr[2:-4][i + 1] and temp_arr[2:-4][i + 1] - temp_arr[2:-4][i] < 2:
                            returns = temp_arr[2:-4][i + 1] - temp_arr[2:-4][i] + returns
                        elif temp_arr[2:-4][i] > temp_arr[2:-4][i + 1]:
                            sales = temp_arr[2:-4][i] - temp_arr[2:-4][i + 1] + sales
                        if temp_arr[2:-4][i] < temp_arr[2:-4][i + 1] and temp_arr[2:-4][i + 1] - temp_arr[2:-4][i] >= 2:
                            supplies = temp_arr[2:-4][i + 1] - temp_arr[2:-4][i] + supplies
                    temp_arr[len(temp_arr) - 1] = sales
                    temp_arr[len(temp_arr) - 2] = supplies
                    temp_arr[len(temp_arr) - 3] = returns
                    temp_arr[len(temp_arr) - 4] = sales
                    new_arr.append(temp_arr)
        columns = np.insert(columns, len(columns) - 4, f'{datetime.datetime.now().strftime("%H:%M")}')
        data = pd.DataFrame(new_arr, columns=columns)
        data.style.format({'Номенклатура': "{:.2%}"})
        data.to_excel(
            PathManager.get(f'excels/speed_calc/sales_stats_{datetime.datetime.now().strftime("%d-%m-%Y")}.xlsx'),
            index=False)
        logger.info(f'Executed at time:{datetime.datetime.now()}', value=10)


def stat_for_day():
    data = pd.read_excel(
        PathManager.get(f'excels/speed_calc/sales_stats_{datetime.datetime.now().strftime("%d-%m-%Y")}.xlsx'))
    data_arrayed = data.values.tolist()

    for arr in data_arrayed:
        del arr[2:-4]

    return data_arrayed


def stats_for_day_per_hour():
    data = pd.read_excel(
        PathManager.get(f'excels/speed_calc/sales_stats_{datetime.datetime.now().strftime("%d-%m-%Y")}.xlsx'))
    data_arrayed = data.values.tolist()
    global_data_per_day_by_hour = []
    columns_count = 0
    for arr in data_arrayed:
        data_for_hour_report = []
        wh_quantity_per_day = arr[2:-4]
        wh_name_n_barcode = arr[0:2]
        splits = [wh_quantity_per_day[i:i + 13] for i in range(0, len(wh_quantity_per_day), 12)]
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
                elif hour_data[i] > hour_data[i + 1]:
                    sales = hour_data[i] - hour_data[i + 1] + sales
                elif hour_data[i] < hour_data[i + 1] and hour_data[i + 1] - hour_data[i] >= 2:
                    supplies = hour_data[i + 1] - hour_data[i] + supplies
            excel_line.append(sales)
            excel_line.append(returns)
            excel_line.append(supplies)

            data_for_hour_report += excel_line
        global_data_per_day_by_hour.append(data_for_hour_report)

    columns = columns_names(columns_count)
    data = pd.DataFrame(global_data_per_day_by_hour, columns=columns)
    data.to_excel(
        PathManager.get(f'excels/speed_calc/stats_per_hours_{datetime.datetime.now().strftime("%d-%m-%Y")}.xlsx'),
        index=False)


def main():
    schedule.every(5).minutes.do(sell_speed)
    schedule.every().day.at('00:20').do(stats_for_day_per_hour)

    while True:
        schedule.run_pending()


if __name__ == '__main__':
    main()
