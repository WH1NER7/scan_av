from datetime import datetime, timedelta

import pandas as pd
from pymongo import MongoClient

from misc.inserter import get_actual_cards_info, wh_code
from misc.pathManager import PathManager

try:
    conn = MongoClient()
    db = conn["gram_base"]
    print("Connected successfully!!!")
except:
    print("Could not connect to MongoDB")



def add_all_old_reports_to_db():
    actual_cards_data = get_actual_cards_info()

    barcodes = actual_cards_data[0]
    articles = actual_cards_data[1]
    sizes = actual_cards_data[2]

    for day in range(13, 100):
        try:
            data_day_ago = datetime.now() - timedelta(days=day)
            new_time = data_day_ago.strftime("%d-%m-%Y")
            print(new_time)
            data = pd.read_excel(
                PathManager.get(f'excels/speed_calc/sales_stats_{new_time}.xlsx'))
            data_arrayed = data.values.tolist()

            for excel_line in data_arrayed:
                barcode = excel_line[1]
                wh_name = excel_line[0]
                wh_code_num = int(wh_code(wh_name))
                print(wh_code_num)
                article = articles[barcodes.index(barcode)]
                print(article)
                size = sizes[barcodes.index(barcode)]
                insert_any_day_doc(barcode, excel_line[2], wh_code_num, article, size, new_time)
                print(barcode, excel_line[2], wh_code_num, article, size, new_time)
                for qnt in excel_line[3:-4]:
                    add_sell_speed(barcode, qnt, wh_code_num, new_time)
        except Exception as e:
            print(e)

def add_all_old_reports_to_db_new_format():
    for day in range(1, 13):
        try:
            data_day_ago = datetime.now() - timedelta(days=day)
            new_time = data_day_ago.strftime("%d-%m-%Y")
            print(new_time)
            data = pd.read_excel(
                PathManager.get(f'excels/speed_calc/sales_stats_{new_time}.xlsx'))
            data_arrayed = data.values.tolist()

            for excel_line in data_arrayed:
                wh_code_num = excel_line[0]
                barcode = excel_line[1]
                article = excel_line[2]
                size = excel_line[3]
                insert_any_day_doc(barcode, excel_line[4], wh_code_num, article, size, new_time)
                for qnt in excel_line[4:-4]:
                    add_sell_speed(barcode, qnt, wh_code_num, new_time)
        except Exception as e:
            print(e)

def insert_any_day_doc(barcode, qnt, wh_code_number, article, size, date):
    speed_data = {
        "date": date,
        "quantity": [qnt, qnt],
        "barcode": barcode,
        "article": article,
        "size": size,
        "wh_code": wh_code_number
    }
    db.sell_speed.insert_one(speed_data)


def insert_todays_doc(barcode, qnt, wh_code, article, size):
    speed_data = {
        "date": datetime.now().strftime('%d-%m-%Y'),
        "quantity": [qnt, qnt],
        "barcode": barcode,
        "article": article,
        "size": size,
        "wh_code": wh_code
    }
    db.sell_speed.insert_one(speed_data)


def add_sell_speed(barcode, qnt, wh_code, date):
    db.sell_speed.update_one({"barcode": barcode, "wh_code": wh_code, "date": date}, {"$push": {"quantity": qnt}})


def find_qnt_track(date, barcode, wh_code):
    data = db.sell_speed.find()

    for data1 in data:
        if data1.get('barcode') == barcode and data1.get('date') == date:
            return data1.get(wh_code)

add_all_old_reports_to_db()
add_all_old_reports_to_db_new_format()