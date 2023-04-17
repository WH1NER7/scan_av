from datetime import datetime, timedelta
from pymongo import MongoClient


try:
    conn = MongoClient()
    db = conn["gram_base"]
    print("Connected successfully!!!")
except:
    print("Could not connect to MongoDB")


def add_all_old_reports_to_db():
    pass


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
