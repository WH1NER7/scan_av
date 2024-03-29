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


def insert_todays_doc(barcode, qnt, wh_code, article, size, company):
    speed_data = {
        "date": datetime.now().strftime('%d-%m-%Y'),
        "quantity": [qnt],
        "barcode": barcode,
        "article": article,
        "size": size,
        "wh_code": wh_code,
        "company": company,
        'time_stamps': [datetime.now().strftime("%H:%M")]
    }
    db.sell_speed.insert_one(speed_data)


def add_sell_speed(barcode, qnt, wh_code_param, date, company):
    try:
        # Находим или создаем документ для данной комбинации date, barcode, wh_code
        existing_record = db.sell_speed.find_one({
            "date": date,
            "barcode": barcode,
            "wh_code": wh_code_param
        })

        if existing_record:
            # Если документ уже существует, обновляем его
            db.sell_speed.update_one(
                {"_id": existing_record["_id"]},
                {"$push": {"quantity": qnt, 'time_stamps': datetime.now().strftime("%H:%M")}}
            )
        else:
            # Если документ не существует, создаем новый
            new_record = {
                "date": date,
                "quantity": [qnt],
                "barcode": barcode,
                "wh_code": wh_code_param,
                "company": company,
                "time_stamps": [datetime.now().strftime("%H:%M")]
            }
            db.sell_speed.insert_one(new_record)
    except Exception as e:
        print(f"Error: {e}")


def find_qnt_track(date, barcode, wh_code_num):
    data = db.sell_speed.find()

    for data1 in data:
        if data1.get('barcode') == barcode and data1.get('date') == date:
            return data1.get(wh_code_num)

# add_all_old_reports_to_db()
# add_all_old_reports_to_db_new_format()


def find_qnt_doc_in_bd(date_start, date_finish, barcode, wh_code1):
    time_delta_param = 0

    new_time = date_start
    json_arr_to_return = []
    while datetime.strptime(date_finish, "%d-%m-%Y") != datetime.strptime(new_time, "%d-%m-%Y"):
        data = db.sell_speed.find({'barcode': barcode, "date": new_time, 'wh_code': wh_code1})

        start_json = {'barcode': barcode, "date": new_time, 'wh_code': wh_code1}
        try:
            for doc in data:
                qnt = doc.get('quantity')
                time_stamps = doc.get('time_stamps')
                for time_stamp in time_stamps:
                    start_json[time_stamp] = qnt[time_stamps.index(time_stamp)]
                if len(qnt) > 0:
                    json_arr_to_return.append(start_json)
        except:
            print('Запрашиваемая дата не существует')

        time_delta_param = 1
        data_some_days_ago = datetime.strptime(new_time, "%d-%m-%Y") - timedelta(days=time_delta_param)
        new_time = data_some_days_ago.strftime("%d-%m-%Y")
        if datetime.strptime(new_time, "%d-%m-%Y") < datetime.strptime(date_finish, "%d-%m-%Y"):
            break

    unique_els = []
    for dict_item in json_arr_to_return:
        if dict_item not in unique_els:
            unique_els.append(dict_item)
    return unique_els

# print(find_qnt_doc_in_bd('12-05-2023', '01-05-2023', 2037267708361, 507))


def insert_docs():
    db.sell_speed.delete_many({'date': "17-05-2023"})

    for date1 in ['17-05-2023']:
        data = db.sell_speed.find({"date": '15-05-2023'})
        for data1 in data:
            data1.pop('_id')
            data1['date'] = date1
            print(data1)
            db.sell_speed.insert_one(data1)

# insert_docs()


def get_data_for_day():
    wh_code_list = []

    for day in range(1, 8):
        date_day_ago = datetime.now() - timedelta(days=day)
        date_formatted = date_day_ago.strftime('%d-%m-%Y')
        data = db.sell_speed.find({"date": date_formatted})
        for data1 in data:
            wh_code_list.append(data1.get('wh_code'))

    return set(wh_code_list)


def get_qnt_arr_daily(date, wh_code2,  barcode):
    data = db.sell_speed.find_one({"date": date, 'wh_code': wh_code2, 'barcode': barcode})
    # for data1 in data:
    #     print(data1)
    return data.get("quantity")

# print(get_qnt_arr_daily('15-05-2023', 507, 2037280326849))


def add_to_db_sell_report(date, barcode, wh_code_num, reg_speed, losed_speed, sum_speed, article, size, company):
    db.sell_speed_report.insert_one({
        'upd_date': date,
        'barcode': barcode,
        'article': article,
        'size': size,
        'warehouse_code': wh_code_num,
        'regular_speed': reg_speed,
        'losed_speed': losed_speed,
        'summary_speed': sum_speed,
        'company': company
    })


def get_data_sell_speed():
    json_list = []
    data = db.sell_speed_report.find({"upd_date": datetime.utcnow().strftime('%d-%m-%Y')})
    count_id = 1

    for data1 in data:
        json_el = {
            "id": count_id,
            "barcode": data1.get('barcode'),
            "warehouse_code": data1.get('warehouse_code'),
            "regular_speed": data1.get('regular_speed'),
            "losed_speed": data1.get('losed_speed'),
            "summary_speed": data1.get('summary_speed'),
            'size': data1.get('size'),
            'article': data1.get('article')
        }
        count_id += 1
        json_list.append(json_el)

    return json_list

# print(get_data_sell_speed())


def sell_data_by_period(date_start, date_finish):
    new_time = date_start
    json_arr_to_return = []
    while datetime.strptime(date_finish, "%d-%m-%Y") != datetime.strptime(new_time, "%d-%m-%Y"):
        data = db.sell_speed.find({"date": new_time})
        for doc in data:
            try:
                barcode = doc.get('barcode')
                wh_code = doc.get('wh_code')
                asked_date = datetime.strptime(new_time, "%d-%m-%Y")
                last_qnt = doc.get('quantity')[len(doc.get('quantity')) - 1]
                date = datetime.strptime(new_time, "%d-%m-%Y") + timedelta(days=1)
                date_str = date.strftime("%d-%m-%Y")
                day_summary = db.sell_speed_report.find_one({"barcode": barcode, "warehouse_code": wh_code, "upd_date": date_str})
                nice_day_summary = {
                    'barcode': barcode,
                    'warehouse_code': wh_code,
                    'regular_speed': day_summary.get('regular_speed'),
                    'losed_speed': day_summary.get('losed_speed'),
                    'summary_speed': day_summary.get('summary_speed'),
                    'asked_date': asked_date.strftime("%d-%m-%Y"),
                    'last_tracked_qnt': last_qnt
                }
                json_arr_to_return.append(nice_day_summary)
            except Exception as e:
                # print(e)
                pass

        data_some_days_ago = datetime.strptime(new_time, "%d-%m-%Y") - timedelta(days=1)
        new_time = data_some_days_ago.strftime("%d-%m-%Y")
        if datetime.strptime(new_time, "%d-%m-%Y") < datetime.strptime(date_finish, "%d-%m-%Y"):
            break

    return json_arr_to_return


def get_wb_sup_tokens(company_name):
    tokens_arr = db.companys.find_one({'company_name': company_name}).get('tokens')
    sup_token = tokens_arr[0].get('suplier_token')
    wb_token = tokens_arr[0].get('wb_token')
    return f'WBToken={wb_token}; x-supplier-id={sup_token};'


def add_doc_to_fin_rep(columns, data, date):
    item = {"Date": date}
    counter = 0
    for feature in columns:
        try:
            item[feature] = data[counter]
            counter += 1
        except Exception as e:
            print(e)
    db.sell_reports.insert_one(item)


def get_sell_speed_report_data():
    return db.sell_speed.find({"date": '15-09-2023'})


def insert_sell_speed_report_data(data):
    db.sell_speed.insert_one(data)


def transform_and_insert_to_mongo(api_data, mongo_mapping):
    # Переименование ключей в соответствии с заданным соответствием
    renamed_data = {mongo_mapping[key]: api_data[key] for key in api_data if key in mongo_mapping}

    # Проверка уникальности записи в коллекции
    if not db.fin_reports.find_one({'srid': renamed_data['Srid'], 'justification_for_payment': renamed_data['justification_for_payment']}):
        # Вставка данных в коллекцию
        db.fin_reports.insert_one(renamed_data)
        print('Data inserted successfully.')
    else:
        print('Data already exists in the collection.')


def add_percent_to_sales():
    data = db.sell_speed_report.find({"upd_date": "15-08-2023"})
    for doc in data:
        speed = int(doc.get('regular_speed')) * 1.2
        db.sell_speed_report.update_one({"upd_date": "15-08-2023", '_id': doc.get('_id')}, {"$set": {"regular_speed": speed}})


def delete_nums():
    data = db.sell_reports.delete_many({
    'Date': '15.09.23'
    })


def delete_sell_speed_nums():
    data = db.sell_reports.delete_many({
        'Date': '09.10.23'
    })

# delete_sell_speed_nums()
# add_percent_to_sales()

def delete_duplicates():
    pipeline = [
        {
            '$match': {
                'Date': '09.10.23'
            }
        },
        {
            '$group': {
                '_id': {'barcode': '$barcode', 'warehouse_code': '$warehouse_code'},
                'count': {'$sum': 1},
                'duplicates': {'$push': '$_id'}
            }
        },
        {
            '$match': {
                'count': {'$gt': 1}
            }
        }
    ]

    duplicates = list(db.sell_speed_report.aggregate(pipeline))

    # Удалить дубликаты
    for duplicate in duplicates:
        duplicate_ids = duplicate['duplicates'][1:]  # Оставляем один документ, удаляем остальные
        db.sell_speed_report.delete_many({'_id': {'$in': duplicate_ids}})

# data = (db.sell_reports.find({}))
# money = 0
# for data1 in data:
#     money += int(data1.get('transfer_money'))
#     print(data1)
# print(money)


# delete_duplicates()