import math

import pandas as pd
import requests as requests

from misc.arrays_n_xlsx import transpose_array, read_xlsx
from misc.pathManager import PathManager
from misc.spp_req import post_request_spp

spp = (post_request_spp('https://www.wildberries.ru/webapi/personalinfo').get('value').get('personalDiscount'))


def wh_name(code):
    codes = transpose_array(
        read_xlsx(PathManager.get('excels/Соответствия складов 19.10.xlsx'), ['Код']))[0]
    names = transpose_array(
        read_xlsx(PathManager.get('excels/Соответствия складов 19.10.xlsx'), ['Склад']))[0]

    for i in range(len(codes)):
        if codes[i] == code:
            return names[i]
    return False

def wh_names():
    codes = pd.read_excel(PathManager.get('excels/Соответствия складов 19.10.xlsx')).values.tolist()
    return codes

def inserter():
    wh_names_arr = wh_names()
    info = []
    barcodes = transpose_array(read_xlsx(PathManager.get('excels/11otchet.xlsx'), ['Баркод']))[0]
    articles = transpose_array(read_xlsx(PathManager.get('excels/11otchet.xlsx'), ['Номенклатура']))[0]
    sizes = transpose_array(read_xlsx(PathManager.get('excels/11otchet.xlsx'), ['Размер вещи']))[0]
    articles1 = list(map(str, articles))
    string = ';'.join(articles1)
    string1 = f'https://card.wb.ru/cards/detail?spp={spp}&regions=64,58,83,4,38,80,33,70,82,86,30,69,22,66,31,40,1,48&pricemarginCoeff=1.0&reg=1&appType=1&emp=0&locale=ru&lang=ru&curr=rub&couponsGeo=2,12,7,3,6,13,21&dest=-1113276,-79379,-1104258,-5818883&nm=' + string
    response = requests.get(string1)
    json1 = response.json()
    for u in range(len(json1.get('data').get('products'))):
        sizesSpp = json1.get('data').get('products')[u].get('sizes')
        for i in sizesSpp:
            for j in range(len(sizes)):
                try:
                    if i.get('origName') == sizes[j] and json1.get('data').get('products')[u].get('id') == articles[j]:
                        currentBarcode = math.floor(barcodes[j])
                        tmp = i.get('stocks')
                        if i.get('stocks') != []:
                            for w in tmp:
                                info.append([wh_name(w.get('wh')), currentBarcode, w.get('qty')])
                        else:
                            for wh_name_in_arr in wh_names_arr:
                                info.append([wh_name_in_arr[0], currentBarcode, 0])
                except:
                    pass
    return list(map(list, {tuple(x) for x in info}))
