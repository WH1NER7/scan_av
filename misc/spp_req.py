from datetime import datetime, timedelta

import requests


def post_request_spp(url):  # Функция для нахождения значения spp
    headers = {"Content-Type": "application/json; charset=utf-8",
               "Cookie": 'BasketUID=ba122ba2-b5a3-46d0-af0f-5877d7f262f7; _wbauid=4425762941660724039; _gcl_au=1.1.1811259854.1660724039; ___wbu=70026317-145a-4e79-81fd-f04885ca6cbd.1660724040; _ga=GA1.2.221691201.1660724040; __wba_s=1; WILDAUTHNEW_V3=97D79FC0FBA85DBD7FC490DBE736DE074B6F86E328D77E4008A6ECA7D1568B9F40954CE087EC7FE9DDEEC2B402E3E2F3316DDC795A3ED3600C08F8A0369F1C27E67CE79D583215C0DD58A91561F8F30100A49235D17977738F26A232CBD37F8CE7D2BC0A4A111B36C304280F8F9E34EE1702227C4A348221C31A797FA4DE4857C094C8A7AFCB913FD35927FF5DB3DC61B532CD098583C1309936AB6EF7055794BB58CD1D8D3F756F97EB02C51134393B327C0EAD1C5BB750F8CE75F1D7287AAA8B4FE603A8EA5E32494F2EC97E8DF6F148100F59752320DD0DAAED6C39210AE99565062850E8C97A3D2CCDDE3B28801F55F454532E15C162F8F197EEAC09D4D81FB1295FC8873B796A0DE65C55471E758947907BA87DACF2D50FCA54B3C53DCCED8CC689; _wbSes=CfDJ8BjlkHolJOpHqSNBMqPGxoG5VzUjcaleaYbEwD4H2RM2XaoMwmHaAdGxsVHv%2FTdiUyyrbjiEjxR5dWWjjd9h3%2F%2FeFT6z4a%2F82mqVK%2Bm%2Ft6y3oL64kgBNtjmgVTMifL1Xmdgc7OulQQlFrbqCFs0EXbrzQj13cLBGBkN9yxR7pxMA; __bsa=basket-ru-42; _ym_uid=1663067945348684095; _ym_d=1663067945; __wbl=cityId%3D0%26regionId%3D0%26city%3D%D0%95%D0%BA%D0%B0%D1%82%D0%B5%D1%80%D0%B8%D0%BD%D0%B1%D1%83%D1%80%D0%B3%26phone%3D84957755505%26latitude%3D56%2C837814%26longitude%3D60%2C596844%26src%3D1; __store=1733_686_117986_117501_507_3158_120762_204939_130744_159402_2737_1193_206968_206348_205228_172430_117442_117866_121709; __region=80_64_58_83_4_38_33_70_82_69_68_86_30_40_48_1_22_66_31; __pricemargin=1.0--; __cpns=2_12_7_3_6_13_21; __sppfix=4; __dst=-1113276_-79379_-1104258_-5803327; ncache=1733_686_117986_117501_507_3158_120762_204939_130744_159402_2737_1193_206968_206348_205228_172430_117442_117866_121709%3B80_64_58_83_4_38_33_70_82_69_68_86_30_40_48_1_22_66_31%3B1.0--%3B2_12_7_3_6_13_21%3B4%3B-1113276_-79379_-1104258_-5803327; front-wbguide=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6MTMwMjgxNiwiZ29vZ2xlX2lkIjoiMjIxNjkxMjAxLjE2NjA3MjQwNDAiLCJ5YW5kZXhfaWQiOiIxNjYzMDY3OTQ1MzQ4Njg0MDk1IiwiYWNjZXNzZXMiOlt7ImlkIjoxLCJuYW1lIjoiYXJ0aWNsZXMiLCJkZXNjcmlwdGlvbiI6ItCh0L7Qt9C00LDQvdC40LUg0YHRgtCw0YLQtdC5In1dLCJhY3RpdmUiOnRydWUsImF2YXRhcl9saW5rIjoiaHR0cHM6Ly9pbWFnZXMud2JzdGF0aWMubmV0L2d1aWRlL2RlZmF1bHQtYXZhdGFyLnBuZyIsImV4cCI6MTY5OTk1NDQ1OH0; adult-content-wbguide=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJTaG93QWR1bHQiOnRydWUsImV4cCI6MTY5OTk1NDQ1OH0; x-supplier-id-external=1f887b2d-305d-5025-bc81-caab0465bb07; um=uid%3Dw7TDssOkw7PCu8K5wrjCt8K4wrbCtsKzwrA%253d%3Aproc%3D100%3Aehash%3Dd41d8cd98f00b204e9800998ecf8427e; ___wbs=cc641b95-4154-4498-8e6d-8552f01ce9b3.1668496562; __tm=1668507439'
               }
    data = {}
    response = requests.post(url, headers=headers, json=data)
    response.raise_for_status()
    return response.json()


def get_fbo_sku(prod_id):
    headers = {
        'Client-Id': '1043385',
        'Api-Key': '48a95b86-26b2-48c6-afd5-309616e8b202',
        'Content-Length': '63',
        'Host': 'api-seller.ozon.ru'
    }
    data = {
        "offer_id": "",
        "product_id": prod_id,
        "sku": 0
    }
    response = requests.post('https://api-seller.ozon.ru/v2/product/info', headers=headers, json=data)
    response.raise_for_status()
    return response.json().get('result').get('fbo_sku')


def ozon_skus_ozon():
    headers = {
        'Client-Id': '1043385',
        'Api-Key': '48a95b86-26b2-48c6-afd5-309616e8b202',
        'Content-Length': '84',
        'Host': 'api-seller.ozon.ru'
    }
    data = {
        "filter": {
            "visibility": "ALL"
        },
        "last_id": "",
        "limit": 1000
    }
    response = requests.post('https://api-seller.ozon.ru/v2/product/list', headers=headers, json=data)
    response.raise_for_status()

    data_arr = []

    for item in response.json().get('result').get('items'):
        data_arr.append({
            "product_id": item.get('product_id'),
            "offer_id": item.get('offer_id'),
            "fbo_sku": get_fbo_sku(item.get('product_id')),
            "is_archived": item.get('archived')
        })

    return data_arr


# print(ozon_skus_ozon())
