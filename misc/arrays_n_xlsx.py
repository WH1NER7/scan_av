import pandas as pd


def transpose_array(arr): # [[1, 2, 3], [1, 2, 3]] => [[1, 1], [2, 2], [3, 3]]
    result = [None] * len(arr[0])
    for col in range(len(arr[0])):
        result[col] = [None] * len(arr)
        for row in range(len(arr)):
            result[col][row] = arr[row][col]
    return result

def read_xlsx(excel_name, columns):  # читает эксель и возвращает numpy array. Columns в виде массива ['col_name','col_name']
    excel_data = pd.read_excel(excel_name, engine="openpyxl")
    data = pd.DataFrame(excel_data, columns=columns)
    arr = data.to_numpy()
    return arr


def columns_names(columns_count):
    columns = ['Склад', 'Номенклатура']
    columns_hours = []

    for num in range(0, columns_count):
        for i in ['Продажа', 'Возврат', 'Поставка']:
            columns_hours.append(f'{i} час {num}')

    columns += columns_hours
    return columns
