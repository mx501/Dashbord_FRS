import Dashbord_obrabotka_Finrez as fn
import pandas as pd
import numpy as np
import openpyxl
from openpyxl.utils import get_column_letter
from openpyxl import Workbook
from openpyxl.styles import numbers
import os
import xlsxwriter
from datetime import datetime, timedelta, time



PUT = "D:\\Python\\Dashboard\\"
def to_exel(x, name):
    x.to_excel(PUT + "TEMP\\" + name + ".xlsx", index=False)
    workbook = openpyxl.load_workbook(PUT + "TEMP\\" + name + ".xlsx")
    worksheet = workbook.active
    worksheet.column_dimensions['A'].width = 28
    for col in worksheet.columns:
        if col[0].column != 1:
            col_letter = col[0].column_letter
            worksheet.column_dimensions[col_letter].width = 15
    workbook.save(PUT + "TEMP\\" + name + ".xlsx")

FINREZ = pd.read_csv(PUT + "RESULT\\" + "Финрез_Обработанный.csv", sep=";", encoding='ANSI', parse_dates=['дата'], dayfirst=True, low_memory=False)

FINREZ = FINREZ.loc[(FINREZ['статья'] == '2.3.1. Электроэнергия') &(FINREZ['канал'] == 'ФРС')]
FINREZ = FINREZ[["дата","магазин","значение_фрс"]]

FINREZ_2023 =FINREZ.copy()

FINREZ_2023 = FINREZ_2023[FINREZ_2023['дата'].dt.year == 2023]
FINREZ_2023['месяц'] = FINREZ_2023['дата'].dt.month
FINREZ_2023_max  = FINREZ_2023 ['месяц'].max()
ren_mes = {
    1: 'Январь_23',
    2: 'Февраль_23',
    3: 'Март_23',
    4: 'Апрель_23',
    5: 'Май_23',
    6: 'Июнь_23',
    7: 'Июль_23',
    8: 'Август_23',
    9: 'Сентябрь_23',
    10: 'Октябрь_23',
    11: 'Ноябрь_23',
    12: 'Декабрь_23'}
FINREZ_2023.loc[:, 'месяц название'] = FINREZ_2023['дата'].dt.month.replace(ren_mes)
FINREZ_2023 = FINREZ_2023.pivot(index=['дата','магазин','месяц'], columns='месяц название', values='значение_фрс')
FINREZ_2023 = FINREZ_2023.reset_index()
FINREZ_2023[["Февраль_23","Январь_23"]] = FINREZ_2023[["Февраль_23","Январь_23"]].fillna(0)
FINREZ_2023[["Февраль_23","Январь_23"]] = FINREZ_2023[["Февраль_23","Январь_23"]].replace(",", ".", regex=True)
FINREZ_2023[["Февраль_23","Январь_23"]] = FINREZ_2023[["Февраль_23","Январь_23"]].fillna(0)
FINREZ_2023[["Февраль_23","Январь_23"]] = FINREZ_2023[["Февраль_23","Январь_23"]].astype(float)
FINREZ_2023["2023"] = FINREZ_2023["Февраль_23"] + FINREZ_2023["Январь_23"]
FINREZ_2023 = FINREZ_2023.drop(columns={ "дата" })
# 22
FINREZ_2022 =FINREZ.copy()
FINREZ_2022["значение_фрс"] = FINREZ_2022["значение_фрс"].replace(to_replace="NaN", value=0)
FINREZ_2022["значение_фрс"] = FINREZ_2022["значение_фрс"].replace(to_replace=np.nan, value=0)
FINREZ_2022["значение_фрс"] = FINREZ_2022["значение_фрс"].fillna(0)

FINREZ_2022 = FINREZ_2022[FINREZ_2022['дата'].dt.year == 2022]
FINREZ_2022['месяц'] = FINREZ_2022['дата'].dt.month
FINREZ_2022 = FINREZ_2022.loc[FINREZ_2022["месяц"]<=FINREZ_2023_max]
ren_mes = {
    1: 'Январь_22',
    2: 'Февраль_22',
    3: 'Март_22',
    4: 'Апрель_22',
    5: 'Май_22',
    6: 'Июнь_22',
    7: 'Июль_22',
    8: 'Август_22',
    9: 'Сентябрь_22',
    10: 'Октябрь_22',
    11: 'Ноябрь_22',
    12: 'Декабрь_22'}
FINREZ_2022.loc[:, 'месяц название'] = FINREZ_2022['дата'].dt.month.replace(ren_mes)
FINREZ_2022 = FINREZ_2022.pivot(index=['дата','магазин','месяц'], columns='месяц название', values='значение_фрс')
FINREZ_2022 = FINREZ_2022.reset_index()

FINREZ_2022[["Февраль_22","Январь_22"]] = FINREZ_2022[["Февраль_22","Январь_22"]].replace(",", ".", regex=True)
FINREZ_2022[["Февраль_22","Январь_22"]] = FINREZ_2022[["Февраль_22","Январь_22"]].fillna(0)
FINREZ_2022[["Февраль_22","Январь_22"]] = FINREZ_2022[["Февраль_22","Январь_22"]].astype(float)
FINREZ_2022["2022"] = FINREZ_2022["Февраль_22"] + FINREZ_2022["Январь_22"]
FINREZ_2022 = FINREZ_2022.drop(columns={ "дата" })


Tesla = FINREZ_2022.merge(FINREZ_2023,
                        on=["магазин", "месяц"],
                        how="outer")

Tesla = Tesla.groupby("магазин")[["Февраль_22","Январь_22","2022","Февраль_23","Январь_23","2023"]].sum()
Tesla = Tesla.reset_index()

Tesla["Февраль 2023/2022"] = Tesla["Февраль_23"]-Tesla["Февраль_22"]
Tesla["Январь 2023/2022" ] = Tesla["Январь_23"]-Tesla["Январь_22"]

Tesla["Февраль 2023/2022 %"] = np.where(Tesla["Февраль_23"] == 0, 0,
                                         (Tesla["Февраль_23"] - Tesla["Февраль_22"]) / Tesla["Февраль_22"])

Tesla["Январь 2023/2022 %"] = np.where(Tesla["Январь_23"] == 0, 0,
                                         (Tesla["Январь_23"] - Tesla["Январь_22"]) / Tesla["Январь_22"])

Tesla["Февраль 2023/2022 %"] = Tesla["Февраль 2023/2022 %"].apply(lambda x: "{:.2%}".format(x))
Tesla["Январь 2023/2022 %"] = Tesla["Январь 2023/2022 %"].apply(lambda x: "{:.2%}".format(x))

Tesla.loc[Tesla["Февраль 2023/2022 %"] == "inf%" , "Февраль 2023/2022 %"]= 0
Tesla.loc[Tesla["Январь 2023/2022 %"] == "inf%" , "Январь 2023/2022 %"]= 0
Tesla = Tesla.loc[(Tesla["Февраль_23"] != 0 ) & (Tesla["Январь_23"] != 0) ]
Tesla = Tesla.loc[(Tesla["Февраль_22"] != 0 ) & (Tesla["Январь_22"] != 0) ]

Tesla= Tesla[["магазин","Январь_22","Январь_23","Январь 2023/2022", "Январь 2023/2022 %",  "Февраль_22","Февраль_23","Февраль 2023/2022", "Февраль 2023/2022 %", "2022","2023"]]


to_exel(x=Tesla, name="Электроэнергия")




print(Tesla)


