from pandas.tseries.offsets import DateOffset
from datetime import datetime, timedelta, time
from pandas.tseries.offsets import MonthBegin
import time
import os
import pandas as pd
from tqdm import tqdm
import sys
import math
import gc
# from memory_profiler import profile
import numpy as np

pd.set_option("expand_frame_repr", False)
pd.set_option('display.max_colwidth', None)

# region ПУТЬ ДОПАПКИ С ФАЙЛАМИ
PUT = "D:\\Python\\Dashboard\\"


# endregion

# region ПУТЬ ДОПАПКИ С ФАЙЛАМИ
'''обновить все данные'''
'''создать временные файлы финреза, продаж, минимальных дат'''
# endregion

class RENAME:
    def Rread(self):
        replacements = pd.read_excel("D:\\Python\\Dashboard\\DATA_2\\ДЛЯ ЗАМЕНЫ.xlsx",
                                     sheet_name="Лист1")
        rng = len(replacements)
        return rng, replacements
    '''блок переименования'''
    def HOZY(self):
        Spisania_HOZI = pd.read_csv("D:\\Python\\Dashboard\\SPISANIA_HOZI\\1.txt", sep="\t", encoding='utf-8', skiprows=8,
                                    names=("!МАГАЗИН!", "Номенклатура", "Сумма", "Сумма без НДС"))
        Spisania_HOZI = Spisania_HOZI["Номенклатура"].unique()
        return Spisania_HOZI
    '''блок хозы'''
"""чтение файлов для замены назани магазинов и базы номенклатуры хоз оваров"""

class DOC:
    def to(self, x, name):
        x.to_csv(PUT + "RESULT\\" + name, encoding="ANSI", sep=';',
                 index=False, decimal='.')
        return x
    def to_POWER_BI(self, x, name):
        x.to_csv(PUT + "RESULT\\" + name, encoding="ANSI", sep=';',
                 index=False, decimal=',')
    def to_ERROR(self, x, name):
        x.to_csv(PUT + "ERROR\\" + name, encoding="ANSI", sep=';',
                 index=False, decimal=',')
    def to_TEMP(self, x, name):
        x.to_csv(PUT + "TEMP\\" + name, encoding="ANSI", sep=';',
                 index=False, decimal='.')
    def to_exel(self,x, name):
        x.to_excel(PUT + "TEMP\\" + name, index=False)

"""функция сохранения файлов по папкам"""

class NEW:
    def Finrez(self):
        rng, replacements = RENAME().Rread()
        print(
            "Обновление финреза\n")
        for files in os.listdir(PUT + "DATA\\"):
            FINREZ = pd.read_excel(PUT + "DATA\\" + files, sheet_name="Динамика ТТ исходник")
            FINREZ = FINREZ.rename(columns={"Торговая точка": "!МАГАЗИН!", "Дата": "дата"})
            print("ФАЙЛ - ", files)
            for i in tqdm(range(rng), desc="(Финрез) 3. Переименование магазинов   --  ", ncols=120, colour="#F8C9CE"):
                FINREZ['!МАГАЗИН!'] = FINREZ['!МАГАЗИН!'].replace(replacements["НАЙТИ"][i], replacements["ЗАМЕНИТЬ"][i],
                                                                  regex=False)
            FINREZ = FINREZ.reset_index(drop=True)
            FINREZ = FINREZ.loc[FINREZ['дата'] >= "2021-01-01"]
            # region выбор столбцов в файле
            FINREZ = FINREZ[
                ["дата", "!МАГАЗИН!", "Режим налогообложения", "Канал", "Канал на последний закрытый период",
                 "Товарооборот (продажи) МКП, ед", "Товарооборот (продажи) МКП, руб с НДС",
                 "Товарооборот (продажи) КП, ед",
                 "Товарооборот (продажи) КП, руб с НДС", "Товарооборот (продажи) сопутка, ед",
                 "Товарооборот (продажи) сопутка, руб с НДС",
                 # ---Доход
                 "Выручка Итого, руб без НДС",
                 "Прочие доходы (субаренда), руб без НДС", "Прочие доходы (утилизация), руб без НДС",
                 "Доход от продажи ТМЦ, руб без НДС",
                 "Прочие доходы (паушальный взнос, услуги по открытию), руб без НДС", "Доход Штрафы, руб без НДС",
                 "Доход Аренда помещений, руб без НДС",
                 "Доход (аренда оборудования), руб без НДС", "Доход Роялти, руб без НДС",
                 "Доход комиссионное вознаграждение, руб без НДС",
                 "Доход Услуги по договору комиссии интернет-магазин, руб без НДС",
                 # ---Закуп
                 "* Закуп товара (МКП, КП, сопутка), руб без НДС",
                 # ---Затраты
                 "ОЕ - Общие Операционные расходы (сумма всех статей расходов), руб без НДС",

                 "2.1. ФОТ+Отчисления", "2.2. Аренда", "2.19. Бонусы программы лояльности",
                 "2.3.1. Электроэнергия", "2.3.2. Вывоз мусора, ЖБО, ТБО",
                 "2.3.3. Тепловая энергия",
                 "2.3.4. Водоснабжение",
                 "2.3.5. Водоотведение",
                 "2.3.6. Прочие коммунальные услуги (ФРС)",
                 "2.3.7. Газоснабжение",
                 "2.11. Маркетинговые расходы",
                 "2.9. Налоги",
                 "2.5.2. НЕУ",
                 "2.10. Питание сотрудников ",
                 "2.17. Распределяемая аналитика",
                 "2.18. Затраты службы развития",
                 "2.3.8. Охрана",
                 "2.4. Услуги банка",
                 "2.7. Прочие прямые затраты",
                 "2.7.1. Дезинфекционные средства",
                 "2.7.10. Услуги сотовой связи",
                 "2.7.2. Канцелярские товары",
                 "2.7.3. Командировочные расходы",
                 "2.7.4. Медицинские услуги, медикаменты, медосмотры",
                 "2.7.5. Расходы на аренду прочего имущества",
                 "2.7.6. Спецодежда, спецобувь, СИЗ",
                 "2.7.7. Транспортные услуги",
                 "2.7.8. Интернет",
                 "2.7.9. Услуги по дератизации, дезинсекции",
                 "2.16. Роялти",
                 "2.5.1. Списание потерь (до ноября 19г НЕУ + Списание потерь)",
                 "2.13. Инструменты/инвентарь",
                 "2.14. Ремонт и содержание зданий, оборудования",
                 "2.15.ТО оборудования (аутсорсинг)",
                 "2.6. Хозяйственные товары",
                 "2.8. ТМЦ ",
                 "Рентабельность, %",
                 "Прибыль (+) / Убыток (-) (= Т- ОЕ), руб без НДС",
                 "Точка безубыточности (МКП, КП, Сопутка), руб с НДС",
                 "Наценка Общая, руб без НДС",
                 "Наценка Общая, %",
                 "Наценка МКП и КП, руб с НДС",
                 "Наценка сопутка, руб с НДС",
                 "Наценка МКП и КП, %",
                 "Наценка сопутка, %",
                 ##
                 "Доля колбаса",
                 "Доля п/ф",
                 "Доля  гриль",
                 "Доля  Кости ливер отруба",
                 "Доля куриные п/ф",
                 "Доля субпродукты кур",
                 "Доля сопутка",
                 "Доля Калина малина",
                 "Доля зеленый магазин",
                 "Доля Волков Кофе",
                 "Доля \"Изготовлено по заказу\"",
                 "Доля Рыбные п/ф",
                 "Доля Продукция кулинарного цеха КХВ",
                 "Доля Пекарня",
                 ###"Временные столбцы удалить"
                 "1.1.Закуп товара (МКП и КП), руб с НДС",
                 "1.2.Закуп товара (сопутка), руб с НДС",
                 "Выручка Итого, руб с НДС"]]
            # endregion
            # сохранение временного файла с датами
            FINREZ_MAX = FINREZ[["дата"]]
            DOC().to_TEMP(x=FINREZ_MAX, name="MIN_MAX_FINREZ_DATE.csv")
            print("Сохранено - MIN_MAX_FINREZ_DATE.csv")
            # переименование обобщения
            FINREZ.loc[FINREZ['!МАГАЗИН!'] == "Офис", "Канал"] = "Офис"
            FINREZ.loc[FINREZ['!МАГАЗИН!'] == "Роялти ФРС", "Канал"] = "Роялти ФРС"
            FINREZ = FINREZ.reset_index(drop=True)
            # сохранение временного файла с каналами и режимом налогобложения
            FINREZ_MAX = FINREZ[["дата",'!МАГАЗИН!', 'Режим налогообложения','Канал','Канал на последний закрытый период']]
            DOC().to_TEMP(x=FINREZ_MAX, name="FINREZ_Nalog_Kanal.csv")
            # сохранение временного файла для дальнецшей обработки
            DOC().to_TEMP(x=FINREZ_MAX, name="MIN_MAX_FINREZ_DATE.csv")
            DOC().to_TEMP(x=FINREZ, name="FINREZ_DATE_TEMP.csv")
            print("Сохранено - FINREZ_Nalog_Kanal.csv")
            return FINREZ
    '''отвечает первоначальную обработку, сохранение временных файлов для вычисления минимальной и максимальной даты,
     сохраненние вреенного файла с каналати и режимом налогобложения'''
    def obnovlenie(self):
        print("ОБНОВЛЕНИЕ ПРОДАЖ........\n")
        rng, replacements =  RENAME().Rread()
        for rootdir, dirs, files in os.walk(PUT+ "NEW\\"):
            for file in files:
                if((file.split('.')[-1])=='txt'):
                    pyt_txt = os.path.join(rootdir, file)
                    read = pd.read_csv(pyt_txt, sep="\t", encoding='utf-8', skiprows=3, names=(
                    ['Склад магазин.Наименование', 'Номенклатура', 'По дням', 'Количество продаж', 'ВесПродаж',
                     'Себестоимость',
                     'Выручка', 'Прибыль', 'СписРуб', 'Списания, кг']))
                    for i in tqdm(range(rng), desc="Переименование тт продажи -" + file, ncols=120, colour="#F8C9CE" ):
                        read['Склад магазин.Наименование'] = read['Склад магазин.Наименование'].replace(replacements["НАЙТИ"][i], replacements["ЗАМЕНИТЬ"][i],
                                                                                                  regex=False)
                    read = read.loc[read['Склад магазин.Наименование'] != "Итого"]
                    read = read.reset_index(drop=True)
                    read.to_csv(PUT + "ПУТЬ ДО ФАЙЛОВ С НОВЫМИ ФАЙЛАМИ\\Текщий год\\" + file, encoding='utf-8', sep="\t",index=False)
                if ((file.split('.')[-1]) == 'xlsx'):
                        pyt_excel = os.path.join(rootdir, file)
                        read = pd.read_excel(pyt_excel, sheet_name="Sheet1")
                        for i in tqdm(range(rng), desc="Переименование тт чеки -" + file, ncols=120, colour="#F8C9CE", ):
                            read[
                            'Магазин'] = read['Магазин'].replace(replacements["НАЙТИ"][i], replacements["ЗАМЕНИТЬ"][i], regex=False)
                        read = read.reset_index(drop=True)
                        read.to_excel(PUT  + "NEW\\" + file,
                                    index=False)
                gc.enable()
    '''отвечает за загрузку и переименование новых данных продаж и чеков'''
    def nds_vir(self):
        rng, replacements = RENAME().Rread()
        print("Обновление данных выручки ндс\n")
        vir_NDS = pd.DataFrame()
        for rootdir, dirs, files in os.walk(PUT+"NDS\\"):
            for file in files:
                if ((file.split('.')[-1]) == 'txt'):
                    pyt_txt = os.path.join(rootdir, file)
                    vir_NDS_00 = pd.read_csv(pyt_txt, sep="\t", encoding='utf-8', skiprows=8, names=("!МАГАЗИН!", "ПРОДАЖИ С НДС", "ПРОДАЖИ БЕЗ НДС"))
                    for i in tqdm(range(rng), desc="Переименование тт выручка ндс -" + file, ncols=120, colour="#F8C9CE"):
                        vir_NDS_00["!МАГАЗИН!"] = vir_NDS_00["!МАГАЗИН!"].replace(replacements["НАЙТИ"][i], replacements["ЗАМЕНИТЬ"][i], regex=False)
                    date = file[0:len(file) - 4]
                    vir_NDS_00 = vir_NDS_00.loc[vir_NDS_00["!МАГАЗИН!"] != "Итого"]
                    vir_NDS_00["Дата"] = date
                    vir_NDS_00["Дата"] = pd.to_datetime(vir_NDS_00["Дата"], dayfirst=True)
                    vir_NDS = pd.concat([vir_NDS, vir_NDS_00], axis=0)
        Ren = ["ПРОДАЖИ С НДС", "ПРОДАЖИ БЕЗ НДС"]
        for r in Ren:
            vir_NDS[r] = vir_NDS[r].str.replace(',', '.')
            vir_NDS[r] = vir_NDS[r].str.replace(' ', '')
            vir_NDS[r] = vir_NDS[r].astype("float")
        vir_NDS["ставка выручка ндс"] = (vir_NDS["ПРОДАЖИ БЕЗ НДС"] / vir_NDS["ПРОДАЖИ С НДС"])
        vir_NDS["ПРОВЕРКАА"] = vir_NDS["ПРОДАЖИ С НДС"] * vir_NDS["ставка выручка ндс"]
        vir_NDS = vir_NDS.rename(columns={'Дата': 'дата'})
        gc.enable()
        return vir_NDS
    '''отвечает за загрузку данных для  расчета ставки выручки ндс'''
    def Spisania(self):
        rng, replacements = RENAME().Rread()
        print("Обновление данных списания без хозов ндс\n")
        Spisania = pd.DataFrame()
        for rootdir, dirs, files in os.walk(PUT + "NDS\\"):
            for file in files:
                if ((file.split('.')[-1]) == 'txt'):
                    pyt_txt = os.path.join(rootdir, file)
                    Spisania_00 = pd.read_csv(pyt_txt, sep="\t", encoding='utf-8', skiprows=8,
                                             names=("!МАГАЗИН!", "ПРОДАЖИ С НДС", "ПРОДАЖИ БЕЗ НДС"))
                    for i in tqdm(range(rng), desc="Переименование тт списания без хозов ндс -" + file, ncols=120, colour="#F8C9CE"):
                        Spisania_00["!МАГАЗИН!"] = Spisania_00["!МАГАЗИН!"].replace(replacements["НАЙТИ"][i],
                                                                                  replacements["ЗАМЕНИТЬ"][i],
                                                                                  regex=False)
                    date = file[0:len(file) - 4]
                    Spisania_00 = Spisania_00.loc[Spisania_00["!МАГАЗИН!"] != "Итого"]
                    Spisania_00["Дата"] = date
                    Spisania_00["Дата"] = pd.to_datetime(Spisania_00["Дата"], dayfirst=True)
                    Spisania = pd.concat([Spisania, Spisania_00], axis=0)
        Ren = ["ПРОДАЖИ С НДС", "ПРОДАЖИ БЕЗ НДС"]
        for r in Ren:
            Spisania[r] = Spisania[r].str.replace(',', '.')
            Spisania[r] = Spisania[r].str.replace(' ', '')
            Spisania[r] = Spisania[r].astype("float")
        Spisania["ставка списание без хозов ндс"] = (Spisania["ПРОДАЖИ БЕЗ НДС"] / Spisania["ПРОДАЖИ С НДС"])
        Spisania["ПРОВЕРКАА"] = Spisania["ПРОДАЖИ С НДС"] * Spisania["ставка списание без хозов ндс"]
        Spisania = Spisania.rename(columns={'Дата': 'дата'})
        gc.enable()
        return Spisania
    '''отвечает за загрузку данных для  расчета ставки списания без хозов ндс'''
    def Pitanie(self):
        rng, replacements = RENAME().Rread()
        print("Обновление данных питание персонала ндс\n")
        Pitanie = pd.DataFrame()
        for rootdir, dirs, files in os.walk(PUT + "PITANIE\\"):
            for file in files:
                if ((file.split('.')[-1]) == 'txt'):
                    pyt_txt = os.path.join(rootdir, file)
                    Pitanie_00 = pd.read_csv(pyt_txt, sep="\t", encoding='utf-8', skiprows=8,
                                              names=("!МАГАЗИН!", "ПРОДАЖИ С НДС", "ПРОДАЖИ БЕЗ НДС"))
                    for i in tqdm(range(rng), desc="Переименование тт списания без хозов ндс -" + file, ncols=120,
                                  colour="#F8C9CE"):
                        Pitanie_00["!МАГАЗИН!"] = Pitanie_00["!МАГАЗИН!"].replace(replacements["НАЙТИ"][i],
                                                                                    replacements["ЗАМЕНИТЬ"][i],
                                                                                    regex=False)
                    date = file[0:len(file) - 4]
                    Pitanie_00 = Pitanie_00.loc[Pitanie_00["!МАГАЗИН!"] != "Итого"]
                    Pitanie_00["Дата"] = date
                    Pitanie_00["Дата"] = pd.to_datetime(Pitanie_00["Дата"], dayfirst=True)
                    Pitanie = pd.concat([Pitanie, Pitanie_00], axis=0)
        Ren = ["ПРОДАЖИ С НДС", "ПРОДАЖИ БЕЗ НДС"]
        for r in Ren:
            Pitanie[r] = Pitanie[r].str.replace(',', '.')
            Pitanie[r] = Pitanie[r].str.replace(' ', '')
            Pitanie[r] = Pitanie[r].astype("float")
        Pitanie["питание ставка ндс"] = (Pitanie["ПРОДАЖИ БЕЗ НДС"] / Pitanie["ПРОДАЖИ С НДС"])
        Pitanie["ПРОВЕРКАА"] = Pitanie["ПРОДАЖИ С НДС"] * Pitanie["питание ставка ндс"]
        Pitanie = Pitanie.rename(columns={'Дата': 'дата'})
        gc.enable()
        return Pitanie
    '''отвечает за загрузку данных для  расчета ставки питание с ндс'''
    def Zakup(self):
        rng, replacements = RENAME().Rread()
        print("Обновление данных закуп ндс\n")
        Zakup = pd.DataFrame()
        for rootdir, dirs, files in os.walk(PUT + "ZAKUP_FIX\\"):
            for file in files:
                if ((file.split('.')[-1]) == 'csv'):
                    pyt_txt = os.path.join(rootdir, file)
                    Zakup_00 = pd.read_csv(pyt_txt, sep=";", encoding='ANSI', skiprows=1,
                                              names=("!МАГАЗИН!", "ПРОДАЖИ С НДС", "ПРОДАЖИ БЕЗ НДС",'ставка закуп ндс'))
                    for i in tqdm(range(rng), desc="Переименование тт списания без хозов ндс -" + file, ncols=120,
                                  colour="#F8C9CE"):
                        Zakup_00["!МАГАЗИН!"] = Zakup_00["!МАГАЗИН!"].replace(replacements["НАЙТИ"][i],
                                                                                    replacements["ЗАМЕНИТЬ"][i],
                                                                                    regex=False)
                    Zakup_00['ставка закуп ндс'] = Zakup_00['ставка закуп ндс'].str.replace(',', '.')
                    Zakup_00['ставка закуп ндс'] = Zakup_00['ставка закуп ндс'].str.replace(' ', '')
                    Zakup_00['ставка закуп ндс'] = Zakup_00['ставка закуп ндс'].astype("float")
                    date = file[0:len(file) - 4]
                    Zakup_00 = Zakup_00.loc[Zakup_00["!МАГАЗИН!"] != "Итого"]
                    Zakup_00["дата"] = date
                    Zakup_00["дата"] = pd.to_datetime(Zakup_00["дата"], dayfirst=True)
                    Zakup = pd.concat([Zakup, Zakup_00], axis=0)
                    print(Zakup)
                    gc.enable()
        return Zakup
    '''отвечает за загрузку данных для  расчета ставки питание с ндс'''
    def Nalog_Kanal(self):

        canal_nalg = pd.read_csv(PUT + "TEMP\\" + "FINREZ_Nalog_Kanal.csv",
                                 sep=";", encoding='ANSI', parse_dates=['дата'], dayfirst=True)
        print("получение списка каналов и редима налога")
        return (canal_nalg)
    '''отвечает за загрузку данных каналов и режима налога'''
    def Stavka_nds_Kanal(self):
        Zakup = NEW().Zakup()
        canal_nalog = NEW().Nalog_Kanal()
        pitanie = NEW().Pitanie()
        spisanie_not_hoz = NEW().Spisania()
        sales = NEW().nds_vir()
        print("формирование таблицы ставок ндс")

        # обьеденене ставок ндс
        sales = sales.drop(['ПРОДАЖИ С НДС', 'ПРОДАЖИ БЕЗ НДС','ПРОВЕРКАА'], axis=1)
        NDS = sales.merge(spisanie_not_hoz[["!МАГАЗИН!", "дата", "ставка списание без хозов ндс"]],
                                                  on=["!МАГАЗИН!", "дата"], how="left")
        NDS = NDS.merge(pitanie[["!МАГАЗИН!", "дата", "питание ставка ндс"]],
                           on=["!МАГАЗИН!", "дата"], how="left")
        NDS["хозы ставка ндс"] = 0.80

        NDS = NDS.merge(Zakup[["!МАГАЗИН!", "дата", 'ставка закуп ндс']],
                        on=["!МАГАЗИН!", "дата"], how="left")

        # добавление режима налогобложения для установки ставки на упраенку 1'''
        canal_nalog_maxdate = canal_nalog["дата"].max()
        canal_nalog = canal_nalog.loc[canal_nalog['дата'] == canal_nalog_maxdate]
        NDS = NDS.merge(canal_nalog[["!МАГАЗИН!", 'Режим налогообложения','Канал','Канал на последний закрытый период']],
                           on=["!МАГАЗИН!"], how="outer")
        NDS.loc[NDS['Режим налогообложения']=="упрощенка", [ 'ставка выручка ндс','ставка списание без хозов ндс',"питание ставка ндс","хозы ставка ндс",'ставка закуп ндс']] =[1,1,1,1,1]

        # тестовый
        DOC().to_TEMP(x=NDS, name="FINREZ_Nalog_Kanal_test.csv")
        print("Сохранен - FINREZ_Nalog_Kanal_test.csv")
        return NDS
    '''отвечает за обьеденение ставок nds  в одну таблицу вычисление налога для упращенки'''
    def Hoz(self):
        HOZ = pd.read_csv("D:\\Python\\Dashboard\\SPISANIA_HOZI\\1.txt", sep="\t", encoding='utf-8',
                                    skiprows=8,
                                    names=("!МАГАЗИН!", "Номенклатура", "Сумма", "Сумма без НДС"))
        HOZ = HOZ["Номенклатура"].unique()
        print("получение списка номенклатуры хозов")
        return HOZ
    '''справочник хозы'''
    def STATYA(self):
        STATYA = pd.read_excel(PUT + "DATA_2\\" + "@СПРАВОЧНИК_СТАТЕЙ.xlsx",
                               sheet_name="STATYA_REDAKT")
        return STATYA
    '''справочник статей'''
"""функция за обновление данных разнос по папкам, формирование справочнх таблиц"""

class MIN_MAX:
    def max_FINREZ_Month(self):
        print("получение данных максимальный месяц")
        FINREZ = pd.read_csv(PUT + "TEMP\\" + "MIN_MAX_FINREZ_DATE.csv",
                         sep=";", encoding='ANSI', parse_dates=['дата'], dayfirst=True)
        FINREZ = FINREZ[["дата"]]
        FINREZ = FINREZ.reset_index(drop=True)
        FINREZ = FINREZ.loc[FINREZ['дата'] >= "2023-01-01"]
        FINREZ = FINREZ.reset_index(drop=True)
        FINREZ['Дата2'] = FINREZ['дата'].dt.month
        FINREZ_MAX_DATE = FINREZ['Дата2'].max()
        return FINREZ_MAX_DATE
    '''макс дата в формате номера месяца'''
    def max_FINREZ_DATA(self):
        print("получение данных максимальная дата")
        FINREZ = pd.read_csv(PUT + "TEMP\\" + "MIN_MAX_FINREZ_DATE.csv",
                             sep=";", encoding='ANSI', parse_dates=['дата'], dayfirst=True)
        FINREZ = FINREZ[["дата"]]
        FINREZ = FINREZ.reset_index(drop=True)
        FINREZ = FINREZ.loc[FINREZ['дата'] >= "2023-01-01"]
        FINREZ = FINREZ.reset_index(drop=True)
        FINREZ_MAX_DATE = FINREZ['дата'].max()
        return FINREZ_MAX_DATE
    '''макс дата в формате даты'''
"""содержит информацию о максимальных датах"""

class OBRABOTKA:
    def SALES_obrabotka(self):
        MIN_MAX().max_FINREZ_Month()
        PROD_SVOD = pd.DataFrame()
        print("ОБНОВЛЕНИЕ СВОДНОЙ ПРОДАЖ")
        start = PUT + "ПУТЬ ДО ФАЙЛОВ С НОВЫМИ ФАЙЛАМИ\\Текщий год\\"
        for rootdir, dirs, files in os.walk(start):
            for file in tqdm(files, desc="(Обработака новых данных) 3.Склеивание данных   --  ", ncols=120,
                             colour="#F8C9CE"):
                if ((file.split('.')[-1]) == 'txt'):
                    pyt_txt = os.path.join(rootdir, file)
                    PROD_SVOD_00 = pd.read_csv(pyt_txt, sep="\t", encoding='utf-8', parse_dates=['По дням'],
                                               dayfirst=True)
                    lg = ('Выручка', "Количество продаж", "ВесПродаж", "Прибыль", "СписРуб", "Себестоимость")
                    for e in lg:
                        PROD_SVOD_00[e] = PROD_SVOD_00[e].str.replace(" ", "")
                        PROD_SVOD_00[e] = PROD_SVOD_00[e].str.replace(",", ".")
                        PROD_SVOD_00[e] = PROD_SVOD_00[e].str.replace(" ", "")
                        PROD_SVOD_00[e] = PROD_SVOD_00[e].astype("float")
                        PROD_SVOD_00['Склад магазин.Наименование'] = PROD_SVOD_00['Склад магазин.Наименование'].astype(
                            "category")
                        PROD_SVOD_00['Номенклатура'] = PROD_SVOD_00['Номенклатура'].astype("str")
                        PODAROK = ("Подарочная карта КМ 500р+ конверт", "Подарочная карта КМ 1000р+ конверт",
                                   "подарочная карта КМ 500 НОВАЯ",
                                   "подарочная карта КМ 1000 НОВАЯ")
                        for x in PODAROK:
                            PROD_SVOD_00 = PROD_SVOD_00.loc[PROD_SVOD_00['Номенклатура'] != x]
                    PROD_SVOD = pd.concat([PROD_SVOD, PROD_SVOD_00], axis=0)
                gc.enable()
                PROD_SVOD_00 = pd.DataFrame()
        # Создание столбцов Списания хозы и списания без хозов
        Hoz = NEW().Hoz()
        mask = PROD_SVOD['Номенклатура'].isin(Hoz)
        PROD_SVOD.loc[mask, 'СписРуб_ХОЗЫ'] = PROD_SVOD.loc[mask, 'СписРуб']
        PROD_SVOD.loc[mask, 'СписРуб'] = np.nan
        PROD_SVOD['СписРуб_ХОЗЫ'] = PROD_SVOD['СписРуб_ХОЗЫ'].astype("float")
        PROD_SVOD['СписРуб'] = PROD_SVOD['СписРуб'].astype("float")
        # region ГРУППИРОВКА ТАБЛИЦЫ(Без номенклатуры по дням)
        PROD_SVOD = PROD_SVOD.rename(
            columns={"По дням": "ДАТА", 'Выручка': "Выручка Итого, руб с НДС",
                     'Склад магазин.Наименование': "!МАГАЗИН!",
                     'СписРуб': "2.5.1. Списание потерь (до ноября 19г НЕУ + Списание потерь)",
                     "СписРуб_ХОЗЫ": "2.6. Хозяйственные товары",
                     "Себестоимость": "Закуп товара (МКП, КП, сопутка), руб c НДС",
                     "Прибыль": "Наценка Общая, руб"})

        PROD_SVOD = PROD_SVOD.groupby(["ДАТА", "!МАГАЗИН!"], as_index=False) \
            .aggregate({"Выручка Итого, руб с НДС": "sum", "Количество продаж": "sum", "ВесПродаж": "sum",
                        "Наценка Общая, руб": "sum",
                        "2.5.1. Списание потерь (до ноября 19г НЕУ + Списание потерь)": "sum",
                        "2.6. Хозяйственные товары": "sum", "Закуп товара (МКП, КП, сопутка), руб c НДС": "sum"}) \
            .sort_values("Выручка Итого, руб с НДС", ascending=False)
        # endregion
        # region ФИЛЬТРАЦИЯ ТАБЛИЦЫ > МАКС ДАТЫ КАЛЕНДАРЯ И выручка > 0
        PROD_SVOD = PROD_SVOD.loc[PROD_SVOD["Выручка Итого, руб с НДС"] > 0]
        PROD_SVOD["Месяц"] = PROD_SVOD["ДАТА"]
        PROD_SVOD.loc[~PROD_SVOD["Месяц"].dt.is_month_start, "Месяц"] = PROD_SVOD["Месяц"] - MonthBegin()
        PROD_SVOD["НОМЕР МЕСЯЦА"] = PROD_SVOD["ДАТА"].dt.month
        PROD_SVOD = PROD_SVOD.loc[PROD_SVOD["НОМЕР МЕСЯЦА"] > MIN_MAX().max_FINREZ_Month()]
        PROD_SVOD = PROD_SVOD.reset_index(drop=True)
        # endregion
        # region ГРУПИРОВКА ПО МЕСЯЦАМ
        PROD_SVOD = PROD_SVOD.groupby(["Месяц", "!МАГАЗИН!"], as_index=False) \
            .aggregate(
            {"ДАТА": "nunique", "Выручка Итого, руб с НДС": "sum", "Количество продаж": "sum", "ВесПродаж": "sum",
             "Наценка Общая, руб": "sum",
             "2.5.1. Списание потерь (до ноября 19г НЕУ + Списание потерь)": "sum", "2.6. Хозяйственные товары": "sum",
             "Закуп товара (МКП, КП, сопутка), руб c НДС": "sum"}) \
            .sort_values("!МАГАЗИН!", ascending=False)

        PROD_SVOD = PROD_SVOD.rename(
            columns={'Склад магазин.Наименование': "!МАГАЗИН!", 'ДАТА': "Факт отработанных дней"})
        PROD_SVOD = PROD_SVOD.rename(columns={'Месяц': 'дата'})
        print(PROD_SVOD)
        # endregion
        # redion добавление ставки ндс вычисление выручки без ндс
        nds = NEW().Stavka_nds_Kanal()
        PROD_SVOD = PROD_SVOD.merge(nds, on=["дата", "!МАГАЗИН!"], how="left")
        PROD_SVOD["Выручка Итого, руб без НДС"] = PROD_SVOD["Выручка Итого, руб с НДС"] * PROD_SVOD["ставка выручка ндс"]
        PROD_SVOD["Закуп товара (МКП, КП, сопутка), руб без НДС"] = PROD_SVOD["Закуп товара (МКП, КП, сопутка), руб c НДС"] *  PROD_SVOD['ставка закуп ндс']
        PROD_SVOD["2.5.1. Списание потерь (до ноября 19г НЕУ + Списание потерь)"] = PROD_SVOD["2.5.1. Списание потерь (до ноября 19г НЕУ + Списание потерь)"]* PROD_SVOD['ставка списание без хозов ндс']
        PROD_SVOD['2.5.2. НЕУ'] = PROD_SVOD["2.5.1. Списание потерь (до ноября 19г НЕУ + Списание потерь)"] * 0.15
        PROD_SVOD["2.6. Хозяйственные товары"] = PROD_SVOD["2.6. Хозяйственные товары"] * PROD_SVOD["хозы ставка ндс"]
        PROD_SVOD = PROD_SVOD.reset_index(drop=True)
        # endregion

        DOC().to_TEMP(x=PROD_SVOD, name="PROD_SVOD_TEMP.csv")
        return PROD_SVOD
    """обработка пути продаж формирование, групировка таблиц"""
    def Sales_prognoz(self):
        PROD_SVOD = pd.read_csv(PUT + "TEMP\\" + "PROD_SVOD_TEMP.csv",
                                sep=";", encoding='ANSI', parse_dates=['дата'], dayfirst=True)
        print("расчет прогнлза продаж")
        # region ДОБАВЛЕНИЕ ДАННЫХ КАЛЕНДАРЯ
        Calendar = pd.read_excel(PUT + "DATA_2\\Календарь.xlsx", sheet_name="Query1")
        Calendar.loc[~Calendar["дата"].dt.is_month_start, "дата"] = Calendar["дата"] - MonthBegin()
        Calendar = Calendar.groupby(["ГОД", "НОМЕР МЕСЯЦА", "дата"], as_index=False) \
            .aggregate({'ДНЕЙ В МЕСЯЦЕ': "max"}) \
            .sort_values("ГОД", ascending=False)
        PROD_SVOD = PROD_SVOD.rename(columns={'Склад магазин.Наименование': "!МАГАЗИН!"})
        PROD_SVOD = PROD_SVOD.rename(columns={'Месяц': 'дата'})
        PROD_SVOD = PROD_SVOD.merge(Calendar, on=["дата"], how="left")
        PROD_SVOD["Осталось дней продаж"] = PROD_SVOD["ДНЕЙ В МЕСЯЦЕ"] - PROD_SVOD["Факт отработанных дней"]
        dd = PROD_SVOD.groupby('дата')['Осталось дней продаж'].aggregate('min')
        PROD_SVOD = PROD_SVOD.merge(dd, on=["дата"], how="left")
        PROD_SVOD.loc[
            PROD_SVOD["Осталось дней продаж_x"] > PROD_SVOD["Осталось дней продаж_y"], 'Осталось дней продаж_x'] = \
        PROD_SVOD["Осталось дней продаж_y"]
        PROD_SVOD = PROD_SVOD.drop(columns={"Осталось дней продаж_y", "НОМЕР МЕСЯЦА", "ГОД"})
        PROD_SVOD = PROD_SVOD.rename(columns={'Осталось дней продаж_x': "Осталось дней продаж"})

        # region ДОБАВЛЕНИЕ КАНАЛОВ ОБОБЩАЮЩИХ В ТАБЛИЦУ ПРОДАЖ
        #canal = pd.read_excel(PUT + "DATA_2\\" + "Каналы.xlsx", sheet_name="Лист1")
        #canal["дата"] = canal["дата"].astype("datetime64[ns]")
        #PROD_SVOD = pd.concat([PROD_SVOD, canal], axis=0)
        #PROD_SVOD = PROD_SVOD.reset_index(drop=True)
        # endregion

        # region РАЗВОРОТ ТАБЛИЦЫ ПРОДАЖ
        PROD_SVOD = PROD_SVOD.drop(columns={"ставка выручка ндс", "ставка списание без хозов ндс", "питание ставка ндс","хозы ставка ндс","ставка закуп ндс",})
        PROD_SVOD = PROD_SVOD.melt(
            id_vars=["дата", "!МАГАЗИН!", "ДНЕЙ В МЕСЯЦЕ", "Осталось дней продаж", "Факт отработанных дней","Режим налогообложения","Канал","Канал на последний закрытый период"])
        PROD_SVOD = PROD_SVOD.rename(columns={"variable": "cтатья", "value": "значение"})
        # endregion
        PROD_SVOD["значение"] = PROD_SVOD["значение"].astype("float")
        PROD_SVOD["Факт отработанных дней"] = PROD_SVOD["Факт отработанных дней"].astype("float")
        # region добавление прогноза
        PROD_SVOD["значение"] = ((PROD_SVOD["значение"] / PROD_SVOD["Факт отработанных дней"]) * PROD_SVOD[
            "Осталось дней продаж"]) + PROD_SVOD["значение"]
        PROD_SVOD["значение"] = PROD_SVOD["значение"].round(2)
        # endregion
        PROD_SVOD_00 = PROD_SVOD.groupby(["!МАГАЗИН!", "дата"])['Канал'].nunique().reset_index()
        PROD_SVOD_00 = PROD_SVOD_00.rename(columns={'Канал': 'Канал_кол'})
        PROD_SVOD = pd.merge(PROD_SVOD, PROD_SVOD_00[['!МАГАЗИН!', 'дата', 'Канал_кол']], on=['!МАГАЗИН!', 'дата'], how='left')

        sp  = ["Выручка Итого, руб без НДС", "Закуп товара (МКП, КП, сопутка), руб без НДС", "2.5.1. Списание потерь (до ноября 19г НЕУ + Списание потерь)", "2.5.2. НЕУ","2.6. Хозяйственные товары"]
        for i in sp:
            PROD_SVOD.loc[(PROD_SVOD["Канал"] == "ФРС") & (
                    PROD_SVOD['Канал_кол'] == 2) & (PROD_SVOD["cтатья"] == i), "значение" ] = 0




        DOC().to_TEMP(x=PROD_SVOD, name="PROD_SVOD_PROGNOZ_TEMP.csv")
        return PROD_SVOD
    """функция за обработку данных"""
    def Finrez_fakt(self):
        print("Расчет фактичисих данных финреза")
        FINREZ_FAKT = pd.read_csv(PUT + "TEMP\\" + "FINREZ_DATE_TEMP.csv",
                             sep=";", encoding='ANSI', parse_dates=['дата'], dayfirst=True)

        FINREZ_FAKT["Закуп товара общий, руб с НДС"] = FINREZ_FAKT["1.1.Закуп товара (МКП и КП), руб с НДС"] + \
                                                       FINREZ_FAKT["1.2.Закуп товара (сопутка), руб с НДС"]
        FINREZ_FAKT.loc[(FINREZ_FAKT["Канал"] == "ФРС") & (FINREZ_FAKT["Режим налогообложения"] == "упрощенка"),
        "* Закуп товара (МКП, КП, сопутка), руб без НДС"] = FINREZ_FAKT["Закуп товара общий, руб с НДС"]

        # разворот таблицы фнреза
        FINREZ_FAKT = FINREZ_FAKT.melt(
            id_vars=["дата", "!МАГАЗИН!", "Режим налогообложения", "Канал", "Канал на последний закрытый период"])
        FINREZ_FAKT = FINREZ_FAKT.rename(columns={"variable": "cтатья", "value": "значение"})
        # очистка от мусора
        FINREZ_FAKT['значение'] = FINREZ_FAKT['значение'].astype("str")
        FINREZ_FAKT['значение'] = FINREZ_FAKT['значение'].str.replace(" ", "")
        FINREZ_FAKT['значение'] = np.where((FINREZ_FAKT['значение'] == 0), "nan", FINREZ_FAKT['значение'])
        FINREZ_FAKT['значение'] = np.where((FINREZ_FAKT['значение'] == "-"), "nan", FINREZ_FAKT['значение'])
        FINREZ_FAKT['значение'] = np.where((FINREZ_FAKT['значение'] == "#ДЕЛ/0!"), "nan", FINREZ_FAKT['значение'])
        FINREZ_FAKT['значение'] = np.where((FINREZ_FAKT['значение'] == "#ЗНАЧ!"), "nan", FINREZ_FAKT['значение'])
        FINREZ_FAKT['значение'] = FINREZ_FAKT['значение'].str.replace(",", ".")
        FINREZ_FAKT = FINREZ_FAKT.loc[(FINREZ_FAKT['значение'] != "nan")]
        FINREZ_FAKT['значение'] = FINREZ_FAKT['значение'].astype("float")
        FINREZ_FAKT = FINREZ_FAKT.loc[(FINREZ_FAKT['значение'] != 0)]

        FINREZ_FAKT.loc[
            FINREZ_FAKT[
                "cтатья"] == "* Закуп товара (МКП, КП, сопутка), руб без НДС", "cтатья"] = "Закуп товара (МКП, КП, сопутка), руб без НДС"


        # endregion

        return FINREZ_FAKT
    """Прошедшие периоды"""
    def Finrez_averge(self):
        print("расчет средних значений финреза")
        date_max = MIN_MAX().max_FINREZ_DATA()
        FINREZ = pd.read_csv(PUT + "TEMP\\" + "FINREZ_DATE_TEMP.csv",
                                sep=";", encoding='ANSI', parse_dates=['дата'], dayfirst=True)
        # region фильтрация таблицы, очистка от мусора
        FINREZ = FINREZ[
            ["дата", "!МАГАЗИН!", "Режим налогообложения", "Канал", "Канал на последний закрытый период",
             # ---Доход
             #"Выручка Итого, руб без НДС",
             "Прочие доходы (субаренда), руб без НДС",
             "Прочие доходы (утилизация), руб без НДС",
             "Доход от продажи ТМЦ, руб без НДС",
             "Прочие доходы (паушальный взнос, услуги по открытию), руб без НДС",
             "Доход Штрафы, руб без НДС",
             "Доход Аренда помещений, руб без НДС",
             "Доход (аренда оборудования), руб без НДС", "Доход Роялти, руб без НДС",
             "Доход комиссионное вознаграждение, руб без НДС",
             "Доход Услуги по договору комиссии интернет-магазин, руб без НДС",
             # ---Закуп
             #"* Закуп товара (МКП, КП, сопутка), руб без НДС",
             # ---Затраты
             #"ОЕ - Общие Операционные расходы (сумма всех статей расходов), руб без НДС",

             "2.1. ФОТ+Отчисления",
             "2.2. Аренда",
             "2.19. Бонусы программы лояльности",
             "2.3.1. Электроэнергия",
             "2.3.2. Вывоз мусора, ЖБО, ТБО",
             "2.3.3. Тепловая энергия",
             "2.3.4. Водоснабжение",
             "2.3.5. Водоотведение",
             "2.3.6. Прочие коммунальные услуги (ФРС)",
             "2.3.7. Газоснабжение",
             "2.11. Маркетинговые расходы",
             "2.9. Налоги",
             #"2.5.2. НЕУ",
             #"2.10. Питание сотрудников ",
             "2.17. Распределяемая аналитика",
             "2.18. Затраты службы развития",
             "2.3.8. Охрана",
             "2.4. Услуги банка",
             "2.7. Прочие прямые затраты",
             "2.7.1. Дезинфекционные средства",
             "2.7.10. Услуги сотовой связи",
             "2.7.2. Канцелярские товары",
             "2.7.3. Командировочные расходы",
             "2.7.4. Медицинские услуги, медикаменты, медосмотры",
             "2.7.5. Расходы на аренду прочего имущества",
             "2.7.6. Спецодежда, спецобувь, СИЗ",
             "2.7.7. Транспортные услуги",
             "2.7.8. Интернет",
             "2.7.9. Услуги по дератизации, дезинсекции",
             "2.16. Роялти",
             #"2.5.1. Списание потерь (до ноября 19г НЕУ + Списание потерь)",
             "2.13. Инструменты/инвентарь",
             "2.14. Ремонт и содержание зданий, оборудования",
             "2.15.ТО оборудования (аутсорсинг)",
             "2.6. Хозяйственные товары",
             #"2.8. ТМЦ ",
             #"Рентабельность, %",
             #"Прибыль (+) / Убыток (-) (= Т- ОЕ), руб без НДС",
             #"Точка безубыточности (МКП, КП, Сопутка), руб с НДС",
             ##
             ###"Временные столбцы удалить"
             ]]
        # разворот таблицы фнреза
        FINREZ = FINREZ.melt(
            id_vars=["дата", "!МАГАЗИН!", "Режим налогообложения", "Канал", "Канал на последний закрытый период"])
        FINREZ = FINREZ.rename(columns={"variable": "cтатья", "value": "значение"})
        # очистка от мусора
        FINREZ['значение'] = FINREZ['значение'].astype("str")
        FINREZ['значение'] = FINREZ['значение'].str.replace(" ", "")
        FINREZ['значение'] = np.where((FINREZ['значение'] == 0), "nan", FINREZ['значение'])
        FINREZ['значение'] = np.where((FINREZ['значение'] == "-"), "nan", FINREZ['значение'])
        FINREZ['значение'] = np.where((FINREZ['значение'] == "#ДЕЛ/0!"), "nan", FINREZ['значение'])
        FINREZ['значение'] = np.where((FINREZ['значение'] == "#ЗНАЧ!"), "nan", FINREZ['значение'])
        FINREZ['значение'] = FINREZ['значение'].str.replace(",", ".")
        FINREZ = FINREZ.loc[(FINREZ['значение'] != "nan")]
        FINREZ['значение'] = FINREZ['значение'].astype("float")
        FINREZ = FINREZ.loc[(FINREZ['значение'] != 0)]
        # endregion
        """группировка продаж за последние 3 месяца"""

        FINREZ['дата'] = pd.to_datetime(FINREZ['дата'])
        FINREZ.set_index('дата', inplace=True)

        # выделение последних трех месяцев
        FINREZ = FINREZ[
            FINREZ.index >= FINREZ.index.max() - pd.DateOffset(months=2)]
        FINREZ = FINREZ.groupby(['cтатья', '!МАГАЗИН!', 'Канал на последний закрытый период', 'Канал',"Режим налогообложения"])[
            ['значение']].sum().reset_index()
        FINREZ['значение'] = FINREZ['значение']/3
        FINREZ = FINREZ.reset_index(drop=True)

        FINREZ = FINREZ.reset_index()

        # Добавляем дату в датафрейм + 1 месяц
        FINREZ_01 = FINREZ.copy()
        FINREZ_01["дата"] = date_max + DateOffset(months=1)

        # Добавляем дату в датафрейм + 2 месяца
        FINREZ_02 = FINREZ.copy()
        FINREZ_02["дата"] = date_max + DateOffset(months=2)

        # оьеденяем датафреймы
        FINREZ = pd.concat([FINREZ_01, FINREZ_02], axis=0)
        FINREZ = FINREZ.drop('index', axis=1)
        FINREZ = FINREZ.reset_index(drop=True)

        # endregion
        DOC().to_TEMP(x=FINREZ, name="Средние продажи вычесленные.csv")
        return FINREZ
    """средние значения будующих дат"""
    def Sliyanie(self):
        FINREZ_FAKT =OBRABOTKA().Finrez_fakt()
        PROD_SVOD = pd.read_csv(PUT + "TEMP\\" + "PROD_SVOD_PROGNOZ_TEMP.csv",
                                sep=";", encoding='ANSI', parse_dates=['дата'], dayfirst=True)
        FINREZ = OBRABOTKA().Finrez_averge()
        print("Слияние таблиц")
        FINREZ = pd.concat([FINREZ, PROD_SVOD, FINREZ_FAKT], axis=0)
        # удалене итоговых значений
        # исключение новый магазин
        FINREZ.loc[FINREZ['!МАГАЗИН!'] == "Красноярск, ул.Чернышевского, 79" , ['Канал на последний закрытый период','Канал','Режим налогообложения']] = ['упрощенка','Франшиза внешняя','Франшиза внешняя']
        FINREZ = FINREZ.loc[FINREZ["Режим налогообложения"].notnull()]
        # region добавление справочника сатей
        STATYA = NEW().STATYA()
        FINREZ = FINREZ.merge(STATYA[["cтатья", "фрс_расчет среднего",
                                      "фр_расчет чистой прибыли", "подгруппа", "группа", "фрс_расчет чистой прибыли"]],
                              on=["cтатья"], how="left")
        FINREZ["каскад"] = FINREZ["значение"]
        # endregion
        # region замена положительных на отрицательные
        FINREZ.loc[FINREZ["группа"] == "Расход", "каскад"] = -FINREZ["значение"]
        FINREZ.loc[FINREZ["группа"] == "Закуп", "каскад"] = -FINREZ["значение"]
        # endregion
        # region ФРАНШИЗА
        FINREZ_FRANSHIZA = FINREZ.loc[(FINREZ["Канал"] == "Франшиза в аренду") | (FINREZ["Канал"] == "Франшиза внешняя")]
        FINREZ_FRANSHIZA = FINREZ_FRANSHIZA.loc[(FINREZ_FRANSHIZA["фр_расчет чистой прибыли"] == "да")]
        FINREZ_FRANSHIZA = FINREZ_FRANSHIZA.loc[(FINREZ_FRANSHIZA["cтатья"] != "2.5.1. Списание потерь (до ноября 19г НЕУ + Списание потерь)")]
        FINREZ_FRANSHIZA = FINREZ_FRANSHIZA.loc[(FINREZ_FRANSHIZA["cтатья"] != "2.5.2. НЕУ")]
        FINREZ_FRANSHIZA = FINREZ_FRANSHIZA.loc[(FINREZ_FRANSHIZA["cтатья"] != "2.6. Хозяйственные товары")]
        # добавление чистой прибыли
        grouped = FINREZ_FRANSHIZA.groupby(['!МАГАЗИН!', 'дата', 'Канал',"Канал на последний закрытый период","Режим налогообложения"])
        sums = grouped['каскад'].agg('sum')
        new_row = pd.DataFrame({
            '!МАГАЗИН!': sums.index.get_level_values('!МАГАЗИН!'),
            'дата': sums.index.get_level_values('дата'),
            "Канал на последний закрытый период": sums.index.get_level_values("Канал на последний закрытый период"),
            "Режим налогообложения": sums.index.get_level_values("Режим налогообложения"),
            'Канал': sums.index.get_level_values('Канал'),
            "cтатья": 'чистая прибыль',
            'значение': sums.values,
            'каскад': sums.values})
        FINREZ_FRANSHIZA = pd.concat([FINREZ_FRANSHIZA, new_row], axis=0)
        # region ERROR ФР
        FINREZ_FAKT.loc[
            FINREZ_FAKT["cтатья"] == "Прибыль (+) / Убыток (-) (= Т- ОЕ), руб без НДС", "cтатья"] = 'чистая прибыль'
        FINREZ_ERROR = FINREZ_FAKT.loc[FINREZ_FAKT["cтатья"] == 'чистая прибыль'].copy()
        FINREZ_ERROR = FINREZ_ERROR.rename(columns={"значение": "значение из итогов"})
        FINREZ_FRANSHIZA_00 = FINREZ_FRANSHIZA.copy()
        FINREZ_FRANSHIZA_00 = FINREZ_FRANSHIZA_00.loc[FINREZ_FRANSHIZA_00["cтатья"] == "чистая прибыль"]

        FINREZ_ERROR_FR = FINREZ_FRANSHIZA_00.merge(FINREZ_ERROR[["дата","значение из итогов","!МАГАЗИН!", "cтатья",'Канал']],
                                                  on=["cтатья","!МАГАЗИН!", "дата",'Канал'], how="left")
        FINREZ_ERROR_FR["расхождение"]  = FINREZ_ERROR_FR["значение"]-FINREZ_ERROR_FR["значение из итогов"]
        FINREZ_ERROR_FR = FINREZ_ERROR_FR.loc[
            (FINREZ_ERROR_FR["расхождение"] < -10) | (FINREZ_ERROR_FR["расхождение"] > 10)]

        # endregion
        # endregion
        # region ФРС
        FINREZ_FRS = FINREZ.loc[FINREZ["Канал"] == "ФРС"]
        FINREZ_FRS = FINREZ_FRS.loc[(FINREZ_FRS["фрс_расчет чистой прибыли"] == "да")]
        # region добавление чистой прибыли
        grouped = FINREZ_FRS.groupby(['!МАГАЗИН!', 'дата', 'Канал',"Канал на последний закрытый период","Режим налогообложения"])
        sums = grouped['каскад'].agg('sum')
        new_row = pd.DataFrame({
            '!МАГАЗИН!': sums.index.get_level_values('!МАГАЗИН!'),
            'дата': sums.index.get_level_values('дата'),
            "Канал на последний закрытый период": sums.index.get_level_values("Канал на последний закрытый период"),
            "Режим налогообложения": sums.index.get_level_values("Режим налогообложения"),
            'Канал': sums.index.get_level_values('Канал'),
            "cтатья": 'чистая прибыль',
            'значение': sums.values,
            'каскад': sums.values})
        FINREZ_FRS = pd.concat([FINREZ_FRS, new_row], axis=0)
        # endregion
        # region ФРС ERROR
        FINREZ_FRS_00 =  FINREZ_FRS.copy()
        FINREZ_FRS_00 =  FINREZ_FRS_00.loc[FINREZ_FRS_00["cтатья"] == "чистая прибыль"]
        FINREZ_ERROR_FRS = FINREZ_FRS_00.merge(
            FINREZ_ERROR[["дата", "значение из итогов", "!МАГАЗИН!", "cтатья", 'Канал']],
            on=["cтатья", "!МАГАЗИН!", "дата", 'Канал'], how="left")
        FINREZ_ERROR_FRS["расхождение"] = FINREZ_ERROR_FRS["значение"] - FINREZ_ERROR_FRS["значение из итогов"]
        FINREZ_ERROR_FRS = FINREZ_ERROR_FRS.loc[
            (FINREZ_ERROR_FRS["расхождение"] < -10) | (FINREZ_ERROR_FRS["расхождение"] > 10)]
        # endregion
        # region добавление общих итогов ФРС
        grouped = FINREZ_FRS.loc[FINREZ_FRS["cтатья"] != 'чистая прибыль']
        grouped = grouped.groupby(
            ['дата', 'Канал'])
        sums = grouped['каскад'].agg('sum')
        new_row = pd.DataFrame({
            'дата': sums.index.get_level_values('дата'),
            'Канал': sums.index.get_level_values('Канал'),
            "cтатья": 'итого ФРС',
            'значение': sums.values,
            'каскад': sums.values})
        FINREZ_FRS = pd.concat([FINREZ_FRS, new_row], axis=0)
        # endregion
        # endregion


        FINREZ = pd.concat([FINREZ_FRS, FINREZ_FRANSHIZA], axis=0)

        DOC().to_POWER_BI(x=FINREZ, name="FINREZ.csv")
        DOC().to_ERROR(x=FINREZ_ERROR_FRS, name="FINREZ_ERROR_FRS.csv")
        DOC().to_ERROR(x=FINREZ_ERROR_FR, name="FINREZ_ERROR_FR.csv")
        DOC().to_POWER_BI(x=FINREZ_FRS, name="FINREZ_FRS.csv")
        DOC().to_POWER_BI(x=FINREZ_FRANSHIZA, name="FINREZ_FRANSHIZA.csv")
        print("сохраненно")
        return

    """функция за обработку финреза, вычисление среднего только по входящим данным, без производных для дальнейшего склеивания с прогнозом продаж"""
"""блок обновления данных разнос файлов по папкам и сохранения временных файлов ставки ндс в папку TEMP"""







NEW().Finrez()
NEW().obnovlenie()
NEW().Stavka_nds_Kanal()
OBRABOTKA().SALES_obrabotka()
#OBRABOTKA(). Sales_prognoz()
#OBRABOTKA().sliyanie()

OBRABOTKA().Sales_prognoz()
OBRABOTKA().Sliyanie()