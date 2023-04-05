from pandas. tseries.offsets import DateOffset
from datetime import datetime,timedelta,time
from pandas.tseries.offsets import MonthBegin
import time
import os
import pandas as pd
from tqdm import tqdm
import sys
import math
import gc
#from memory_profiler import profile
import numpy as np
pd.set_option("expand_frame_repr", False)
pd.set_option('display.max_colwidth', None)
# region !!!!ДОАВИТЬ В ДАШБОРД!!!!!
"""Добавить столбцы со скидками  что бы сопоставлять списания с наценкой"""
"""Заменить азвания магазинов"""
# endregion

# region ПУТЬ ДОПАПКИ С ФАЙЛАМИ
PUT = "C:\\Users\\lebedevvv\\Desktop\\ДАШБОРД\\"
# endregion

class RENAME:
    """Переименования магазинов"""
    def Rread(self):

        replacements = pd.read_excel("C:\\Users\\lebedevvv\\Desktop\\Показатели ФРС\\СПРАВОЧНИК ТТ\\ДЛЯ ЗАМЕНЫ.xlsx",
                                     sheet_name="Лист1")
        rng = len(replacements)
        return rng, replacements


class NEW:
    def obnovlenie(self):
        print("1. ОБНОВЛЕНИЕ ПРОДАЖ........\n")
        rng, replacements =  RENAME().Rread()
        for rootdir, dirs, files in os.walk(PUT+ "NEW\\"):
            for file in files:
                if((file.split('.')[-1])=='txt'):
                    print("2. ФАЙЛ НАЙДЕН(ПРОДАЖИ)", file)
                    pyt_txt = os.path.join(rootdir, file)
                    read = pd.read_csv(pyt_txt, sep="\t", encoding='utf-8', skiprows=3, names=(
                    ['Склад магазин.Наименование', 'Номенклатура', 'По дням', 'Количество продаж', 'ВесПродаж',
                     'Себестоимость',
                     'Выручка', 'Прибыль', 'СписРуб', 'Списания, кг']))
                    for i in tqdm(range(rng), desc="(Обработака новых данных) 3. Переименование магазинов   --  ", ncols=120, colour="#F8C9CE" ):
                        read['Склад магазин.Наименование'] = read['Склад магазин.Наименование'].replace(replacements["НАЙТИ"][i], replacements["ЗАМЕНИТЬ"][i],
                                                                                                  regex=False)
                    read = read.loc[read['Склад магазин.Наименование'] != "Итого"]
                    read = read.reset_index(drop=True)
                    read.to_csv("C:\\Users\\lebedevvv\\Desktop\\Показатели ФРС\\Продажи, Списания, Прибыль\\Текщий год\\" + file, encoding='utf-8', sep="\t",index=False)
                if ((file.split('.')[-1]) == 'xlsx'):
                        print("2. ФАЙЛ НАЙДЕН(Чеков)", file)
                        pyt_excel = os.path.join(rootdir, file)
                        read = pd.read_excel(pyt_excel, sheet_name="Sheet1")
                        for i in tqdm(range(rng), desc="(Обработака новых данных) 3. Переименование магазинов   --  ", ncols=120, colour="#F8C9CE", ):
                            read[
                            'Магазин'] = read['Магазин'].replace(replacements["НАЙТИ"][i], replacements["ЗАМЕНИТЬ"][i], regex=False)
                        read = read.reset_index(drop=True)
                        read.to_excel("C:\\Users\lebedevvv\\Desktop\\Показатели ФРС\\ЧЕКИ\\2023\\" + file,
                                    index=False)
                        print(" НЕТ ФАЙЛОВ")
                        #return read


    def nds_vir(self):
        """ЗАгрузка новых данных для вычисления ставки ндс, обьеденнеие, проставляет даты"""
        rng, replacements = RENAME().Rread()
        print("1.ОБНОВЛЕНИЕ ДАННЫХ НДС........\n")
        vir_NDS = pd.DataFrame()
        for rootdir, dirs, files in os.walk(PUT+"NDS\\"):
            for file in files:
                if ((file.split('.')[-1]) == 'txt'):
                    print("2. ФАЙЛ НАЙДЕН ДАННЫХ НДС", file)
                    pyt_txt = os.path.join(rootdir, file)
                    vir_NDS_00 = pd.read_csv(pyt_txt, sep="\t", encoding='utf-8', skiprows=8, names=("!МАГАЗИН!", "ПРОДАЖИ С НДС", "ПРОДАЖИ БЕЗ НДС"))
                    for i in tqdm(range(rng), desc="3. ПЕРЕИМЕНОВНИЕ -- ", ncols=120, colour="#F8C9CE"):
                        vir_NDS_00["!МАГАЗИН!"] = vir_NDS_00["!МАГАЗИН!"].replace(replacements["НАЙТИ"][i], replacements["ЗАМЕНИТЬ"][i], regex=False)
                    date = file[0:len(file) - 4]
                    print("4. ОБРАБОТКА ФАЙЛА ДАННЫХ НДС.... - ", file)
                    vir_NDS_00 = vir_NDS_00.loc[vir_NDS_00["!МАГАЗИН!"] != "Итого"]
                    vir_NDS_00["Дата"] = date
                    vir_NDS_00["Дата"] = pd.to_datetime(vir_NDS_00["Дата"], dayfirst=True)
                    print("5. СОВМЕЩЕНИЕ ФАЙЛОВ.... ")

                    vir_NDS = pd.concat([vir_NDS, vir_NDS_00], axis=0)
        Ren = ["ПРОДАЖИ С НДС", "ПРОДАЖИ БЕЗ НДС"]
        for r in Ren:
            vir_NDS[r] = vir_NDS[r].str.replace(',', '.')
            vir_NDS[r] = vir_NDS[r].str.replace(' ', '')
            vir_NDS[r] = vir_NDS[r].astype("float")
        vir_NDS["Ставка НДС"] = (vir_NDS["ПРОДАЖИ БЕЗ НДС"] / vir_NDS["ПРОДАЖИ С НДС"])
        vir_NDS["ПРОВЕРКАА"] = vir_NDS["ПРОДАЖИ С НДС"] * vir_NDS["Ставка НДС"]
        vir_NDS = vir_NDS.rename(columns={'Дата': 'Месяц'})
        DOC().to(x=vir_NDS, name="TESTOVAYA2.csv")
        gc.enable()
        return vir_NDS
    def Spisania(self):
        """ЗАгрузка новых данных для вычисления ставки ндс, обьеденнеие, проставляет даты"""
        rng, replacements = RENAME().Rread()
        print("1.ОБНОВЛЕНИЕ ДАННЫХ НДС........\n")
        Spisania = pd.DataFrame()
        for rootdir, dirs, files in os.walk(PUT + "NDS\\"):
            for file in files:
                if ((file.split('.')[-1]) == 'txt'):
                    print("2. ФАЙЛ НАЙДЕН ДАННЫХ списаний", file)
                    pyt_txt = os.path.join(rootdir, file)
                    Spisania_00 = pd.read_csv(pyt_txt, sep="\t", encoding='utf-8', skiprows=8,
                                             names=("!МАГАЗИН!", "ПРОДАЖИ С НДС", "ПРОДАЖИ БЕЗ НДС"))
                    for i in tqdm(range(rng), desc="3. ПЕРЕИМЕНОВНИЕ -- ", ncols=120, colour="#F8C9CE"):
                        Spisania_00["!МАГАЗИН!"] = Spisania_00["!МАГАЗИН!"].replace(replacements["НАЙТИ"][i],
                                                                                  replacements["ЗАМЕНИТЬ"][i],
                                                                                  regex=False)
                    date = file[0:len(file) - 4]
                    print("4. ОБРАБОТКА ФАЙЛА ДАННЫХ НДС.... - ", file)
                    Spisania_00 = Spisania_00.loc[Spisania_00["!МАГАЗИН!"] != "Итого"]
                    Spisania_00["Дата"] = date
                    Spisania_00["Дата"] = pd.to_datetime(Spisania_00["Дата"], dayfirst=True)
                    print("5. СОВМЕЩЕНИЕ ФАЙЛОВ.... ")

                    Spisania = pd.concat([Spisania, Spisania_00], axis=0)
        Ren = ["ПРОДАЖИ С НДС", "ПРОДАЖИ БЕЗ НДС"]
        for r in Ren:
            Spisania[r] = Spisania[r].str.replace(',', '.')
            Spisania[r] = Spisania[r].str.replace(' ', '')
            Spisania[r] = Spisania[r].astype("float")
        Spisania["Ставка НДС"] = (Spisania["ПРОДАЖИ БЕЗ НДС"] / Spisania["ПРОДАЖИ С НДС"])
        Spisania["ПРОВЕРКАА"] = Spisania["ПРОДАЖИ С НДС"] * Spisania["Ставка НДС"]
        Spisania = Spisania.rename(columns={'Дата': 'Месяц'})
        DOC().to(x=Spisania, name="TESTOVAYA2.csv")
        gc.enable()
        return Spisania







class FINFEZ:
    """Обработка финреза"""
    def Finrez_obrabotka(self):
        rng, replacements = RENAME().Rread()
        global start_finrez, TEMP
        for rootkit, dirs, files in os.walk(PUT + "DATA\\"):
            for file in files:
                print("OНОВЛЕНИЕ ФИНРЕЗА")
                if ((file.split('.')[-1]) == 'xlsx'):
                    pyt_excel = os.path.join(rootkit, file)
                    FINREZ = pd.read_excel(pyt_excel, sheet_name="Динамика ТТ исходник")
                    FINREZ["!МАГАЗИН!"] = FINREZ["Торговая точка"]
                    for i in tqdm(range(rng), desc="(Финрез) 3. Переименование магазинов   --  ", ncols=120, colour="#F8C9CE"):
                        FINREZ['!МАГАЗИН!'] = FINREZ['!МАГАЗИН!'].replace(replacements["НАЙТИ"][i], replacements["ЗАМЕНИТЬ"][i], regex=False)
                    FINREZ = FINREZ.reset_index(drop=True)
                    FINREZ = FINREZ.loc[FINREZ['Дата'] >= "2021-01-01"]
                    FINREZ = FINREZ[["Дата", "!МАГАЗИН!", "Режим налогообложения", "Канал", "Канал на последний закрытый период",
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
                                     "Выручка Итого, руб с НДС",
                                     ]]
                    FINREZ.loc[FINREZ['!МАГАЗИН!'] == "Офис", "Канал"] = "Офис"
                    FINREZ.loc[FINREZ['!МАГАЗИН!'] == "Роялти ФРС", "Канал"] = "Роялти ФРС"
                    FINREZ = FINREZ.reset_index(drop=True)

                    # region ВЫЧИСЛЕНИЕ НАЦЕНКИ
                    print("ВЫЧИСЛЕНЕИ НАЦЕНКИ")
                    FINREZ["Закуп товара общий, руб с НДС"] = FINREZ["1.1.Закуп товара (МКП и КП), руб с НДС"] + FINREZ[
                        "1.2.Закуп товара (сопутка), руб с НДС"]
                    FINREZ.loc[FINREZ["Режим налогообложения"] == "упрощенка", "Закуп(режм налога)"] = FINREZ[
                        "Закуп товара общий, руб с НДС"]
                    FINREZ.loc[FINREZ["Режим налогообложения"] == "общий", "Закуп(режм налога)"] = FINREZ[
                        "* Закуп товара (МКП, КП, сопутка), руб без НДС"]
                    FINREZ.loc[FINREZ["Канал"] == "Итого Франшиза", "Закуп(режм налога)"] = FINREZ["Наценка Общая, %"]
                    FINREZ.loc[FINREZ["Канал"] == "Итого ФРС", "Закуп(режм налога)"] = FINREZ["Наценка Общая, %"]
                    FINREZ["Товарооборот КП + МКП, руб с НДС"] = FINREZ["Товарооборот (продажи) КП, руб с НДС"] + FINREZ[
                        "Товарооборот (продажи) МКП, руб с НДС"]
                    FINREZ["Товарооборот(Общий) с НДС"] = FINREZ["Товарооборот (продажи) КП, руб с НДС"] + FINREZ[
                        "Товарооборот (продажи) МКП, руб с НДС"] + FINREZ["Товарооборот (продажи) сопутка, руб с НДС"]
                    FINREZ["Наценка (Общий) с НДС"] = FINREZ["Наценка МКП и КП, руб с НДС"] + FINREZ["Наценка сопутка, руб с НДС"]
                    # endregion
                    # region ВЫЧИСЛЕНИЕ ДОЛИ
                    r = ("Доля Калина малина", "Доля Пекарня", "Доля Продукция кулинарного цеха КХВ", "Доля Рыбные п/ф",
                         "Доля \"Изготовлено по заказу\"",
                         "Доля Волков Кофе", "Доля зеленый магазин", "Доля сопутка", "Доля субпродукты кур", "Доля куриные п/ф",
                         "Доля  Кости ливер отруба", "Доля  гриль", "Доля п/ф", "Доля колбаса")
                    for Y in tqdm(r, desc="     Расчет", ncols=120, colour="#F8C9CE", ):
                        time.sleep(0.1)
                        FINREZ[Y] = FINREZ[Y] * FINREZ["Выручка Итого, руб с НДС"]
                    # endregion
                    # region РАЗВОРОТ ТАБЛИЦЫ
                    FINREZ = FINREZ.melt(
                        id_vars=["Дата", "!МАГАЗИН!", "Режим налогообложения", "Канал", "Канал на последний закрытый период"])
                    FINREZ = FINREZ.rename(columns={"variable": "Статья", "value": "Значение"})
                    # endregion
                    # region ЗАГРУЗКА КАНАЛОВ
                    STATYA = SPRAVOCHIK().spravcnik_STATYA()

                    FINREZ_AVERAGE = FINREZ.merge(STATYA[["Статья", "тип данных","расчет среднего для общего",
                                                          'расчет среднего для управщенки_франшиза',	"подгруппа",	"группа"]],
                                                  on=["Статья"], how="left")
                    FINREZ_AVERAGE["каскад"] = FINREZ_AVERAGE["Значение"]
                    FINREZ_AVERAGE.loc[FINREZ_AVERAGE["группа"] == "Расход", "каскад"] = -FINREZ_AVERAGE["Значение"]
                    print("Сохранен")
                    DOC().to(x=FINREZ_AVERAGE, name="St.csv")
                    # endregion
                    # region СПРАВОЧНИК МИН МАКС ФИНРЕЗ
                    print("СОХРАНЕНИЕ ФАЙЛА MIN_MAX_FINREZ.csv")
                    FINREZ['Дата'].to_csv(PUT + "TEMP\\" + "MIN_MAX_FINREZ.csv", encoding="ANSI", sep=';', index=False, decimal='.')
                    # endregion
                    # region УДАЛЕНИЕ МУСОРНЫХ СИМВОЛОВ
                    print("   УДАЛЕНИЕ МУСОРНЫХ СИМВОЛОВ")
                    # endregion
                    # region УДАЛЕНИЕ ОШИБОК В ФАЙЛЕ
                    # 2022-07-01
                    FINREZ.loc[(FINREZ['Статья'] == "Прибыль (+) / Убыток (-) (= Т- ОЕ), руб без НДС") & (FINREZ['Дата'] == "2022-07-01") & (
                            FINREZ["Канал"] == "Итого ФРС"), 'Значение'] = FINREZ['Значение'] - 90000
                    FINREZ.loc[(FINREZ['Статья'] == "Прибыль (+) / Убыток (-) (= Т- ОЕ), руб без НДС") & (FINREZ['Дата'] == "2022-07-01") & (
                            FINREZ["Канал"] == "ФРС+Франшиза"), 'Значение'] = FINREZ['Значение'] - 171925
                    FINREZ.loc[(FINREZ['Статья'] == "Прибыль (+) / Убыток (-) (= Т- ОЕ), руб без НДС") & (FINREZ['Дата'] == "2022-07-01") & (
                            FINREZ["Канал"] == "Итого Франшиза"), 'Значение'] = FINREZ['Значение'] - 81925
                    FINREZ.loc[
                        (FINREZ['Статья'] == "Доход Аренда помещений, руб без НДС") & (FINREZ['Дата'] == "2022-07-01") & (
                                FINREZ["Канал"] == "ФРС+Франшиза"), 'Значение'] = \
                        FINREZ['Значение'] - 81925
                    FINREZ.loc[(FINREZ['Статья'] == "Доход Аренда помещений, руб без НДС") & (FINREZ['Дата'] == "2022-07-01") & (
                            FINREZ["Канал"] == "Итого Франшиза"), 'Значение'] = FINREZ['Значение'] - 81925
                    FINREZ.loc[(FINREZ['Статья'] == "Доход Аренда помещений, руб без НДС") & (FINREZ['Дата'] == "2022-07-01") & (
                            FINREZ["Канал"] == "Розничная сеть"), 'Значение'] = FINREZ['Значение'] - 81925

                    FINREZ.loc[(FINREZ['Статья'] == "2.2. Аренда") & (FINREZ['Дата'] == "2022-07-01") & (FINREZ["Канал"] == "Итого ФРС"), 'Значение'] = \
                    FINREZ[
                        'Значение'] + 90000
                    FINREZ.loc[(FINREZ['Статья'] == "2.2. Аренда") & (FINREZ['Дата'] == "2022-07-01") & (FINREZ["Канал"] == "ФРС+Франшиза"), 'Значение'] = \
                        FINREZ[
                            'Значение'] + 90000
                    FINREZ.loc[(FINREZ['Статья'] == "Точка безубыточности (МКП, КП, Сопутка), руб с НДС") & (FINREZ['Дата'] == "2022-07-01") & (
                            FINREZ["Канал"] == "Итого ФРС"), 'Значение'] = FINREZ['Значение'] + 355810
                    FINREZ.loc[(FINREZ['Статья'] == "Точка безубыточности (МКП, КП, Сопутка), руб с НДС") & (FINREZ['Дата'] == "2022-07-01") & (
                            FINREZ["Канал"] == "Розничная сеть"), 'Значение'] = FINREZ['Значение'] + 355810
                    FINREZ.loc[(FINREZ['Статья'] == "Точка безубыточности (МКП, КП, Сопутка), руб с НДС") & (FINREZ['Дата'] == "2022-07-01") & (
                            FINREZ["Канал"] == "ФРС+Франшиза"), 'Значение'] = FINREZ['Значение'] + 355810
                    # 2022-08-01
                    # endregion
                    # region CРЕДНЕЕ ЗНАЧЕНИЕ
                    FINREZ_KANAL = pd.read_csv(PUT + "RESULT\\" + "STATYA.csv", encoding="ANSI", sep=';')
                    # region формирование таблицы каналов
                    FINREZ_AVERAGE = FINREZ.merge(FINREZ_KANAL[["Статья", "Расчет среднего"]],
                                                  on=["Статья"], how="left")
                    FINREZ_AVERAGE = FINREZ_AVERAGE.loc[FINREZ_AVERAGE["Расчет среднего"] == "да"]
                    FINREZ_AVERAGE_00 = FINREZ.loc[FINREZ["Доход/расход Расшифровка"] == "Прочий доход"]
                    FINREZ_AVERAGE = pd.concat([FINREZ_AVERAGE, FINREZ_AVERAGE_00], axis=0)
                    FINREZ_AVERAGE = FINREZ_AVERAGE.reset_index(drop=True)
                    # region ИСКЛЮЧЕНИЯ СТАТЕЙ ИЗ РАСЧЕТА СРЕДНЕГО
                    FINREZ_AVERAGE = FINREZ_AVERAGE.loc[FINREZ_AVERAGE["Статья"] != "Выручка Итого, руб без НДС"]

                    DOC().to(x=FINREZ_AVERAGE, name="TESTOVAYA.csv")
                    print(FINREZ_AVERAGE)
                    # endregion
                    # endregion
                    FINREZ_AVERAGE['1MTD'] = FINREZ_AVERAGE['Дата'] - DateOffset(months=1)
                    FINREZ_AVERAGE['2MTD'] = FINREZ_AVERAGE['Дата'] - DateOffset(months=2)
                    MTD0 = FINREZ_AVERAGE['Дата'].max()
                    MTD2 = FINREZ_AVERAGE['2MTD'].max()
                    FINREZ_AVERAGE = FINREZ_AVERAGE.loc[(FINREZ_AVERAGE['Дата'] >= MTD2) & (FINREZ_AVERAGE['Дата'] <= MTD0)]
                    FINREZ_AVERAGE = FINREZ_AVERAGE.drop(columns={'2MTD', '1MTD'})
                    FINREZ_AVERAGE['PLUS_1M'] = FINREZ_AVERAGE['Дата'] + DateOffset(months=1)
                    FINREZ_AVERAGE['PLUS_2M'] = FINREZ_AVERAGE['Дата'] + DateOffset(months=2)
                    FINREZ_AVERAGE['PLUS_3M'] = FINREZ_AVERAGE['Дата'] + DateOffset(months=3)
                    D1 = ((FINREZ_AVERAGE['PLUS_1M']).unique()).max()
                    D2 = ((FINREZ_AVERAGE['PLUS_2M']).unique()).max()
                    D3 = ((FINREZ_AVERAGE['PLUS_3M']).unique()).max()
                    # FINREZ_AVERAGE["Режим налогообложения"] = 0
                    # FINREZ_AVERAGE["Канал на последний закрытый период"] = 0
                    # print("(Финрез среднее) 8. Групировка таблицы")
                    aver_00 = FINREZ_AVERAGE.groupby(
                        ["!МАГАЗИН!", "Статья", "Канал", "Режим налогообложения", "Доход/расход", "Доход/расход Расшифровка",
                         "Канал на последний закрытый период"],
                        as_index=False) \
                        .aggregate({"Значение": "mean"})
                    aver_01 = aver_00
                    aver_02 = aver_00
                    aver_03 = aver_00

                    aver_01["Дата"] = D1
                    aver_01 = aver_01.reset_index(drop=True)
                    aver_02["Дата"] = D2
                    aver_02 = aver_02.reset_index(drop=True)
                    aver_03["Дата"] = D3
                    aver_03 = aver_03.reset_index(drop=True)
                    FINREZ_AVERAGE = pd.concat([aver_01, aver_02, aver_03], axis=0)
                    FINREZ_AVERAGE = FINREZ_AVERAGE.reset_index(drop=True)
                    FINREZ_AVERAGE["Значение"] = FINREZ_AVERAGE["Значение"].round(2)
                    DOC().to(x=FINREZ_AVERAGE, name="FINREZ_AVERAGE.csv")
                    # endregion
                    print("СОХРАНЕНИЕ СПРАВОЧНИКА СТАТЕЙ FINREZ_AVERAGE.csv")
                    gc.enable()
                    return FINREZ, STATYA, FINREZ_KANAL, FINREZ_AVERAGE

class SALES:
    def SALES_obrabotka(self):
        PROD_SVOD = pd.DataFrame()
        print("ОБНОВЛЕНИЕ СВОДНОЙ ПРОДАЖ")
        start = "C:\\Users\\lebedevvv\\Desktop\\Показатели ФРС\\Продажи, Списания, Прибыль\\Текщий год\\"
        for rootdir, dirs, files in os.walk(start):
            for file in tqdm(files, desc="(Обработака новых данных) 3.Склеивание данных   --  ", ncols=120, colour="#F8C9CE" ):
                if ((file.split('.')[-1]) == 'txt'):
                    pyt_txt = os.path.join(rootdir, file)
                    PROD_SVOD_00 = pd.read_csv(pyt_txt, sep="\t", encoding='utf-8', parse_dates=['По дням'], dayfirst=True)
                    lg = ('Выручка', "Количество продаж")
                    for e in lg:
                        PROD_SVOD_00[e] = PROD_SVOD_00[e].str.replace(" ", "")
                        PROD_SVOD_00[e] = PROD_SVOD_00[e].str.replace(",", ".")
                        PROD_SVOD_00[e] = PROD_SVOD_00[e].str.replace(" ", "")
                        PROD_SVOD_00[e] = PROD_SVOD_00[e].astype("float")
                        PROD_SVOD_00['Склад магазин.Наименование'] = PROD_SVOD_00['Склад магазин.Наименование'].astype("category")
                        PROD_SVOD_00['Номенклатура'] = PROD_SVOD_00['Номенклатура'].astype("str")
                        PODAROK = ("Подарочная карта КМ 500р+ конверт", "Подарочная карта КМ 1000р+ конверт", "подарочная карта КМ 500 НОВАЯ",
                                   "подарочная карта КМ 1000 НОВАЯ")
                        for x in PODAROK:
                            PROD_SVOD_00 = PROD_SVOD_00.loc[PROD_SVOD_00['Номенклатура'] != x]
                    PROD_SVOD = pd.concat([PROD_SVOD, PROD_SVOD_00], axis=0)
                #gc.enable()
                PROD_SVOD_00 = pd.DataFrame()
        # region ГРУППИРОВКА ТАБЛИЦЫ(Без номенклатуры по дням)
        PROD_SVOD = PROD_SVOD.rename(
            columns={"По дням": "ДАТА", 'Выручка': "Выручка Итого, руб с НДС",'Склад магазин.Наименование':"!МАГАЗИН!"})
        PROD_SVOD = PROD_SVOD.groupby(["ДАТА", "!МАГАЗИН!"], as_index=False) \
            .aggregate({"Выручка Итого, руб с НДС": "sum"}) \
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
            .aggregate({"ДАТА": "nunique", "Выручка Итого, руб с НДС": "sum"}) \
            .sort_values("!МАГАЗИН!", ascending=False)
        # endregion
        # region  ДОБАВЛЕНИЕ ДАННЫХ
        # region выручка без ндс
        nds_vir = NEW().nds_vir()

        PROD_SVOD = PROD_SVOD.merge(nds_vir, on=["Месяц","!МАГАЗИН!"], how="left")
        PROD_SVOD["Выручка Итого, руб без НДС"] = PROD_SVOD["Выручка Итого, руб с НДС"] * PROD_SVOD["Ставка НДС"]
        PROD_SVOD = PROD_SVOD.reset_index(drop=True)

        # endregion
        # endregion
        #region ДОБАВЛЕНИЕ ДАННЫХ КАЛЕНДАРЯ
        Calendar = pd.read_excel(PUT + "DATA_2\\Календарь.xlsx", sheet_name="Query1")
        Calendar.loc[~Calendar["ДАТА"].dt.is_month_start, "ДАТА"] = Calendar["ДАТА"] - MonthBegin()
        Calendar = Calendar.groupby(["ГОД", "НОМЕР МЕСЯЦА", "ДАТА"], as_index=False) \
            .aggregate({'ДНЕЙ В МЕСЯЦЕ': "max"}) \
            .sort_values("ГОД", ascending=False)
        PROD_SVOD = PROD_SVOD.rename(columns={'Склад магазин.Наименование': "!МАГАЗИН!", 'ДАТА': "Факт отработанных дней"})
        PROD_SVOD = PROD_SVOD.rename(columns={'Месяц': 'ДАТА'})
        PROD_SVOD = PROD_SVOD.merge(Calendar, on=["ДАТА"], how="left")
        PROD_SVOD["Осталось дней продаж"] = PROD_SVOD["ДНЕЙ В МЕСЯЦЕ"] - PROD_SVOD["Факт отработанных дней"]
        dd = PROD_SVOD.groupby('ДАТА')['Осталось дней продаж'].aggregate('min')
        PROD_SVOD = PROD_SVOD.merge(dd, on=["ДАТА"], how="left")
        PROD_SVOD.loc[PROD_SVOD["Осталось дней продаж_x"] > PROD_SVOD["Осталось дней продаж_y"], 'Осталось дней продаж_x'] = PROD_SVOD["Осталось дней продаж_y"]
        PROD_SVOD = PROD_SVOD.drop(columns={"Осталось дней продаж_y", "НОМЕР МЕСЯЦА", "ГОД" })
        PROD_SVOD = PROD_SVOD.rename(columns={'Осталось дней продаж_x': "Осталось дней продаж"})
        # endregion
        # region ДОБАВЛЕНИЕ КАНАЛОВ ОБОБЩАЮЩИХ В ТАБЛИЦУ ПРОДАЖ
        canal = pd.read_excel(PUT + "DATA_2\\" + "Каналы.xlsx", sheet_name="Лист1")
        canal["ДАТА"] = canal["ДАТА"].astype("datetime64[ns]")
        PROD_SVOD = pd.concat([PROD_SVOD, canal], axis=0)
        PROD_SVOD = PROD_SVOD.reset_index(drop=True)
        # endregion
        # region РАЗВОРОТ ТАБЛИЦЫ ПРОДАЖ
        print("РАЗВОРОТ ТАБЛИЦЫ ПРОДАЖ")
        PROD_SVOD = PROD_SVOD.drop(columns={"ГОД","НОМЕР МЕСЯЦА", "ПРОДАЖИ БЕЗ НДС", "Ставка НДС", "ПРОВЕРКАА", "ПРОДАЖИ С НДС" })
        PROD_SVOD = PROD_SVOD.melt(id_vars=["ДАТА", "!МАГАЗИН!", "ДНЕЙ В МЕСЯЦЕ", "Осталось дней продаж", "Факт отработанных дней"])
        PROD_SVOD = PROD_SVOD.rename(columns={"variable": "Статья", "value": "Значение"})
        # endregion
        # region ОКРУГЛЕНИЕ
        PROD_SVOD["Значение"] =PROD_SVOD["Значение"].round(2)
        # endregion
        # region ДОБАВЛЕНИЕ КАНАЛ НА ПОСЛЕДНИЙ ЗАКРЫТЫЙ ПЕРИОД
        FINREZ_KANAL = pd.read_csv(PUT + "RESULT\\" + "FINREZ_KANAL.csv", encoding="ANSI", sep=';')
        PROD_SVOD = PROD_SVOD.merge(FINREZ_KANAL,
                               on=["!МАГАЗИН!"], how="left")
        PROD_SVOD =PROD_SVOD.reset_index(drop=True)
        PROD_SVOD.loc[(PROD_SVOD["Учитывать выручку с НДС"] == "нет"), "Значение"] = 0
        PROD_SVOD = PROD_SVOD.loc[PROD_SVOD["Значение"] > 0 ]
        PROD_SVOD = PROD_SVOD.drop(columns={"Учитывать выручку с НДС"})
        # endregion
        print("Каналы добавлены")
        # region ВЫЧИСЛЕНИЕ ПРОГНОЗА
        PROD_SVOD["Значение"] = ((PROD_SVOD["Значение"]/PROD_SVOD["Факт отработанных дней"])*PROD_SVOD["Осталось дней продаж"])+PROD_SVOD["Значение"]
        # endregion
        gc.enable()
        return PROD_SVOD

class MIN_MAX:
    def max_FINREZ_Month(self):
        print("МАКС ФИНРЕЗ")
        FINREZ = pd.read_csv(PUT + "TEMP\\" + "MIN_MAX_FINREZ.csv",
                         sep=";", encoding='ANSI', parse_dates=['Дата'], dayfirst=True)
        FINREZ = FINREZ[["Дата"]]
        FINREZ = FINREZ.reset_index(drop=True)
        FINREZ = FINREZ.loc[FINREZ['Дата'] >= "2023-01-01"]
        FINREZ = FINREZ.reset_index(drop=True)
        FINREZ['Дата2'] = FINREZ['Дата'].dt.month
        FINREZ_MAX_DATE = FINREZ['Дата2'].max()
        return FINREZ_MAX_DATE

    def max_FINREZ_DATA(self):
        print("МАКС ФИНРЕЗ")
        FINREZ = pd.read_csv(PUT + "TEMP\\" + "MIN_MAX_FINREZ.csv",
                             sep=";", encoding='ANSI', parse_dates=['Дата'], dayfirst=True)
        FINREZ = FINREZ[["Дата"]]
        FINREZ = FINREZ.reset_index(drop=True)
        FINREZ = FINREZ.loc[FINREZ['Дата'] >= "2023-01-01"]
        FINREZ = FINREZ.reset_index(drop=True)
        FINREZ_MAX_DATE = FINREZ['Дата'].max()
        return FINREZ_MAX_DATE

class SPRAVOCHIK:
    def spravcnik_STATYA(self):
        STATYA = pd.read_excel("C:\\Users\\lebedevvv\\Desktop\\ДАШБОРД\\RESULT\\@СПРАВОЧНИК_СТАТЕЙ.xlsx",
                                     sheet_name="STATYA_REDAKT")
        return STATYA

class DOC:
    def to(self, x, name):
        x.to_csv(PUT + "RESULT\\" + name, encoding="ANSI", sep=';',
                         index=False, decimal='.')
        return x
    def to_POWER_BI(self,x,name):
        x.to_csv(PUT + "RESULT\\" + name, encoding="ANSI", sep=';',
                 index=False, decimal=',')

class SVOD:
    def svod(self):
        PROD_SVOD = NEW().obnovlenie()
        FINREZ ,STATYA,FINREZ_KANAL,FINREZ_AVERAGE = FINFEZ().Finrez_obrabotka()
        PROD_SVOD = SALES().SALES_obrabotka()
        FINREZ = FINREZ.rename(columns={"Дата": "ДАТА"})
        FINREZ_AVERAGE = FINREZ_AVERAGE.rename(columns={"Дата": "ДАТА"})
        print(FINREZ_AVERAGE)
        print(PROD_SVOD)
        print(FINREZ)
        ITOGI = pd.concat([FINREZ_AVERAGE, PROD_SVOD, FINREZ], axis=0)
        print(ITOGI)
        ITOGI = ITOGI.reset_index(drop=True)
        ITOGI["Значение"] = ITOGI["Значение"].round(2)
        ITOGI = ITOGI.drop(columns={"Осталось дней продаж", "Факт отработанных дней","ДНЕЙ В МЕСЯЦЕ"})
        DOC().to_POWER_BI(x=ITOGI, name="@ФИНРЕЗУЛЬТАТ ОБРАБОТАННЫЙ.csv")
        print("OOOOOOOOOOOOOOOOOOOOOOO")
        return ITOGI
#NEW().nds_vir()
#SVOD().svod()

FINFEZ().Finrez_obrabotka()

#SALES().SALES_obrabotka()
#NEW().obnovlenie()
