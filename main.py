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

class RENAME:
    """Переименования магазинов"""

    def Rread(self):
        replacements = pd.read_excel("D:\\Python\\Dashboard\\DATA_2\\ДЛЯ ЗАМЕНЫ.xlsx",
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
                    read.to_csv(PUT + "ПУТЬ ДО ФАЙЛОВ С НОВЫМИ ФАЙЛАМИ\\Текщий год\\" + file, encoding='utf-8', sep="\t",index=False)
                if ((file.split('.')[-1]) == 'xlsx'):
                        print("2. ФАЙЛ НАЙДЕН(Чеков)", file)
                        pyt_excel = os.path.join(rootdir, file)
                        read = pd.read_excel(pyt_excel, sheet_name="Sheet1")
                        for i in tqdm(range(rng), desc="(Обработака новых данных) 3. Переименование магазинов   --  ", ncols=120, colour="#F8C9CE", ):
                            read[
                            'Магазин'] = read['Магазин'].replace(replacements["НАЙТИ"][i], replacements["ЗАМЕНИТЬ"][i], regex=False)
                        read = read.reset_index(drop=True)
                        read.to_excel(PUT  + "NEW\\" + file,
                                    index=False)
                        print(" НЕТ ФАЙЛОВ")
                        #return read
    def nds_vir(self):
        """ЗАгрузка новых данных для вычисления ставки ндс, обьеденнеие, проставляет даты"""
        rng, replacements = RENAME().Rread()
        start_nds = "C:\\Users\\lebedevvv\\Desktop\\PYTHON PROJECT\\СТАВКА НДС ВЫРУЧКА"
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
        vir_NDS = vir_NDS.rename(columns={'Дата': 'дата'})
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
        Spisania = Spisania.rename(columns={'Дата': 'дата'})
        DOC().to(x=Spisania, name="TESTOVAYA2.csv")
        gc.enable()
        print(Spisania)
        return Spisania
    def NDS(self):

        return
    def HOZY(self):
        Spisania_HOZI= pd.read_csv("D:\\Python\\Dashboard\\SPISANIA_HOZI\\1.txt", sep="\t", encoding='utf-8', skiprows=8,
                                  names=("!МАГАЗИН!", "Номенклатура", "Сумма","Сумма без НДС"))
        Spisania_HOZI = Spisania_HOZI["Номенклатура"].unique()
        return Spisania_HOZI


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


class SPRAVOCHIK:
    def spravcnik_STATYA(self):
        STATYA = pd.read_excel(PUT + "DATA_2\\" + "@СПРАВОЧНИК_СТАТЕЙ.xlsx",
                               sheet_name="STATYA_REDAKT")
        return STATYA


class FINFEZ:
    """Обработка финреза"""
    def Finrez_obrabotka(self):
        rng, replacements = RENAME().Rread()
        print("Обновление финреза роверка на ошибки, сохранение файлов , и сохранение общего файла по доходам и затратам")
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
            FINREZ_MAX  = FINREZ[["дата"]]
            DOC().to_TEMP(x=FINREZ_MAX, name="MIN_MAX_FINREZ_DATE.csv")
            print("Сохранено - MIN_MAX_FINREZ_DATE.csv")

            # region переименование обобщения
            FINREZ.loc[FINREZ['!МАГАЗИН!'] == "Офис", "Канал"] = "Офис"
            FINREZ.loc[FINREZ['!МАГАЗИН!'] == "Роялти ФРС", "Канал"] = "Роялти ФРС"
            FINREZ = FINREZ.reset_index(drop=True)
            # endregion yt
            # region вычисление наценки
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
            FINREZ["Наценка (Общий) с НДС"] = FINREZ["Наценка МКП и КП, руб с НДС"] + FINREZ[
                "Наценка сопутка, руб с НДС"]
            # endregion
            # region вычисление доли
            r = ("Доля Калина малина", "Доля Пекарня", "Доля Продукция кулинарного цеха КХВ", "Доля Рыбные п/ф",
                 "Доля \"Изготовлено по заказу\"",
                 "Доля Волков Кофе", "Доля зеленый магазин", "Доля сопутка", "Доля субпродукты кур", "Доля куриные п/ф",
                 "Доля  Кости ливер отруба", "Доля  гриль", "Доля п/ф", "Доля колбаса")
            for Y in tqdm(r, desc="     Расчет", ncols=120, colour="#F8C9CE", ):
                time.sleep(0.1)
                FINREZ[Y] = FINREZ[Y] * FINREZ["Выручка Итого, руб с НДС"]
            # endregion
            # region добавление закупа с ндс
            FINREZ["@Закуп товара (МКП, КП, сопутка), руб c НДС"] = FINREZ["1.1.Закуп товара (МКП и КП), руб с НДС"] + \
                                                                    FINREZ["1.2.Закуп товара (сопутка), руб с НДС"]
            FINREZ.loc[(FINREZ["Канал"] == "ФРС") & (FINREZ["Режим налогообложения"] == "упрощенка"),
            "* Закуп товара (МКП, КП, сопутка), руб без НДС" ] = FINREZ["@Закуп товара (МКП, КП, сопутка), руб c НДС"]
            # endregion
            # region разворот таблицы финреза
            """таблица без разворота"""

            """расзвернутая таблица"""
            FINREZ = FINREZ.melt(
                id_vars=["дата", "!МАГАЗИН!", "Режим налогообложения", "Канал", "Канал на последний закрытый период"])
            FINREZ = FINREZ.rename(columns={"variable": "статья", "value": "Значение"})
            # endregion
            # region очистка от мусора
            FINREZ['Значение'] = FINREZ['Значение'].astype("str")
            FINREZ['Значение'] = FINREZ['Значение'].str.replace(" ", "")
            FINREZ['Значение'] = np.where((FINREZ['Значение'] == 0), "nan", FINREZ['Значение'])
            FINREZ['Значение'] = np.where((FINREZ['Значение'] == "-"), "nan", FINREZ['Значение'])
            FINREZ['Значение'] = np.where((FINREZ['Значение'] == "#ДЕЛ/0!"), "nan", FINREZ['Значение'])
            FINREZ['Значение'] = np.where((FINREZ['Значение'] == "#ЗНАЧ!"), "nan", FINREZ['Значение'])
            FINREZ['Значение'] = FINREZ['Значение'].str.replace(",", ".")
            FINREZ = FINREZ.loc[(FINREZ['Значение'] != "nan")]
            FINREZ['Значение'] = FINREZ['Значение'].astype("float")
            FINREZ = FINREZ.loc[(FINREZ['Значение'] != 0)]
            # endregion
            # region удаление ошибок в файле
            # 2022-07-01
            """      FINREZ.loc[(FINREZ['статья'] == "Прибыль (+) / Убыток (-) (= Т- ОЕ), руб без НДС") & (
                        FINREZ['дата'] == "2022-07-01") & (
                               FINREZ["Канал"] == "Итого ФРС"), 'Значение'] = FINREZ['Значение'] - 90000
            FINREZ.loc[(FINREZ['статья'] == "Прибыль (+) / Убыток (-) (= Т- ОЕ), руб без НДС") & (
                        FINREZ['дата'] == "2022-07-01") & (
                               FINREZ["Канал"] == "ФРС+Франшиза"), 'Значение'] = FINREZ['Значение'] - 171925
            FINREZ.loc[(FINREZ['статья'] == "Прибыль (+) / Убыток (-) (= Т- ОЕ), руб без НДС") & (
                        FINREZ['дата'] == "2022-07-01") & (
                               FINREZ["Канал"] == "Итого Франшиза"), 'Значение'] = FINREZ['Значение'] - 81925
            FINREZ.loc[
                (FINREZ['статья'] == "Доход Аренда помещений, руб без НДС") & (FINREZ['дата'] == "2022-07-01") & (
                        FINREZ["Канал"] == "ФРС+Франшиза"), 'Значение'] = \
                FINREZ['Значение'] - 81925
            FINREZ.loc[
                (FINREZ['статья'] == "Доход Аренда помещений, руб без НДС") & (FINREZ['дата'] == "2022-07-01") & (
                        FINREZ["Канал"] == "Итого Франшиза"), 'Значение'] = FINREZ['Значение'] - 81925
            FINREZ.loc[
                (FINREZ['статья'] == "Доход Аренда помещений, руб без НДС") & (FINREZ['дата'] == "2022-07-01") & (
                        FINREZ["Канал"] == "Розничная сеть"), 'Значение'] = FINREZ['Значение'] - 81925

            FINREZ.loc[(FINREZ['статья'] == "2.2. Аренда") & (FINREZ['дата'] == "2022-07-01") & (
                        FINREZ["Канал"] == "Итого ФРС"), 'Значение'] = \
                FINREZ[
                    'Значение'] + 90000
            FINREZ.loc[(FINREZ['статья'] == "2.2. Аренда") & (FINREZ['дата'] == "2022-07-01") & (
                        FINREZ["Канал"] == "ФРС+Франшиза"), 'Значение'] = \
                FINREZ[
                    'Значение'] + 90000
            FINREZ.loc[(FINREZ['статья'] == "Точка безубыточности (МКП, КП, Сопутка), руб с НДС") & (
                        FINREZ['дата'] == "2022-07-01") & (
                               FINREZ["Канал"] == "Итого ФРС"), 'Значение'] = FINREZ['Значение'] + 355810
            FINREZ.loc[(FINREZ['статья'] == "Точка безубыточности (МКП, КП, Сопутка), руб с НДС") & (
                        FINREZ['дата'] == "2022-07-01") & (
                               FINREZ["Канал"] == "Розничная сеть"), 'Значение'] = FINREZ['Значение'] + 355810
            FINREZ.loc[(FINREZ['статья'] == "Точка безубыточности (МКП, КП, Сопутка), руб с НДС") & (
                        FINREZ['дата'] == "2022-07-01") & (
                               FINREZ["Канал"] == "ФРС+Франшиза"), 'Значение'] = FINREZ['Значение'] + 355810"""
            # 2022-08-01
            # endregion
            # region добавление справочника сатей
            STATYA = SPRAVOCHIK().spravcnik_STATYA()
            FINREZ = FINREZ.merge(STATYA[["статья", "тип данных", "фрс_расчет среднего",
                                          "фр_расчет чистой прибыли", "подгруппа", "группа","фрс_расчет чистой прибыли"]],
                                  on=["статья"], how="left")
            FINREZ["каскад"] = FINREZ["Значение"]
            # endregion
            # region замена положительных на отрицательные
            FINREZ.loc[FINREZ["группа"] == "Расход", "каскад"] = -FINREZ["Значение"]
            FINREZ.loc[FINREZ["группа"] == "Закуп", "каскад"] = -FINREZ["Значение"]

            # endregion

            # region деление на каналы
            # region ФРС
            # таблица сравнения с итого ФРС'''
            FINREZ_PROWERKA = FINREZ.loc[FINREZ["!МАГАЗИН!"] == "ФРС без затрат офиса"]
            FINREZ_FRS = FINREZ.loc[FINREZ["Канал"] == "ФРС"]
            FINREZ_FRS = FINREZ_FRS.loc[(FINREZ_FRS["фрс_расчет чистой прибыли"] == "да")]
            FINREZ_FRS = FINREZ_FRS.loc[(FINREZ_FRS["статья"] != "@Закуп товара (МКП, КП, сопутка), руб c НДС")]

            # endregion
            # region ERROR_ФРС
            FINREZ_FRS_ERROR = FINREZ_FRS.groupby(["дата", "статья","фрс_расчет чистой прибыли"], as_index=False) \
                .aggregate({'Значение': "sum", "каскад": "sum" }) \
                .sort_values('Значение', ascending=False)
            FINREZ_FRS_ERROR = FINREZ_FRS_ERROR.merge(FINREZ_PROWERKA[["дата", "статья","каскад","Значение"]],
                                          on=[ "дата", "статья"], how="left")
            FINREZ_FRS_ERROR = FINREZ_FRS_ERROR.rename(columns={"каскад_y":"ФРС_ИТОГИ(каскад)","Значение_y":"ФРС_ИТОГИ","каскад_x":"каскадная","Значение_x":"значение"})
            FINREZ_FRS_ERROR["расхождение"] = FINREZ_FRS_ERROR["ФРС_ИТОГИ"] - FINREZ_FRS_ERROR["значение"]
            FINREZ_FRS_ERROR = FINREZ_FRS_ERROR.loc[(FINREZ_FRS_ERROR["расхождение"] < -10 ) | (FINREZ_FRS_ERROR["расхождение"] > 10)]
            FINREZ_FRS_ERROR = FINREZ_FRS_ERROR[["дата","статья","значение","ФРС_ИТОГИ","расхождение" ]]
            DOC().to_ERROR(x=FINREZ_FRS_ERROR, name="FINREZ_FRS_ERROR.csv")
            # endregion
            # region добавляем чистую прибыль ФРС
            grouped = FINREZ_FRS.groupby(['!МАГАЗИН!', 'дата', 'Канал',"Канал на последний закрытый период"])
            sums = grouped['каскад'].agg('sum')
            new_row = pd.DataFrame({
                '!МАГАЗИН!': sums.index.get_level_values('!МАГАЗИН!'),
                'дата': sums.index.get_level_values('дата'),
                'Канал': sums.index.get_level_values('Канал'),
                "Канал на последний закрытый период": sums.index.get_level_values("Канал на последний закрытый период"),
                "статья": 'Чистая прибыль',
                'Значение': sums.values,
                'каскад': sums.values})
            FINREZ_FRS = pd.concat([FINREZ_FRS,new_row], axis=0)
            # endregion
            # region СОХРАНЕНИЕ ТАБЛИЦЫ ФРС
            FINREZ_FRS_AVER  = FINREZ_FRS
            FINREZ_FRS = FINREZ_FRS[["дата","Канал","!МАГАЗИН!", "Канал на последний закрытый период", "статья", "Значение",
                                                 "фрс_расчет чистой прибыли", "подгруппа", "группа", "каскад"]]
            FINREZ_FRS = FINREZ_FRS.rename(columns={"фрс_расчет чистой прибыли": "расчет чистой прибыли"})
            DOC().to_POWER_BI(x=FINREZ_FRS, name="FRS.csv")
            # endregion
            # region вычисление среднего для ФРС
            FINREZ_FRS_AVER = FINREZ_FRS_AVER.loc[(FINREZ_FRS_AVER["фрс_расчет среднего"] == "да")]
            DOC().to_POWER_BI(x=FINREZ_FRS_AVER, name="ФРС_Среднее.csv")
            # endregion

            # region ФРАНШИЗА ВНЕШНЯЯ и аренда
            FINREZ_PROWERKA = FINREZ.loc[FINREZ["!МАГАЗИН!"] == "Франшиза без затрат офиса"]
            FINREZ_FRANSHIZA = FINREZ.loc[(FINREZ["Канал"] == "Франшиза в аренду") | (FINREZ["Канал"] == "Франшиза внешняя")]
            FINREZ_FRANSHIZA= FINREZ_FRANSHIZA.loc[(FINREZ_FRANSHIZA["фр_расчет чистой прибыли"] == "да")]
            # endregion
            # region ERROR_ФР
            FINREZ_FRANSHIZA_ERROR = FINREZ_FRANSHIZA.groupby(["дата", "статья","фр_расчет чистой прибыли"], as_index=False) \
                .aggregate({'Значение': "sum", "каскад": "sum" }) \
                .sort_values('Значение', ascending=False)
            FINREZ_FRANSHIZA_ERROR = FINREZ_FRANSHIZA_ERROR.merge(FINREZ_PROWERKA[["дата", "статья","каскад","Значение"]],
                                          on=[ "дата", "статья"], how="left")
            FINREZ_FRANSHIZA_ERROR = FINREZ_FRANSHIZA_ERROR.rename(columns={"каскад_y":"ФР_ИТОГИ(каскад)","Значение_y":"ФР_ИТОГИ","каскад_x":"каскадная","Значение_x":"значение"})
            FINREZ_FRANSHIZA_ERROR["расхождение"] = FINREZ_FRANSHIZA_ERROR["ФР_ИТОГИ"] - FINREZ_FRANSHIZA_ERROR["значение"]
            FINREZ_FRANSHIZA_ERROR = FINREZ_FRANSHIZA_ERROR.loc[(FINREZ_FRANSHIZA_ERROR["расхождение"] < -10 ) | (FINREZ_FRANSHIZA_ERROR["расхождение"] > 10)]
            FINREZ_FRANSHIZA_ERROR = FINREZ_FRANSHIZA_ERROR[["дата","статья","значение","ФР_ИТОГИ","расхождение" ]]
            DOC().to_ERROR(x=FINREZ_FRANSHIZA_ERROR, name="FINREZ_FRANSHIZA_ERROR.csv")
            # endregion
            # region добавляем чистую прибыль ФР
            grouped = FINREZ_FRANSHIZA.groupby(['!МАГАЗИН!', 'дата', 'Канал'])
            sums = grouped['каскад'].agg('sum')
            new_row = pd.DataFrame({
                '!МАГАЗИН!': sums.index.get_level_values('!МАГАЗИН!'),
                'дата': sums.index.get_level_values('дата'),
                'Канал': sums.index.get_level_values('Канал'),
                "статья": 'Чистая прибыль',
                'Значение': sums.values,
                'каскад': sums.values})
            FINREZ_FRANSHIZA = pd.concat([FINREZ_FRANSHIZA,new_row], axis=0)

            # endregio

            # endregion
            # region СОХРАНЕНИЕ ТАБЛИЦЫ ФРАНШИЗЫ
            FINREZ_FRANSHIZA_AVER = FINREZ_FRANSHIZA
            FINREZ_FRANSHIZA = FINREZ_FRANSHIZA[["дата","Канал" ,"!МАГАЗИН!","Канал на последний закрытый период","статья","Значение",
                                                 "фр_расчет чистой прибыли","подгруппа", "группа","каскад"]]
            FINREZ_FRANSHIZA = FINREZ_FRANSHIZA.rename(columns={"фр_расчет чистой прибыли": "расчет чистой прибыли"})

            DOC().to_POWER_BI(x=FINREZ_FRANSHIZA, name="ФFRANSHIZA.csv")
            # endregion
            # Обьеденение таблиц
            FINREZ_OBCHIY  = pd.concat([FINREZ_FRANSHIZA,FINREZ_FRS], axis=0)
            FINREZ_OBCHIY = FINREZ_OBCHIY.reset_index(drop=True)
            #DOC().to_POWER_BI(x=FINREZ_OBCHIY, name="FINREZ_OBCHIY.csv")
            # Проверка на ошибки
            FINREZ_PROWERKA = FINREZ.loc[FINREZ["статья"] == "Прибыль (+) / Убыток (-) (= Т- ОЕ), руб без НДС"]
            FINREZ_PROWERKA.loc[FINREZ_PROWERKA["статья"] == "Прибыль (+) / Убыток (-) (= Т- ОЕ), руб без НДС","статья" ]= "Чистая прибыль"
            FINREZ_OBCHIY_ERROR= FINREZ_OBCHIY.merge(FINREZ_PROWERKA[["статья","дата","!МАГАЗИН!","Канал на последний закрытый период", "Канал","Значение"]],
                                  on=["статья","дата","!МАГАЗИН!","Канал на последний закрытый период", "Канал" ], how="left")
            FINREZ_OBCHIY_ERROR["расхождение"] = FINREZ_OBCHIY_ERROR["Значение_y"] - FINREZ_OBCHIY_ERROR[
                "Значение_x"]
            FINREZ_OBCHIY_ERROR = FINREZ_OBCHIY_ERROR.loc[
                (FINREZ_OBCHIY_ERROR["расхождение"] < -10) | (FINREZ_OBCHIY_ERROR["расхождение"] > 10)]
            FINREZ_OBCHIY_ERROR = FINREZ_OBCHIY_ERROR[["дата", "Канал","!МАГАЗИН!","Канал на последний закрытый период","Значение_y","Значение_x","расхождение" ]]
            FINREZ_OBCHIY_ERROR = FINREZ_OBCHIY_ERROR.rename(columns={"Значение_x": "РАсчет","Значение_y": "с файла"})
            DOC().to_ERROR(x=FINREZ_OBCHIY_ERROR, name="FINREZ_OBCHIY_ERROR.csv")
            # endregion

            print("Сохранен")
            return FINREZ_OBCHIY, FINREZ_OBCHIY_ERROR, FINREZ_FRANSHIZA, FINREZ_FRANSHIZA_ERROR, FINREZ_FRS, FINREZ_FRS_ERROR,FINREZ_MAX

    def Finrez_AVERGE(self):
        FINREZ_OBCHIY, FINREZ_OBCHIY_ERROR, FINREZ_FRANSHIZA, FINREZ_FRANSHIZA_ERROR, FINREZ_FRS, FINREZ_FRS_ERROR, FINREZ_MAX = FINFEZ().Finrez_obrabotka()
        date_max = MIN_MAX().max_FINREZ_DATA()
        # region ДЛЯ ФРС
        """группировка продаж за последние 3 месяца"""
        FINREZ_FRS_AVEREGE = FINREZ_FRS.copy()
        FINREZ_FRS_AVEREGE['дата'] = pd.to_datetime(FINREZ_FRS_AVEREGE['дата'])
        FINREZ_FRS_AVEREGE.set_index('дата', inplace=True)
        
        # выделение последних трех месяцев
        FINREZ_FRS_AVEREGE = FINREZ_FRS_AVEREGE[FINREZ_FRS_AVEREGE.index >= FINREZ_FRS_AVEREGE.index.max() - pd.DateOffset(months=2)]
        FINREZ_FRS_AVEREGE = FINREZ_FRS_AVEREGE.groupby(['статья', '!МАГАЗИН!', 'Канал на последний закрытый период',
                                                         "расчет чистой прибыли", "подгруппа", "группа", 'Канал'])[['Значение', 'каскад']].mean().reset_index()
        FINREZ_FRS_AVEREGE = FINREZ_FRS_AVEREGE.reset_index(drop=True)
        
        FINREZ_FRS_AVEREGE = FINREZ_FRS_AVEREGE.reset_index()
        
        # Добавляем дату в датафрейм + 1 месяц
        FINREZ_FRS_AVEREGE_01  = FINREZ_FRS_AVEREGE.copy()
        FINREZ_FRS_AVEREGE_01["дата"] = date_max + DateOffset(months=1)

        # Добавляем дату в датафрейм + 2 месяца
        FINREZ_FRS_AVEREGE_02 = FINREZ_FRS_AVEREGE.copy()
        FINREZ_FRS_AVEREGE_02["дата"] = date_max + DateOffset(months=2)
        
        # оьеденяем датафреймы
        FINREZ_FRS_AVEREGE  = pd.concat([FINREZ_FRS_AVEREGE_01, FINREZ_FRS_AVEREGE_02], axis=0)
        FINREZ_FRS_AVEREGE = FINREZ_FRS_AVEREGE.drop('index', axis=1)
        FINREZ_FRS_AVEREGE = FINREZ_FRS_AVEREGE.reset_index(drop=True)
        # DOC().to_POWER_BI(x=FINREZ_FRS_AVEREGE, name="FINREZ_FRS_AVEREGE.csv")
        # endregion
        # region ДЛЯ ФРАНШИЗЫ
        FINREZ_FRANSHIZA_AVEREGE = FINREZ_FRANSHIZA.copy()

        # выделение последних трех месяцев
        FINREZ_FRANSHIZA_AVEREGE['дата'] = pd.to_datetime(FINREZ_FRANSHIZA_AVEREGE['дата'])
        FINREZ_FRANSHIZA_AVEREGE.set_index('дата', inplace=True)
        FINREZ_FRANSHIZA_AVEREGE = FINREZ_FRANSHIZA_AVEREGE[
            FINREZ_FRANSHIZA_AVEREGE.index >= FINREZ_FRANSHIZA_AVEREGE.index.max() - pd.DateOffset(months=2)]
        FINREZ_FRANSHIZA_AVEREGE = FINREZ_FRANSHIZA_AVEREGE.groupby(['статья', '!МАГАЗИН!', 'Канал на последний закрытый период',
                                                         "расчет чистой прибыли", "подгруппа", "группа", 'Канал'])[
            ['Значение', 'каскад']].mean().reset_index()

        FINREZ_FRANSHIZA_AVEREGE = FINREZ_FRANSHIZA_AVEREGE.reset_index(drop=True)

        FINREZ_FRANSHIZA_AVEREGE = FINREZ_FRANSHIZA_AVEREGE.reset_index()

        # Добавляем дату в датафрейм + 1 месяц
        FINREZ_FRANSHIZA_AVEREGE_01 = FINREZ_FRANSHIZA_AVEREGE.copy()
        FINREZ_FRANSHIZA_AVEREGE_01["дата"] = date_max + DateOffset(months=1)

        # Добавляем дату в датафрейм + 2 месяца
        FINREZ_FRANSHIZA_AVEREGE_02 = FINREZ_FRANSHIZA_AVEREGE.copy()
        FINREZ_FRANSHIZA_AVEREGE_02["дата"] = date_max + DateOffset(months=2)

        # оьеденяем датафреймы
        FINREZ_FRANSHIZA_AVEREGE = pd.concat([FINREZ_FRANSHIZA_AVEREGE_01, FINREZ_FRANSHIZA_AVEREGE_02], axis=0)
        FINREZ_FRANSHIZA_AVEREGE = FINREZ_FRANSHIZA_AVEREGE.drop('index', axis=1)
        FINREZ_FRANSHIZA_AVEREGE = FINREZ_FRANSHIZA_AVEREGE.reset_index(drop=True)

        #DOC().to_POWER_BI(x=FINREZ_FRANSHIZA_AVEREGE, name="FINREZ_FRANSHIZA_AVEREGE.csv")

        # endregion

        FINREZ_FRANSHIZA_AVEREGE_OBCHEE = pd.concat([FINREZ_FRANSHIZA_AVEREGE, FINREZ_FRS_AVEREGE], axis=0)
        FINREZ_FRANSHIZA_AVEREGE_OBCHEE = FINREZ_FRANSHIZA_AVEREGE_OBCHEE.reset_index(drop=True)

        return FINREZ_FRANSHIZA_AVEREGE_OBCHEE


    def ITOGOVAYA(self):
        FINREZ_FRANSHIZA_AVEREGE_OBCHEE = FINFEZ().Finrez_AVERGE()
        FINREZ_OBCHIY, FINREZ_OBCHIY_ERROR, FINREZ_FRANSHIZA, FINREZ_FRANSHIZA_ERROR, FINREZ_FRS, FINREZ_FRS_ERROR, FINREZ_MAX = FINFEZ().Finrez_obrabotka()

        FINREZ_ITOG = pd.concat([FINREZ_OBCHIY, FINREZ_FRANSHIZA_AVEREGE_OBCHEE], axis=0)
        FINREZ_ITOG = FINREZ_ITOG.reset_index(drop=True)
        DOC().to_POWER_BI(x=FINREZ_ITOG, name="FINREZ_ITOG.csv")
        return FINREZ_ITOG


class MIN_MAX:
    def max_FINREZ_Month(self):
        print("МАКС ФИНРЕЗ")
        FINREZ = pd.read_csv(PUT + "TEMP\\" + "MIN_MAX_FINREZ_DATE.csv",
                         sep=";", encoding='ANSI', parse_dates=['дата'], dayfirst=True)
        FINREZ = FINREZ[["дата"]]
        FINREZ = FINREZ.reset_index(drop=True)
        FINREZ = FINREZ.loc[FINREZ['дата'] >= "2023-01-01"]
        FINREZ = FINREZ.reset_index(drop=True)
        FINREZ['Дата2'] = FINREZ['дата'].dt.month
        FINREZ_MAX_DATE = FINREZ['Дата2'].max()
        return FINREZ_MAX_DATE

    def max_FINREZ_DATA(self):
        print("МАКС ФИНРЕЗ")
        FINREZ = pd.read_csv(PUT + "TEMP\\" + "MIN_MAX_FINREZ_DATE.csv",
                             sep=";", encoding='ANSI', parse_dates=['дата'], dayfirst=True)
        FINREZ = FINREZ[["дата"]]
        FINREZ = FINREZ.reset_index(drop=True)
        FINREZ = FINREZ.loc[FINREZ['дата'] >= "2023-01-01"]
        FINREZ = FINREZ.reset_index(drop=True)
        FINREZ_MAX_DATE = FINREZ['дата'].max()
        return FINREZ_MAX_DATE


class SALES:
    def SALES_obrabotka(self):
        MIN_MAX().max_FINREZ_Month()
        PROD_SVOD = pd.DataFrame()
        print("ОБНОВЛЕНИЕ СВОДНОЙ ПРОДАЖ")
        start = PUT + "ПУТЬ ДО ФАЙЛОВ С НОВЫМИ ФАЙЛАМИ\\Текщий год\\"
        for rootdir, dirs, files in os.walk(start):
            for file in tqdm(files, desc="(Обработака новых данных) 3.Склеивание данных   --  ", ncols=120, colour="#F8C9CE" ):
                if ((file.split('.')[-1]) == 'txt'):
                    pyt_txt = os.path.join(rootdir, file)
                    PROD_SVOD_00 = pd.read_csv(pyt_txt, sep="\t", encoding='utf-8', parse_dates=['По дням'], dayfirst=True)
                    lg = ('Выручка', "Количество продаж", "ВесПродаж", "Прибыль", "СписРуб","Себестоимость")
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
                gc.enable()
                PROD_SVOD_00 = pd.DataFrame()
        # признак ХОЗЫ

        Hoz = NEW().HOZY()
        mask = PROD_SVOD['Номенклатура'].isin(Hoz)
        PROD_SVOD.loc[mask, 'СписРуб_ХОЗЫ'] = PROD_SVOD.loc[mask, 'СписРуб']
        PROD_SVOD.loc[mask, 'СписРуб'] = np.nan
        PROD_SVOD['СписРуб_ХОЗЫ'] = PROD_SVOD['СписРуб_ХОЗЫ'].astype("float")
        PROD_SVOD['СписРуб'] = PROD_SVOD['СписРуб'].astype("float")
        print(PROD_SVOD.info())


        # region ГРУППИРОВКА ТАБЛИЦЫ(Без номенклатуры по дням)
        PROD_SVOD = PROD_SVOD.rename(
            columns={"По дням": "ДАТА", 'Выручка': "Выручка Итого, руб с НДС",'Склад магазин.Наименование':"!МАГАЗИН!",
                     'СписРуб': "2.5.1. Списание потерь (до ноября 19г НЕУ + Списание потерь)","СписРуб_ХОЗЫ": "2.6. Хозяйственные товары",
                     "Себестоимость":"* Закуп товара (МКП, КП, сопутка), руб без НДС", "Прибыль": "Наценка Общая, руб"})

        PROD_SVOD = PROD_SVOD.groupby(["ДАТА", "!МАГАЗИН!"], as_index=False) \
            .aggregate({"Выручка Итого, руб с НДС": "sum", "Количество продаж": "sum", "ВесПродаж": "sum", "Наценка Общая, руб" : "sum",
                        "2.5.1. Списание потерь (до ноября 19г НЕУ + Списание потерь)":"sum", "2.6. Хозяйственные товары":"sum","* Закуп товара (МКП, КП, сопутка), руб без НДС":"sum"}) \
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
            .aggregate({"ДАТА": "nunique", "Выручка Итого, руб с НДС": "sum", "Количество продаж": "sum", "ВесПродаж": "sum", "Наценка Общая, руб" : "sum",
                        "2.5.1. Списание потерь (до ноября 19г НЕУ + Списание потерь)":"sum", "2.6. Хозяйственные товары":"sum","* Закуп товара (МКП, КП, сопутка), руб без НДС":"sum"}) \
            .sort_values("!МАГАЗИН!", ascending=False)

        PROD_SVOD = PROD_SVOD.rename(
            columns={'Склад магазин.Наименование': "!МАГАЗИН!", 'ДАТА': "Факт отработанных дней"})
        PROD_SVOD = PROD_SVOD.rename(columns={'Месяц': 'дата'})
        print(PROD_SVOD)
        # endregion




        DOC().to_TEMP(x=PROD_SVOD, name="PROD_SVOD_TEMP.csv")
        return PROD_SVOD
    """обработка пути продаж"""
    def Sales_prognoz(self):
        NEW().Spisania()
        PROD_SVOD = pd.read_csv(PUT + "TEMP\\" + "PROD_SVOD_TEMP.csv",
                             sep=";", encoding='ANSI', parse_dates=['дата'], dayfirst=True)
        # redion добавление ставки ндс вычисление выручки без ндс
        nds_vir = NEW().nds_vir()
        PROD_SVOD = PROD_SVOD.merge(nds_vir, on=["дата","!МАГАЗИН!"], how="left")
        PROD_SVOD["Выручка Итого, руб без НДС"] = PROD_SVOD["Выручка Итого, руб с НДС"] * PROD_SVOD["Ставка НДС"]
        PROD_SVOD = PROD_SVOD.drop(columns={"ПРОДАЖИ С НДС", "ПРОДАЖИ БЕЗ НДС", "Ставка НДС","ПРОВЕРКАА"})
        PROD_SVOD = PROD_SVOD.reset_index(drop=True)
        # endregion
        #region ДОБАВЛЕНИЕ ДАННЫХ КАЛЕНДАРЯ
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
        PROD_SVOD.loc[PROD_SVOD["Осталось дней продаж_x"] > PROD_SVOD["Осталось дней продаж_y"], 'Осталось дней продаж_x'] = PROD_SVOD["Осталось дней продаж_y"]
        PROD_SVOD = PROD_SVOD.drop(columns={"Осталось дней продаж_y", "НОМЕР МЕСЯЦА", "ГОД" })
        PROD_SVOD = PROD_SVOD.rename(columns={'Осталось дней продаж_x': "Осталось дней продаж"})

        # region ДОБАВЛЕНИЕ КАНАЛОВ ОБОБЩАЮЩИХ В ТАБЛИЦУ ПРОДАЖ
        canal = pd.read_excel(PUT + "DATA_2\\" + "Каналы.xlsx", sheet_name="Лист1")
        canal["дата"] = canal["дата"].astype("datetime64[ns]")
        PROD_SVOD = pd.concat([PROD_SVOD, canal], axis=0)
        PROD_SVOD = PROD_SVOD.reset_index(drop=True)
        # endregion

        # region РАЗВОРОТ ТАБЛИЦЫ ПРОДАЖ
        print("РАЗВОРОТ ТАБЛИЦЫ ПРОДАЖ")
        PROD_SVOD = PROD_SVOD.melt(
            id_vars=["дата", "!МАГАЗИН!", "ДНЕЙ В МЕСЯЦЕ", "Осталось дней продаж", "Факт отработанных дней"])
        PROD_SVOD = PROD_SVOD.rename(columns={"variable": "Статья", "value": "Значение"})
        # endregion

        # region добавление прогноза
        PROD_SVOD["Значение"] = ((PROD_SVOD["Значение"] / PROD_SVOD["Факт отработанных дней"]) * PROD_SVOD[
            "Осталось дней продаж"]) + PROD_SVOD["Значение"]

        PROD_SVOD["Значение"] = PROD_SVOD["Значение"].round(2)

        # endregion

        DOC().to_TEMP(x=PROD_SVOD, name="TEST.csv")









        return





#FINFEZ().ITOGOVAYA()
#print(FINFEZ().ITOGOVAYA())
#MIN_MAX().max_FINREZ_Month()
#MIN_MAX().max_FINREZ_DATA()
#NEW().obnovlenie()
SALES().SALES_obrabotka()
SALES().Sales_prognoz()
