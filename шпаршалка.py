# ыклом заменить значения
for i in range(len(FINREZ['Значение'])):
    value = FINREZ['Значение'][i]
    if value == 0 or value == "-" or value == "#ДЕЛ/0!" or value == "#ЗНАЧ!":
        FINREZ['Значение'][i] = "nan"
    else:
        value = value.replace(" ", "")
        value = value.replace(",", ".")
        FINREZ['Значение'][i] = float(value)

FINREZ = FINREZ.loc[(FINREZ['Значение'] != "nan")]
FINREZ = FINREZ.loc[(FINREZ['Значение'] != 0)]

# проверяем все столбцы на наличие пустых значений
print(df.isnull().any())

# выводим количество пустых значений в каждом столбце DataFrame
print(df.isnull().sum())


№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№
# Создание примера DataFrame
df = pd.DataFrame({'Колонка 1': [1, 2, 3, 4], 'Колонка 2': [5, 6, pd.np.nan, 8]})

# Скрытие строк, в которых значение в столбце "Колонка 2" равно NaN
df = df.dropna(subset=['Колонка 2'])


import pandas as pd
import numpy as np

# создаем DataFrame с продажами по дням
date_rng = pd.date_range(start='1/1/2022', end='1/30/2022', freq='D')
sales_data = np.random.randint(1, high=100, size=(len(date_rng)))
df = pd.DataFrame({'date': date_rng, 'sales': sales_data})

# устанавливаем столбец 'date' в качестве индекса
df.set_index('date', inplace=True)

# используем метод rolling для вычисления средних продаж за последние 3 месяца
rolling_mean = df.rolling(window=90, min_periods=1).mean()

# используем метод reindex для наложения полученных значений на будущие даты
future_dates = pd.date_range(start='1/31/2022', end='2/28/2022', freq='D')
all_dates = pd.concat([df.index, future_dates])
rolling_mean = rolling_mean.reindex(all_dates)

print(rolling_mean)

№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№


import pandas as pd
from tqdm import tqdm

filename = "file.csv"

# Подсчет общего количества строк в файле
with open(filename) as f:
    num_lines = sum(1 for _ in f)

# Чтение файла с прогресс-баром
with tqdm(total=num_lines) as pbar:
    data = pd.read_csv(filename, iterator=True)
    for chunk in data:
        # Обработка данных
        ...
        pbar.update(len(chunk))

3№№№№№№№№№№№№№№№№№№№№№№