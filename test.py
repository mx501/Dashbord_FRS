import numpy as np
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation

# Инициализация фигуры и осей
fig, ax = plt.subplots()
x = np.linspace(0, 2*np.pi, 200)
y = np.sin(x)

# Определение функции обновления данных и перерисовки графика
def update(frame):
    # Изменение данных
    y = np.sin(x + frame/10.0)
    # Очистка предыдущего графика
    ax.cla()
    # Построение нового графика
    ax.plot(x, y)
    # Настройка осей
    ax.set_xlim([0, 2*np.pi])
    ax.set_ylim([-1, 1])
    # Возврат объекта, который будет перерисовываться
    return ax

# Создание объекта Animation
ani = FuncAnimation(fig, update, frames=100, blit=True)

# Отображение графика
plt.show()