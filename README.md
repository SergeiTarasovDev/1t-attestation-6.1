## Аттестационная работа
Тема: Анализ рынка акций

Цель проекта: создать витрину, содержащую обработанные данные по котировкам акций.

Гранулярность: день

Необходимые показатели:
 - Суммарный объем торгов за последние сутки
 - Курс валюты на момент открытия торгов для данных суток
 - Курс валюты на момент закрытия торгов для данных суток
 - Разница (в %) курса с момента открытия до момента закрытия торгов для данных суток
 - Минимальный временной интервал, на котором был зафиксирован самый крупный объем торгов для данных суток
 - Минимальный временной интервал, на котором был зафиксирован максимальный курс для данных суток
 - Минимальный временной интервал, на котором был зафиксирован минимальный курс торгов для данных суток

Источник данных: https://www.alphavantage.co/documentation/

Схема проекта:
![Схема проекта](/src/scheme.png)

Использованные технологии:
 - python
 - postgresql
 - docker-compose

## Инструкция по развертыванию проекта:

1. Запустить docker-compose, с помощью команды docker-compose up
2. Настроить подключение к базе данных, с помощью dBeaver, используя следующие параметры подключения:
   - host: localhost
   - port: 5434
   - user: postgres
   - password: postgres
   - database: quotes

## Описание проекта:

#### Этап 1. Развертывание проекта. 

1. На этом этапе происходит создание необходимых таблиц и их наполнение историческими данными:
    - Таблицы слоя Row 
      - stock_quotes, содержащая исторические котировки акций за прошлые периоды с гранулярностью 1 день.
      - symbols содержит список акций.
    - Таблицы слоя Core
      - f_quotes - таблица фактов, которая содержит исторические данные по котировкам акций и объемам их торгов.
      - d_symbols - таблица измерений с описанием акций.
    - Витрина в слое Mart
      - dm_quotes_analyze - витрина, содержащая обработанные данные по котировкам акций.
2. Далее происходит наполнение таблиц слоя Row историческими данными, собранными в файлы csv/stock_quotes.csv и csv/symbols.csv, эти файлы необходимы в связи с ограничением API в бесплатной версии на 25 запросов в сутки, в них записываются уже полученные ранее данные по API. Также они выполняют роль бэкапа базы данных.

#### Этап 2. Ежедневный сбор данных.

0. Выполняется ежедневный запуск по CRON (не использовал Airflow, т.к. не получилось развернуть на моем ПК)
1. Выполняется запрос к API на получение данных по котировками акций за текущий день и внесение этих данных в таблицы Raw слоя.
2. Выполняется запрос к API на получение исторических данных, для насыщения БД статистикой. Полученные данные записываются в БД и сохраняются в резервные файлы csv. Эти данные пополняются постепенно, в связи с ограничением API на 25 запросов в день.

#### Этап 3. Выполнение вычислений и построение витрины данных

1. Формируются таблицы слоя Core
2. Формируется итоговая витрина в слое Mart