#!/usr/bin/env python3
# -*- coding: utf-8 -*-

############## Import bibliotek ##############
# Podstawa
import numpy as np
import pandas as pd
# SQLAlchemy
from sqlalchemy import create_engine
# Kwerendy druid
from pydruid import *
from pydruid.client import *
from pylab import plt
from pydruid.query import QueryBuilder
from pydruid.utils.postaggregator import *
from pydruid.utils.aggregators import *
from pydruid.utils.filters import *

############## Polaczenie z baza danych w Apache Druid ##############
query = PyDruid('http://localhost:8888', 'druid/v2/')

############## Kwerenda ##############
# Wczytanie calej tabeli data_smaller
data_smaller = query.scan(
            datasource = "data_smaller",
            granularity = 'all',
            intervals = "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z"
)
df_data_smaller = query.export_pandas() # Zamiana danych w ramke danych pandas
print (df_data_smaller)

# Wczytanie calej tabeli data_bigger
data_bigger = query.scan(
            datasource = "data_bigger",
            granularity = 'all',
            intervals = "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z"
)
df_data_bigger = query.export_pandas() # Zamiana danych w ramke danych pandas
print (df_data_bigger)

############## Czyszczenie danych ##############
# Usuwanie niepotrzebnych kolumn
df_data_smaller = df_data_smaller.drop(['__time'], axis = 1)
df_data_bigger = df_data_bigger.drop(['__time'], axis = 1)

# Sortowanie kolumn po id
df_data_smaller = df_data_smaller.sort_values('id')
df_data_bigger = df_data_bigger.sort_values('id')

# Wyrownanie liczby wierszy
df_data_bigger = df_data_bigger.drop(labels = range(0,61), axis = 0)

# Usuwanie niepotrzbnej juz kolumny
df_data_smaller = df_data_smaller.drop(['id'], axis = 1)

# Polaczenie ramek danych
df_data = pd.concat([df_data_bigger, df_data_smaller], axis = 1)

# Sprawdzenie formatu kolumn
df_data.info()

# Konwersja formatow kolumn
cols = ['id', 'year', 'comm_code', 'trade_usd', 'weight_kg', 'quantity', 'IMO']
df_data[cols] = df_data[cols].apply(pd.to_numeric, errors = 'coerce')
df_data.info()

# Zamiana wartosci NA na puste
df_data['country_or_area'] = df_data['country_or_area'].str.replace('NA','')
df_data['commodity'] = df_data['commodity'].str.replace('NA','')
df_data['flow'] = df_data['flow'].str.replace('NA','')
df_data['category'] = df_data['category'].str.replace('NA','')
df_data['SHIP.NAME'] = df_data['SHIP.NAME'].str.replace('NA','')
df_data['CLASS'] = df_data['CLASS'].str.replace('NA','')
df_data['STATUS'] = df_data['STATUS'].str.replace('NA','')
df_data['REASON.FOR.THE.STATUS'] = df_data['REASON.FOR.THE.STATUS'].str.replace('NA','')

############## Wiekszy plik ##############
# Wczytanie danych z pliku csv
df_data_mega = pd.read_csv('/home/gabriel/Desktop/data_wieksze.csv')

# Usuwanie niepotrzenych kolumn
df_data_mega = df_data_mega.drop(['quantity_name'], axis = 1)

# Usuwanie zbyt duzej ilosci kolumn (dla szybkosci dzialania MySQLa)
df_data_mega = df_data_mega.drop(labels = range(700000,1436218), axis = 0)

# Konwersja kolumn na numeric
df_data_mega.info()
cols = ['id', 'year', 'comm_code', 'trade_usd', 'weight_kg', 'quantity', 'IMO']
df_data_mega[cols] = df_data_mega[cols].apply(pd.to_numeric, errors = 'coerce')
df_data_mega.info()

# Zamiana wartosci NA na puste
df_data_mega['country_or_area'] = df_data_mega['country_or_area'].str.replace('nan','')
df_data_mega['commodity'] = df_data_mega['commodity'].str.replace('nan','')
df_data_mega['flow'] = df_data_mega['flow'].str.replace('nan','')
df_data_mega['category'] = df_data_mega['category'].str.replace('nan','')
df_data_mega['SHIP.NAME'] = df_data_mega['SHIP.NAME'].str.replace('nan','')
df_data_mega['CLASS'] = df_data_mega['CLASS'].str.replace('nan','')
df_data_mega['STATUS'] = df_data_mega['STATUS'].str.replace('nan','')
df_data_mega['REASON.FOR.THE.STATUS'] = df_data_mega['REASON.FOR.THE.STATUS'].str.replace('nan','')

# Polaczenie dwoch ramek danych
df = pd.concat([df_data_mega, df_data])

############## Zapis danych ##############
# Polaczenie z baza mysql
# login, haslo, host, baza danych
engine = create_engine('mysql+pymysql://mysql_admin:mysql_admin@localhost:3306/shipment')

# Usuwanie tabeli przed wczytaniem
engine.execute('DROP TABLE data_shipment;')

# Zapis do nowej tabeli data_shipment
df.to_sql('data_shipment', engine, index = False)

# Zamiana kolumny id na primary key
#engine.execute('ALTER TABLE data_shipment ADD PRIMARY KEY (`id`);')
