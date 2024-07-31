import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import dask.dataframe as ddf
from pandas import Series, DataFrame
import geopandas as gpd
import pandas as pd

temp = ddf.read_csv(r'heatwave_definition2/PRISM_apparent_temp_1960_2019.csv', dtype = {'year': int, 'fips': str}).compute().drop(columns={'Unnamed: 0'})
climate = temp[['year', 'month', 'date', 'fips', 'tMean', 'tMin', 'tMax', 'AT_mean', 'AT_min', 'AT_max']]
climate = climate.sort_values(['fips', 'month', 'year'])
climate['date'] = pd.to_datetime(climate['date'])

df_jul_aug = climate[(climate['date'].dt.month.isin([7, 8])) & (climate['date'].dt.year.between(1981, 2010))]

percentiles = df_jul_aug.groupby('fips')['AT_min'].quantile(0.85).reset_index()
percentiles.rename(columns={'AT_min': 'threshold_temp'}, inplace=True)

df_climate = climate.merge(percentiles, on='fips')
df_climate['is_exceedance'] = df_climate['AT_min'] > df_climate['threshold_temp']

def count_heatwave(df):
    df['block'] = (df['is_exceedance'] != df['is_exceedance'].shift(1)).cumsum()
    df_heatwaves = df.groupby(['fips', 'block', 'year']).agg(
        start_date=('date', 'min'),
        end_date=('date', 'max'),
        exceedance_days=('is_exceedance', 'sum')
    )
    df_heatwaves = df_heatwaves.reset_index()
    df_heatwaves = df_heatwaves[df_heatwaves['exceedance_days'] >= 2]
    return df_heatwaves

def split_heatwave(df_heatwave):
    result = []
    for index,row in df_heatwave.iterrows():
        start_date = row['start_date']
        end_date = row['end_date']
        date_range = pd.date_range(start_date, end_date, freq='D')
        fips = row['fips']
        block = row['block']
        df_date = pd.DataFrame(date_range, columns=['date'])
        df_date['year'] = df_date['date'].dt.year
        df_date['month'] = df_date['date'].dt.month
        df_date['day'] = df_date['date'].dt.day
        df_date['fips'] = fips
        df_date['block'] = block
        result.append(df_date)
        
    return result


df_60s = df_climate[(df_climate['year'] >= 1960) & (df_climate['year'] < 1970)]
df_70s = df_climate[(df_climate['year'] >= 1970) & (df_climate['year'] < 1980)]
df_80s = df_climate[(df_climate['year'] >= 1980) & (df_climate['year'] < 1990)]
df_90s = df_climate[(df_climate['year'] >= 1990) & (df_climate['year'] < 2000)]
df_00s = df_climate[(df_climate['year'] >= 2000) & (df_climate['year'] < 2010)]
df_10s = df_climate[(df_climate['year'] >= 2010) & (df_climate['year'] < 2020)]


heatwave_60s = count_heatwave(df_60s)
heatwave_result_60s = split_heatwave(heatwave_60s)
heatwave_days_60s = pd.concat(heatwave_result_60s)
days_60s = heatwave_days_60s.drop('date', axis = 1)
days_60s = days_60s.groupby(['fips', 'year', 'month']).count()
days_60s = days_60s.reset_index()
days_60s = days_60s.rename(columns = {'day': 'heatwave_days'})

heatwave_70s = count_heatwave(df_70s)
heatwave_result_70s = split_heatwave(heatwave_70s)
heatwave_days_70s = pd.concat(heatwave_result_70s)
days_70s = heatwave_days_70s.drop('date', axis = 1)
days_70s = days_70s.groupby(['fips', 'year', 'month']).count()
days_70s = days_70s.reset_index()
days_70s = days_70s.rename(columns = {'day': 'heatwave_days'})

heatwave_80s = count_heatwave(df_80s)
heatwave_result_80s = split_heatwave(heatwave_80s)
heatwave_days_80s = pd.concat(heatwave_result_80s)
days_80s = heatwave_days_80s.drop('date', axis = 1)
days_80s = days_80s.groupby(['fips', 'year', 'month']).count()
days_80s = days_80s.reset_index()
days_80s = days_80s.rename(columns = {'day': 'heatwave_days'})

heatwave_90s = count_heatwave(df_90s)
heatwave_result_90s = split_heatwave(heatwave_90s)
heatwave_days_90s = pd.concat(heatwave_result_90s)
days_90s = heatwave_days_90s.drop('date', axis = 1)
days_90s = days_90s.groupby(['fips', 'year', 'month']).count()
days_90s = days_90s.reset_index()
days_90s = days_90s.rename(columns = {'day': 'heatwave_days'})

heatwave_00s = count_heatwave(df_00s)
heatwave_result_00s = split_heatwave(heatwave_00s)
heatwave_days_00s = pd.concat(heatwave_result_00s)
days_00s = heatwave_days_00s.drop('date', axis = 1)
days_00s = days_00s.groupby(['fips', 'year', 'month']).count()
days_00s = days_00s.reset_index()
days_00s = days_00s.rename(columns = {'day': 'heatwave_days'})

heatwave_10s = count_heatwave(df_10s)
heatwave_result_10s = split_heatwave(heatwave_10s)
heatwave_days_10s = pd.concat(heatwave_result_10s)
days_10s = heatwave_days_10s.drop('date', axis = 1)
days_10s = days_10s.groupby(['fips', 'year', 'month']).count()
days_10s = days_10s.reset_index()
days_10s = days_10s.rename(columns = {'day': 'heatwave_days'})

heatwave_days = [days_60s, days_70s, days_80s, days_90s, days_00s, days_10s]
heatwave = pd.concat(heatwave_days)
heatwave = heatwave.drop('block', axis = 1)

heatwave.to_csv('heatwave_definition2/heatwave_days_county_level_final.csv')

