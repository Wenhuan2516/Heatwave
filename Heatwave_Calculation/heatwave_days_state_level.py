import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import dask.dataframe as ddf
from pandas import Series, DataFrame
import geopandas as gpd
import pandas as pd

temp = ddf.read_csv(r'heatwave_definition2/PRISM_apparent_temp_1960_2019.csv', dtype = {'year': int, 'fips': str, 'state': str}).compute().drop(columns={'Unnamed: 0'})
climate = temp[['year', 'month', 'date', 'fips', 'tMean', 'tMin', 'tMax', 'AT_mean', 'AT_min', 'AT_max', 'state']]

climate_state = climate.drop('fips', axis = 1)
climate_state = climate_state.groupby(['year', 'month', 'date', 'state']).median()
climate_state = climate_state.reset_index()


climate = climate_state.sort_values(['state', 'month', 'year'])
climate['date'] = pd.to_datetime(climate['date'])

df_jul_aug = climate[(climate['date'].dt.month.isin([7, 8])) & (climate['date'].dt.year.between(1981, 2010))]

percentiles = df_jul_aug.groupby('state')['AT_min'].quantile(0.85).reset_index()
percentiles.rename(columns={'AT_min': 'threshold_temp'}, inplace=True)

df_climate = climate.merge(percentiles, on='state')
df_climate['is_exceedance'] = df_climate['AT_min'] > df_climate['threshold_temp']



def count_heatwave(df):
    df['block'] = (df['is_exceedance'] != df['is_exceedance'].shift(1)).cumsum()
    df_heatwaves = df.groupby(['state', 'block', 'year']).agg(
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
        state = row['state']
        block = row['block']
        df_date = pd.DataFrame(date_range, columns=['date'])
        df_date['year'] = df_date['date'].dt.year
        df_date['month'] = df_date['date'].dt.month
        df_date['day'] = df_date['date'].dt.day
        df_date['state'] = state
        df_date['block'] = block
        result.append(df_date)
        
    return result


heatwave = count_heatwave(df_climate)
heatwave_result = split_heatwave(heatwave)
heatwave_days = pd.concat(heatwave_result)
days = heatwave_days.drop('date', axis = 1)
days = days.groupby(['state', 'year', 'month']).count()
days = days.reset_index()
days = days.rename(columns = {'day': 'heatwave_days'})
days = days.drop('block', axis = 1)

days.to_csv('heatwave_definition2/heatwave_days_state_level_final.csv')

