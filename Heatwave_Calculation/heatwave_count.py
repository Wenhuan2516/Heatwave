import pandas as pd
import numpy as np
import dask.dataframe as ddf
from pandas import Series, DataFrame
import math
import datetime

years = [year for year in range(1960, 2021)]
pattern = []

for year in years:
    temp_year = ddf.read_csv(r"/global/cfs/cdirs/m1532/Projects_MVP/geospatial/enviorment/" + 
                             "HeatwaveData/DailyClimateData_CountyLevel/"+
                             "ClimateData_Daily/ClimateData_Daily_" + str(year) + ".csv", 
                             dtype={'fips': 'object'}).compute().drop(columns={'Unnamed: 0'})
    pattern.append(temp_year)

                                                                         
temp = pd.concat(pattern)

def findApparentTemp(temperature):
    e = 0.61094 * math.exp(17.625 * temperature / (temperature + 243.04))
    A = -1.3 + 0.92 * temperature + 2.2 * e
    return A
                                                                     
                                                                     
climate = temp[['year', 'Month', 'date', 'fips', 'mean_temp', 'min_temp', 'max_temp']]
climate = climate.rename(columns = {'Month': 'month'})
                                                                     
def fahrenheit_to_celsius(f):
    c = (f -32)*5/9
    return c
                                                                     
climate['mean_temp'] = climate['mean_temp'].apply(fahrenheit_to_celsius)
climate['min_temp'] = climate['min_temp'].apply(fahrenheit_to_celsius)
climate['max_temp'] = climate['max_temp'].apply(fahrenheit_to_celsius)

climate['AT_mean'] = climate['mean_temp'].apply(findApparentTemp)
climate['AT_min'] = climate['min_temp'].apply(findApparentTemp)
climate['AT_max'] = climate['max_temp'].apply(findApparentTemp)
                                                                     
month_replace = {'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6, 'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10,
                'Nov': 11, 'Dec': 12}
climate['month'] = climate['month'].replace(month_replace)

year_temp = [year for year in range(1981, 2011)]
temp = climate[climate['year'].isin(year_temp)]

temp['date'] = pd.to_datetime(temp['date'])
temp_valid = temp[(temp['month'] == 7) | (temp['month'] == 8)]
                                                                     
mean_temp_p85 = temp_valid.groupby(['fips'])['mean_temp'].apply(lambda x: np.percentile(x.dropna(), 85)).reset_index()
mean_temp_p85.columns = ['fips', 'mean_temp_p85']

min_temp_p85 = temp_valid.groupby(['fips'])['min_temp'].apply(lambda x: np.percentile(x.dropna(), 85)).reset_index()
min_temp_p85.columns = ['fips', 'min_temp_p85']
                                                                    
max_temp_p85 = temp_valid.groupby(['fips'])['max_temp'].apply(lambda x: np.percentile(x.dropna(), 85)).reset_index()
max_temp_p85.columns = ['fips', 'max_temp_p85']

temp_p85 = mean_temp_p85.merge(min_temp_p85, on = ['fips'], how = 'inner')
temp_p85 = temp_p85.merge(max_temp_p85, on = ['fips'], how = 'inner')
                                                                     
def findList(fips, mean_temp_p85):
    temp_p85_list = []
    temp_p85_list.append(fips)
    temp_p85_list.append(mean_temp_p85)
    return temp_p85_list
                                                                     
temp_p85['temp_p85_list'] = temp_p85.apply(lambda x: findList(x['fips'], x['mean_temp_p85']), axis=1)
temp_p85_list = temp_p85['temp_p85_list'].tolist()

df = climate.merge(temp_p85, on='fips', how='left')
df.to_csv('apparent_temperature_1960_2020.csv')

def count_heatwaves(series, threshold):
    is_above_threshold = series > threshold

    heatwave_groups = (is_above_threshold.diff(1) != 0).cumsum()

    group_lengths = heatwave_groups[is_above_threshold].value_counts()

    heatwave_count = (group_lengths >= 2).sum()

    return heatwave_count
                                                                     
                                                                     
heatwave_counts = df.groupby(['year', 'month', 'fips']).apply(lambda group: count_heatwaves(group['AT_min'],
                                                                                            group['mean_temp_p85'].iloc[0]))


heatwave_counts = heatwave_counts.reset_index(name='heatwave_count')
                                                                     
heatwave_counts.to_csv('heatwave_updated/heatwave_count_monthly_1960_2020.csv')


                                                                     

                                                                     
                                                                     
   
                                                                     
                                                                     
                                                                     
                                                                     
                                                                     
                                                                     
                                                                     
                                                                     
                                                                     
                                                                     
    

