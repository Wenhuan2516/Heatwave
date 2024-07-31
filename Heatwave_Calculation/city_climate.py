import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import dask.dataframe as ddf
from pandas import Series, DataFrame
from shapely import wkt
import datetime
import math
import warnings
warnings.filterwarnings('ignore')
from pyproj import Proj, transform

def findCentroid(g):
    p1 = wkt.loads(g)
    return p1.centroid.wkt

def findLon(point):
    strList = point.split(' ')
    lon = strList[1][1:]
    lon = float(lon)
    return lon

def findLat(point):
    strList = point.split(' ')
    lat = strList[2][:-1]
    lat = float(lat)
    return lat

def findLongitude(x, y):
    proj_web_mercator = Proj(init='epsg:3857')
    proj_wgs84 = Proj(init='epsg:4326')
    longitude, latitude = transform(proj_web_mercator, proj_wgs84, x, y)
    return longitude

def findLatitude(x, y):
    proj_web_mercator = Proj(init='epsg:3857')
    proj_wgs84 = Proj(init='epsg:4326')
    longitude, latitude = transform(proj_web_mercator, proj_wgs84, x, y)
    return latitude

def convertString(f):
    return f.rjust(6, '0')

def getMonth(date):
    return(date.strftime('%b'))

def findFog(F):
    return int(F[0])

def findRain(F):
    return int(F[1])

def findSnow(F):
    return int(F[2])

def findHail(F):
    return int(F[3])

def findThunder(F):
    return int(F[4])

def findTornado(F):
    return int(F[5])

def renameColumns(df):
    df = df.rename(columns = {'STATION':'station_id','NAME':'station_name',
                            'LATITUDE':'station_lat', 'LONGITUDE': 'station_lon',
                            'ELEVATION': 'elevation','DATE': 'date', 'TEMP':
                            'mean_temp', 'MIN': 'min_temp', 'MAX': 'max_temp',
                            'DEWP':'dewpoint', 'SLP': 'sea_level_pressure', 'STP':
                            'station_pressure', 'VISIB': 'visibility', 'WDSP':
                            'wind_speed', 'MXSPD': 'max_wind_speed', 'GUST':
                            'wind_gust', 'PRCP': 'precipitation','PRCP_ATTRIBUTES':
                            'precip_flag', 'YEAR':'year', 'Month': 'month'})
    return df

def seperateFRSHTT(df):
    df['fog'] = df['FRSHTT'].apply(findFog)
    df['rain'] = df['FRSHTT'].apply(findRain)
    df['snow'] = df['FRSHTT'].apply(findSnow)
    df['hail'] = df['FRSHTT'].apply(findHail)
    df['thunder'] = df['FRSHTT'].apply(findThunder)
    df['tornado'] = df['FRSHTT'].apply(findTornado)
    return df

def selectColumns(df):
    df = df[['station_id', 'date', 'station_lat', 'station_lon', 'elevation',
             'station_name', 'mean_temp', 'min_temp', 'max_temp', 'dewpoint',
             'sea_level_pressure', 'station_pressure', 'visibility', 'wind_speed',
             'precipitation', 'fog', 'rain', 'snow','hail', 'thunder', 'tornado',
             'year', 'month']]
    return df

def ID_Coordinate(ID,lat, lon):
    setList = []
    setList.append(ID)
    setList.append(lat)
    setList.append(lon)
    return setList

years = [year for year in range(1961, 2021)]

def dist(p0, p1):
    return (((p0[0] - p1[0])**2) + ((p0[1] - p1[1])**2))**.5

def findSquare(pointList, lat, lon):
    square = []
    for point in pointList:
        point_lat = point[1]
        point_lon = point[2]
        if ((point_lat < lat + 1)
            and (point_lat > lat - 1)
            and (point_lon < lon + 1)
            and (point_lon > lon - 1)):
            square.append(point)
    return square

def shortestDistance(lat, lon):
    p1 = []
    p1.append(lat)
    p1.append(lon)
    distants = []
    square = findSquare(pointList, lat, lon)
    for point in square:
        p_id = point[0]
        p_lat = point[1]
        p_lon = point[2]
        p2 = []
        p2.append(p_lat)
        p2.append(p_lon)
        distant = dist(p1, p2)
        dist_id = []
        dist_id.append(p_id)
        dist_id.append(distant)
        distants.append(dist_id)
    distantList = []    
    for ele in distants:
        distantList.append(ele[1])
    for item in distants:
        if item[1] == min(distantList):
            return item[0]

for year in years:
    city = pd.read_csv('/global/cfs/cdirs/m1532/Projects_MVP/geospatial/climate_heatwave/' +
                       'city_heatwave/50_city_boundaries.csv', dtype = {'fips': str})
    city = city.loc[:, ~city.columns.str.contains('^Unnamed')]
    city['Centroid'] = city['geometry'].apply(findCentroid)
    city['X'] = city['Centroid'].apply(findLon)
    city['Y'] = city['Centroid'].apply(findLat)
    city['longitude'] = city.apply(lambda row: findLongitude(row.X, row.Y), axis = 1)
    city['latitude'] = city.apply(lambda row: findLatitude(row.X, row.Y), axis = 1)
    
    ur_files = ddf.read_csv(r'/global/cfs/cdirs/m1532/Projects_MVP/'+
                            'geospatial/enviorment/HeatwaveData/DailyTemp_full/DailyTemp_full_'
                            + str(year) + '.csv', dtype={'STATION': 'object','FRSHTT': 'object'})
    temp = ur_files.compute()
    temp = temp.loc[:, ~temp.columns.str.contains('^Unnamed')]
    temp['FRSHTT'] = temp['FRSHTT'].apply(convertString)
    temp['DATE'] = pd.to_datetime(temp['DATE'])
    temp['month'] = temp['DATE'].apply(getMonth)
    temp = renameColumns(temp)
    temp = seperateFRSHTT(temp)
    temp = selectColumns(temp)
    temp['ID_Coordinate'] = temp.apply(lambda x:ID_Coordinate(x['station_id'], x['station_lat'], x['station_lon']), axis=1)
    pointList = temp['ID_Coordinate'].tolist()
    city['NearestStation'] = city.apply(lambda x: shortestDistance(x['latitude'], x['longitude']), axis=1)
    city = city.rename(columns = {'NearestStation': 'station_id'})
    city_temp = city.merge(temp, on = ['station_id'], how = 'inner')
    city_temp = city_temp[['NAME', 'longitude', 'latitude', 'ID_Coordinate', 'year', 'month', 'date', 
                       'elevation', 'mean_temp', 'min_temp', 'max_temp', 'dewpoint', 'sea_level_pressure', 
                       'station_pressure', 'visibility', 'wind_speed', 'precipitation', 'fog', 
                       'rain', 'snow', 'hail', 'thunder', 'tornado']]
    city_temp = city_temp.rename(columns = {'NAME': 'city_name'})
    city_temp = city_temp.rename(columns = {'ID_Coordinate': 'station_coordinate'})
    city_temp.to_csv('city_climate/city_climate_' + str(year) + '.csv')